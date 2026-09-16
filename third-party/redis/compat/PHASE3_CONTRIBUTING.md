# Phase 3 contributor brief (Redis compatibility layer)

Read this before touching the adapter. It is the map of where things live,
the conventions every command follows, and the exact build/test recipe.

## Architecture in one paragraph

Clients speak RESP to a Rust front end (`rust-lib/src/lib.rs`, one worker
thread per configured Redis worker). Each command that touches data becomes
one `TxnRequest` (an array of `TxnOperation`) handed once across a C ABI
(`include/transaction_ffi.h`) to the C++ executor `execute_transaction` in
`cpp/makoCon.cc`, which runs every op inside a single Mako/STO transaction and
fills a `TxnResponse`. Rust formats the reply. There is no interactive
transaction: the whole op list is built before the call, so a later op cannot
depend on an earlier op's result within one call. Anything needing
read-then-write atomicity must be a single C++ op.

## Where each part of a command lives (Rust, `lib.rs`)

Search for these anchors; do not rely on line numbers.

| Step | Anchor | Notes |
|---|---|---|
| Opcode | `enum OpCode {` | Explicit discriminants. Next free values start at 178. Never reuse a number. |
| Name lookup | `fn parse_opcode(name: &[u8])` | Chain of `ascii_eq_ci`. Add before the final `else { None }`. |
| Parsing | `fn parse_resp3(` → `match op {` | One arm per command: validate arity with `wrong_arity("name")`, keys via `part_to_bytes` + `validate_user_key`, extra args go in `cmd.values`, raw args in `command_args(&parts)`. Return `Err(ParseError::Protocol("syntax error"))` for bad options, `ParseError::Error("...")` for Redis-style `ERR` text. |
| Op building | `fn build_txn_ops(` | Push `TxnOperation`s. Extra data travels as `pack_bytes_list(&cmd.values)` in `val_ptr/val_len`; keep the `Bytes` alive by pushing into `payloads`. All 32 `flags` bits are taken; pass options inside the packed payload instead. |
| Reply | `fn write_command_result(` → `match cmd.op {` | `first` is the first `TxnOpResult` of the command's span. `success=false` usually means WRONGTYPE for typed ops. Use `write_integer/bulk/array_header/null/simple_ok/err`. |
| Retry class | `fn command_needs_retry(` | Add every new storage-backed opcode so OCC aborts retry (32 attempts). |
| Dirty class | `fn is_dirty_command(` | Add every opcode that writes; this drives WATCH invalidation and OOM checks. |
| Router | `fn handle_command(` → the big `OpCode::Get \| ... \| OpCode::ZScan =>` arm | Add storage-backed opcodes to that list. Local/admin commands get their own arm (see `OpCode::SlowLog`). |
| Subscriber mode | `if client_state.in_subscriber_mode()` list in `handle_command` | Only add if the command is legal while subscribed. |
| Command struct | `struct Command` | Fields: `keys`, `val`, `values`, `args`, `expire_at_ms`, `expire_flags`, `scan_*`, `set_*`, `restore_kind`. Add a field only if the existing ones cannot carry the data; update `Command::new`. |
| Memory backend | `fn memory_execute_transaction(` | `MAKO_REDIS_BACKEND=memory` test backend; supports only a dozen ops. Unit tests that need storage use it; do not extend it unless required. |
| Unit tests | `#[cfg(test)] mod tests` at the end | Helpers `command(op, args)`, `run(cmd, txn_state, client_state)`, `run_raw(bytes)`. FFI calls are stubbed in test mode (`cpp_execute_transaction` returns false), so tests cover parsing and local commands, not storage. |

## Where each part lives (C++, `cpp/makoCon.cc`)

`execute_transaction(const TxnRequest*, TxnResponse*)` is one very large
function. Helpers are lambdas defined near its top; ops are handled in a long
`if (op.op == TXN_OP_X) { ... } else if (...)` chain. To add an op, insert a
new `} else if (op.op == TXN_OP_NEW) {` branch (a good insertion point is just
before `} else if (op.op == TXN_OP_RESTORE_LIST) {`).

Inside a branch you have: `op` (the `TxnOperation`), `result` (its
`TxnOpResult`), `txn`, `user_key` (the Redis key), `tl_key_buf` (the storage
key `"table_key_" + user_key` for the string namespace), `all_success`,
`batch_exists` / `batch_values` (per-transaction caches of storage keys, kept
in sync for you by `put_raw` and `delete_raw_if_exists`), and the helper
lambdas.

Useful lambdas (grep `auto NAME = [`):
- strings: `read_current(txn, user_key, storage_key, value, exists)`,
  `put_raw(txn, storage_key, value)`, `delete_raw_if_exists(txn, storage_key)`,
  `string_key_allowed(txn, user_key, storage_key, result, allowed)` (sets
  WRONGTYPE into `result` when the key holds another type; then `continue`).
- type guards: `set_key_allowed`, `zset_key_allowed`, `hash_key_allowed`,
  `load_list_stage`, `expire_logical_key_if_needed(txn, user_key, storage_key)`.
- collections: `read_set_members`, `collect_set_members`,
  `collect_zset_values(txn, key, std::map<std::string,double>&)`,
  `rewrite_zset_values`, `collect_hash_entries`, `read_list_values`,
  `rewrite_list_values`, `read_*_cardinality`, `delete_set/list/zset/hash`.
- TTL: `read_ttl_meta`, `clear_ttl_meta(txn, user_key)`.
- payloads: `pack_bytes_list(std::vector<std::string>)`,
  `unpack_bytes_list(ptr, len, std::vector<std::string>&)`,
  `copy_result_value(result, std::string)` to return bytes.
- zset scores: `encode_zset_score`, `format_zset_score`, `parse_zset_score_value`.

Writes and deletes are both deferred. `put_raw` writes nothing: it records the
raw value in the transaction's `pending_writes` map (last write to a key wins)
and marks the key present in `batch_exists`/`batch_values`.
`delete_raw_if_exists` drops any buffered write for the key and records the
storage key in `pending_deletes`, marking it absent. `read_raw` answers from
`pending_writes` first and reports "not found" for a key in `pending_deletes`,
so every read path in the transaction sees the transaction's own writes.
`flush_pending_writes` then issues one `tx_put` per key and
`flush_pending_deletes` the real `tx_remove` calls — once each, after the
staged collections are written and just before `Commit`. The two sets are
disjoint by construction, so a key deleted and written again in one transaction
is overwritten in place, and a key created and then deleted never reaches
storage at all.

Both buffers exist for the same reason. STO is built with
`-DREAD_MY_WRITES=OFF` (`CMakeLists.txt` passes `-DREAD_MY_WRITES=${STO_RMW}`,
which defaults to OFF), so in `src/mako/sto/MassTrans.hh` a record the
transaction itself created cannot be touched again by that transaction:
`trans_write` allocates the `versioned_value` with the creation value already
in it and puts only the KEY in the TransItem write slot, so `install()`
re-publishes that value and ignores every later `transPut`; `transDelete` on
that record takes the `if (!valid) Sto::abort()` branch; and a remove followed
by a write of the same key leaves `delete_bit` set on an item whose payload is
now the value, so `install()` invalidates the record and calls `remove()` with
the value bytes as the key, stranding it for the life of the process.

Route every write through `put_raw` and every removal through
`delete_raw_if_exists`. A bare `tx_put`/`redis_table_put` escapes the buffer:
a second write of that key in the same transaction is then silently dropped,
and reads in that transaction see stale bytes. A bare
`tx_remove`/`redis_table_delete` strands the key. The one code path that talks
to storage directly, and must stay that way, is the raw GET/SET fast path
(`execute_fast_mako_string`), which runs a single op per transaction and needs
no buffering. What the buffers do not cover is keyspace enumeration: KEYS,
SCAN, DBSIZE and RANDOMKEY scan storage, so inside one transaction they do not
see its own creations or deletions (see `known_divergences.txt`, "Keyspace
enumeration inside a transaction").

Key layout conventions: strings live at `"table_key_" + key`; collections
live under hidden `0x01` prefixes built by `make_set_member_key`,
`make_hash_field_key`, `make_zset_member_key`, `make_zset_score_key`, each
with a `*_meta_key` carrying the cardinality. Follow the same pattern for any
new type: one meta key per logical key plus one composite key per element.

When you add a C++ op you must also:
1. Add `TXN_OP_NEW = <n>` to `include/transaction_ffi.h` (next free: 79) and
   `const TXN_OP_NEW: u32 = <n>;` in `lib.rs`.
2. Classify it in `redis_op_is_read_only` (reads only) and, if it touches
   keys other than `op.key`, in `redis_op_uses_only_primary_lock_key` (return
   false) and `redis_request_lock_stripes` (`add_packed_lock_keys`), so the
   key-stripe locks cover every key the op reads or writes.
3. Never call `unpack_bytes_list` on a payload you did not pack yourself.

`makoConMultiTrd.cc` is a separate binary that must keep compiling; it only
needs the header to build, and it deliberately supports a small op subset.

## Build and test on ag2

Local machines cannot build this tree. Work in the clone
`/home/users/ssoumojit/mako-pr72-perf` on ag2 (reach it with
`ssh -J zoo-gate ag2`). Copy your edited files there with `scp -o
ProxyJump=zoo-gate FILE ag2:/home/users/ssoumojit/mako-pr72-perf/<same path>`.

```sh
# environment for every remote command
TC=/home/users/ssoumojit/.local/pr72-toolchain
export PATH=$TC/cargo/bin:$PATH RUSTUP_HOME=$TC/rustup CARGO_HOME=$TC/cargo
export LD_LIBRARY_PATH=$TC/root/usr/lib/llvm-22/lib:$TC/root/usr/lib/x86_64-linux-gnu
cd /home/users/ssoumojit/mako-pr72-perf

# fast Rust check + unit tests (seconds)
(cd third-party/redis/rust-lib && cargo check && cargo test)

# relink the server (Rust-only change: ~1 min; makoCon.cc change: ~10 min;
# header change: ~30 min because many objects rebuild)
/usr/bin/ninja -C build-o3 makoCon

# run a server on a private port (pick one in 6460-6499)
MAKO_HOST=127.0.0.1 MAKO_PORT=6460 MAKO_REDIS_THREADS=4 MAKO_REDIS_BACKEND=mako \
MAKO_REPLICATION_ENABLED=0 MAKO_PAXOS_PROC_NAME=localhost \
taskset -c 0-3 ./build-o3/makoCon > /tmp/my_server.log 2>&1 &

# functional checks (dependency-free RESP client; add your cases here)
python3 third-party/redis/compat/test_phase2_commands.py 127.0.0.1 6460

# regression gates that must stay green
SP=$(ls -d /home/users/ssoumojit/.local/pr72-venv/lib/python*/site-packages | head -1)
PYTHONPATH=$SP MAKO_REDIS_HOST=127.0.0.1 MAKO_REDIS_PORT=6460 \
  python3 -m pytest third-party/redis/compat -q -p no:cacheprovider \
  --ignore=third-party/redis/compat/test_phase2_commands.py
MAKO_HOST=127.0.0.1 MAKO_PORT=6460 bash third-party/redis/compat/run_tcl_suite.sh   # ~8 min

pkill -x makoCon   # exact name; never pkill -f a pattern that appears in your own command line
```

Gotchas: `pkill -f`/`pgrep -f` match your own shell command; use exact
names or a `[b]racket` trick. If `makoCon` exits with 127, the toolchain
`LD_LIBRARY_PATH` is missing. Ports 31000+ are used by the RPC layer; a stale
server holding them makes the next one panic with `AddressInUse`.

## Conventions and review checklist

- Redis error texts verbatim where Redis defines them; `WRONGTYPE Operation
  against a key holding the wrong kind of value` via `write_wrongtype`.
- Reply shapes must match Redis for RESP2 (the TCL suite runs RESP2).
- Every new command gets: parse arm, build arm, reply arm, retry class, dirty
  class if it writes, router list entry, functional test cases, and an entry in
  `command_tiers.json` is NOT required (the probe compares to a reference
  Redis with fixed arguments).
- Record any deliberate deviation in `known_divergences.txt` with the
  `mako-design-difference` category and a one-line reason.
- Do not touch the raw GET/SET fast path (`process_raw_mako_fast_frame`,
  `execute_fast_mako_string_op`) or the executor's specialized GET/SET path;
  performance claims depend on them.
- Keep `cargo test` and the pytest and TCL gates green before handing back.
- Commit on the current branch with a message that lists commands added,
  deviations, and test results; do not push.
