#!/usr/bin/env python3
"""Functional checks for the phase-2 Redis commands (no third-party deps).

Run against a live makoCon:  python3 test_phase2_commands.py HOST PORT
Exits non-zero on the first failing assertion and prints a summary otherwise.
"""
import socket
import sys
import time


class Resp:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=10)
        self.buf = b""

    def send(self, *args):
        out = b"*%d\r\n" % len(args)
        for arg in args:
            if isinstance(arg, str):
                arg = arg.encode()
            out += b"$%d\r\n%s\r\n" % (len(arg), arg)
        self.sock.sendall(out)

    def _line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _exact(self, n):
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def read(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            return Exception(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self._exact(n)
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        if t == b"%":
            n = int(rest)
            return {self.read(): self.read() for _ in range(n)}
        if t == b"_":
            return None
        raise ValueError("unexpected reply %r" % line)

    def cmd(self, *args):
        self.send(*args)
        return self.read()


FAILED = []
PASSED = 0


def check(name, got, want=None, pred=None):
    global PASSED
    ok = pred(got) if pred else got == want
    if ok:
        PASSED += 1
    else:
        FAILED.append((name, got, want))
        print("FAIL %-40s got=%r want=%r" % (name, got, want))


def is_err(got, prefix=""):
    return isinstance(got, Exception) and str(got).startswith(prefix)


def main():
    host, port = sys.argv[1], int(sys.argv[2])
    c = Resp(host, port)
    c.cmd("FLUSHALL")

    # TOUCH
    c.cmd("SET", "t1", "v")
    check("TOUCH counts existing keys", c.cmd("TOUCH", "t1", "t-missing"), 1)

    # SORT LIMIT / SORT_RO
    c.cmd("RPUSH", "sl", "3", "1", "2")
    check("SORT LIMIT 1 1", c.cmd("SORT", "sl", "LIMIT", "1", "1"), [b"2"])
    check("SORT LIMIT 1 -1", c.cmd("SORT", "sl", "LIMIT", "1", "-1"), [b"2", b"3"])
    check("SORT_RO", c.cmd("SORT_RO", "sl"), [b"1", b"2", b"3"])
    # The adapter reports parser syntax errors as "ERR protocol error: syntax error".
    check("SORT_RO rejects STORE", c.cmd("SORT_RO", "sl", "STORE", "x"), pred=lambda g: is_err(g, "ERR") and "syntax error" in str(g))
    check("SORT LIMIT STORE", c.cmd("SORT", "sl", "LIMIT", "0", "2", "STORE", "sl2"), 2)
    check("SORT LIMIT STORE contents", c.cmd("LRANGE", "sl2", "0", "-1"), [b"1", b"2"])

    # BITCOUNT (values from the Redis documentation examples)
    c.cmd("SET", "bc", "foobar")
    check("BITCOUNT all", c.cmd("BITCOUNT", "bc"), 26)
    check("BITCOUNT 0 0", c.cmd("BITCOUNT", "bc", "0", "0"), 4)
    check("BITCOUNT 1 1", c.cmd("BITCOUNT", "bc", "1", "1"), 6)
    check("BITCOUNT 1 1 BYTE", c.cmd("BITCOUNT", "bc", "1", "1", "BYTE"), 6)
    check("BITCOUNT 5 30 BIT", c.cmd("BITCOUNT", "bc", "5", "30", "BIT"), 17)
    check("BITCOUNT missing", c.cmd("BITCOUNT", "bc-missing"), 0)
    c.cmd("RPUSH", "bc-list", "x")
    check("BITCOUNT wrongtype", c.cmd("BITCOUNT", "bc-list"), pred=lambda g: is_err(g, "WRONGTYPE"))

    # BITPOS (Redis documentation examples)
    c.cmd("SET", "bp1", b"\xff\xf0\x00")
    check("BITPOS first 0", c.cmd("BITPOS", "bp1", "0"), 12)
    c.cmd("SET", "bp2", b"\x00\xff\xf0")
    check("BITPOS 1 from byte 0", c.cmd("BITPOS", "bp2", "1", "0"), 8)
    check("BITPOS 1 2 -1 BYTE", c.cmd("BITPOS", "bp2", "1", "2", "-1", "BYTE"), 16)
    check("BITPOS 1 7 15 BIT", c.cmd("BITPOS", "bp2", "1", "7", "15", "BIT"), 8)
    c.cmd("SET", "bp3", b"\x00\x00\x00")
    check("BITPOS 1 none", c.cmd("BITPOS", "bp3", "1"), -1)
    c.cmd("SET", "bp4", b"\xff\xff")
    check("BITPOS 0 all ones no end", c.cmd("BITPOS", "bp4", "0"), 16)
    check("BITPOS 0 all ones with end", c.cmd("BITPOS", "bp4", "0", "0", "1"), -1)
    check("BITPOS missing bit 1", c.cmd("BITPOS", "bp-missing", "1"), -1)
    check("BITPOS missing bit 0", c.cmd("BITPOS", "bp-missing", "0"), 0)

    # BITOP (Redis documentation example)
    c.cmd("SET", "k1", "foobar")
    c.cmd("SET", "k2", "abcdef")
    check("BITOP AND len", c.cmd("BITOP", "AND", "dest", "k1", "k2"), 6)
    check("BITOP AND value", c.cmd("GET", "dest"), b"`bc`ab")
    check("BITOP OR len", c.cmd("BITOP", "OR", "dest2", "k1", "k2"), 6)
    check("BITOP NOT", c.cmd("BITOP", "NOT", "dest3", "k1"), 6)
    check("BITOP NOT value", c.cmd("GET", "dest3"), bytes(~b & 0xFF for b in b"foobar"))
    check("BITOP NOT multi src rejected", c.cmd("BITOP", "NOT", "d", "k1", "k2"), pred=lambda g: is_err(g, "ERR"))
    check("BITOP empty result deletes dest", c.cmd("BITOP", "AND", "dest", "missing1", "missing2"), 0)
    check("BITOP dest gone", c.cmd("EXISTS", "dest"), 0)
    check("BITOP wrongtype source", c.cmd("BITOP", "OR", "d", "bc-list"), pred=lambda g: is_err(g, "WRONGTYPE"))

    # HyperLogLog: PFADD / PFCOUNT / PFMERGE (phase 3 package 1)
    # The sketch is a private dense format kept in a plain string value.
    check("PFADD doc example", c.cmd("PFADD", "hll", "a", "b", "c", "d", "e", "f", "g"), 1)
    check("PFCOUNT doc example", c.cmd("PFCOUNT", "hll"), 7)
    check("PFADD known element", c.cmd("PFADD", "hll", "a"), 0)

    check("PFADD no elements creates", c.cmd("PFADD", "hll-empty"), 1)
    check("PFCOUNT empty sketch", c.cmd("PFCOUNT", "hll-empty"), 0)
    check("PFADD no elements again", c.cmd("PFADD", "hll-empty"), 0)
    check("PFCOUNT missing key", c.cmd("PFCOUNT", "hll-gone"), 0)

    # Multi-key PFCOUNT is the cardinality of the union and touches nothing.
    left = ["u%d" % i for i in range(120)]
    right = ["u%d" % i for i in range(80, 200)]
    c.cmd("PFADD", "hll1", *left)
    c.cmd("PFADD", "hll2", *right)
    true_union = len(set(left) | set(right))
    before = c.cmd("PFCOUNT", "hll1")
    tolerance = max(1, int(round(true_union * 0.02)))
    check(
        "PFCOUNT union of two keys",
        c.cmd("PFCOUNT", "hll1", "hll2"),
        pred=lambda g: isinstance(g, int) and abs(g - true_union) <= tolerance,
    )
    check("PFCOUNT leaves sources alone", c.cmd("PFCOUNT", "hll1"), before)
    check("PFCOUNT union is order independent",
          c.cmd("PFCOUNT", "hll2", "hll1"), c.cmd("PFCOUNT", "hll1", "hll2"))

    # 10,000 distinct elements, within the documented 1.5% band.
    for base in range(0, 10000, 500):
        c.cmd("PFADD", "hll-big", *["e%d" % i for i in range(base, base + 500)])
    check(
        "PFCOUNT 10k within 1.5%",
        c.cmd("PFCOUNT", "hll-big"),
        pred=lambda g: isinstance(g, int) and abs(g - 10000) <= 150,
    )

    # PFMERGE
    c.cmd("PFADD", "hll-src1", *["m%d" % i for i in range(100)])
    c.cmd("PFADD", "hll-src2", *["m%d" % i for i in range(50, 150)])
    check("PFMERGE", c.cmd("PFMERGE", "hll-dest", "hll-src1", "hll-src2"), "OK")
    check(
        "PFMERGE count matches union",
        c.cmd("PFCOUNT", "hll-dest"),
        pred=lambda g: isinstance(g, int) and abs(g - 150) <= 3,
    )
    check("PFMERGE equals multi-key PFCOUNT",
          c.cmd("PFCOUNT", "hll-dest"), c.cmd("PFCOUNT", "hll-src1", "hll-src2"))
    check("PFMERGE is idempotent", c.cmd("PFMERGE", "hll-dest", "hll-src1"), "OK")
    check("PFMERGE idempotent count", c.cmd("PFCOUNT", "hll-dest"),
          c.cmd("PFCOUNT", "hll-src1", "hll-src2"))
    check("PFMERGE onto missing dest", c.cmd("PFMERGE", "hll-dest2", "hll-src1"), "OK")
    check("PFMERGE new dest count", c.cmd("PFCOUNT", "hll-dest2"), c.cmd("PFCOUNT", "hll-src1"))

    # The sketch is an ordinary string value for every other command.
    check("TYPE of a sketch", c.cmd("TYPE", "hll"), "string")
    check("GET of a sketch", c.cmd("GET", "hll"),
          pred=lambda g: isinstance(g, bytes) and g.startswith(b"MHLL"))
    check("STRLEN of a sketch", c.cmd("STRLEN", "hll"), 16400)
    check("DEL of a sketch", c.cmd("DEL", "hll"), 1)

    # A plain string is not a valid sketch; another type is a plain WRONGTYPE.
    c.cmd("SET", "hll-plain", "x")
    check("PFADD on a plain string", c.cmd("PFADD", "hll-plain", "a"),
          pred=lambda g: is_err(g, "WRONGTYPE Key is not a valid HyperLogLog"))
    check("PFCOUNT on a plain string", c.cmd("PFCOUNT", "hll-plain"),
          pred=lambda g: is_err(g, "WRONGTYPE Key is not a valid HyperLogLog"))
    check("PFMERGE from a plain string", c.cmd("PFMERGE", "hll-d3", "hll-plain"),
          pred=lambda g: is_err(g, "WRONGTYPE Key is not a valid HyperLogLog"))
    c.cmd("RPUSH", "hll-list", "x")
    check("PFADD on a list", c.cmd("PFADD", "hll-list", "a"),
          pred=lambda g: is_err(g, "WRONGTYPE Operation against"))
    check("PFCOUNT on a list", c.cmd("PFCOUNT", "hll-list"),
          pred=lambda g: is_err(g, "WRONGTYPE Operation against"))
    check("PFADD arity", c.cmd("PFADD"), pred=lambda g: is_err(g, "ERR wrong number"))
    check("PFCOUNT arity", c.cmd("PFCOUNT"), pred=lambda g: is_err(g, "ERR wrong number"))
    check("PFMERGE arity", c.cmd("PFMERGE"), pred=lambda g: is_err(g, "ERR wrong number"))

    # Hash-tag style key names and TTLs behave like any other string key.
    check("PFADD tagged key", c.cmd("PFADD", "{visits}:2026", "a", "b"), 1)
    check("EXPIRE on a sketch", c.cmd("EXPIRE", "{visits}:2026", "100"), 1)
    check("PFADD after EXPIRE", c.cmd("PFADD", "{visits}:2026", "c"), 1)
    check("PFCOUNT after EXPIRE", c.cmd("PFCOUNT", "{visits}:2026"), 3)
    check("PFADD keeps the TTL", c.cmd("TTL", "{visits}:2026"),
          pred=lambda g: isinstance(g, int) and 0 < g <= 100)

    # BITFIELD_RO
    c.cmd("SET", "bf", b"\x01\x02\xff")
    check("BITFIELD_RO u8 0", c.cmd("BITFIELD_RO", "bf", "GET", "u8", "0"), [1])
    check("BITFIELD_RO u16 0", c.cmd("BITFIELD_RO", "bf", "GET", "u16", "0"), [258])
    check("BITFIELD_RO i8 16", c.cmd("BITFIELD_RO", "bf", "GET", "i8", "16"), [-1])
    check("BITFIELD_RO u4 #1", c.cmd("BITFIELD_RO", "bf", "GET", "u4", "#1"), [1])
    check("BITFIELD_RO multi", c.cmd("BITFIELD_RO", "bf", "GET", "u8", "0", "GET", "u8", "8"), [1, 2])
    check("BITFIELD_RO beyond end", c.cmd("BITFIELD_RO", "bf", "GET", "u8", "100"), [0])
    check("BITFIELD_RO bad type", c.cmd("BITFIELD_RO", "bf", "GET", "u64", "0"), pred=lambda g: is_err(g, "ERR"))
    check("BITFIELD_RO rejects SET", c.cmd("BITFIELD_RO", "bf", "SET", "u8", "0", "1"), pred=lambda g: is_err(g, "ERR"))

    # BITFIELD (phase 3 package 2). Examples from the Redis documentation.
    check("BITFIELD doc example", c.cmd("BITFIELD", "bfw-doc", "INCRBY", "i5", "100", "1", "GET", "u4", "0"), [1, 0])

    # The documented overflow walk: WRAP on the first field, SAT on the second.
    for want in ([1, 1], [2, 2], [3, 3], [0, 3]):
        check("BITFIELD overflow walk %r" % want,
              c.cmd("BITFIELD", "bfw-ovf", "incrby", "u2", "100", "1", "OVERFLOW", "SAT", "incrby", "u2", "102", "1"),
              want)
    check("BITFIELD OVERFLOW FAIL returns nil", c.cmd("BITFIELD", "bfw-ovf", "OVERFLOW", "FAIL", "incrby", "u2", "102", "1"), [None])
    check("BITFIELD OVERFLOW FAIL wrote nothing", c.cmd("BITFIELD_RO", "bfw-ovf", "GET", "u2", "102"), [3])

    # SET replies with the previous value, GET with the current one.
    check("BITFIELD SET on a new key", c.cmd("BITFIELD", "bfw-set", "SET", "u8", "0", "255"), [0])
    check("BITFIELD SET returns the old value", c.cmd("BITFIELD", "bfw-set", "SET", "u8", "0", "1"), [255])
    check("BITFIELD GET after SET", c.cmd("BITFIELD", "bfw-set", "GET", "u8", "0"), [1])
    check("BITFIELD SET creates the key", c.cmd("EXISTS", "bfw-set"), 1)
    check("BITFIELD SET sizes the string", c.cmd("STRLEN", "bfw-set"), 1)

    # Signed wraparound, saturation and failure on the same value.
    check("BITFIELD SET i8 127", c.cmd("BITFIELD", "bfw-wrap", "SET", "i8", "0", "127"), [0])
    check("BITFIELD INCRBY wraps", c.cmd("BITFIELD", "bfw-wrap", "INCRBY", "i8", "0", "1"), [-128])
    check("BITFIELD SET i8 127 again", c.cmd("BITFIELD", "bfw-sat", "SET", "i8", "0", "127"), [0])
    check("BITFIELD INCRBY saturates", c.cmd("BITFIELD", "bfw-sat", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "1"), [127])
    check("BITFIELD SAT left the value at the max", c.cmd("BITFIELD", "bfw-sat", "GET", "i8", "0"), [127])
    check("BITFIELD SET i8 127 once more", c.cmd("BITFIELD", "bfw-fail", "SET", "i8", "0", "127"), [0])
    check("BITFIELD INCRBY fails", c.cmd("BITFIELD", "bfw-fail", "OVERFLOW", "FAIL", "INCRBY", "i8", "0", "1"), [None])
    check("BITFIELD FAIL left the value alone", c.cmd("BITFIELD", "bfw-fail", "GET", "i8", "0"), [127])
    # SAT clamps a SET whose value does not fit the type, FAIL refuses it.
    check("BITFIELD SAT clamps a SET", c.cmd("BITFIELD", "bfw-satset", "OVERFLOW", "SAT", "SET", "u2", "0", "9", "GET", "u2", "0"), [0, 3])
    check("BITFIELD WRAP wraps a SET", c.cmd("BITFIELD", "bfw-wrapset", "SET", "u2", "0", "9", "GET", "u2", "0"), [0, 1])

    # "#" offsets address the n-th field of the given width.
    check("BITFIELD # offset", c.cmd("BITFIELD", "bfw-hash", "SET", "u8", "#1", "7", "GET", "u8", "#1"), [0, 7])
    check("BITFIELD # offset is bit 8", c.cmd("BITFIELD", "bfw-hash", "GET", "u8", "8"), [7])

    # OVERFLOW applies to the subcommands after it and produces no reply item.
    check("BITFIELD mixed ordering", c.cmd("BITFIELD", "bfw-mix", "SET", "u8", "0", "250", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "10", "GET", "u8", "0"), [0, 255, 255])

    # An all-GET BITFIELD is read-only and matches BITFIELD_RO; no key is made.
    check("BITFIELD all GET", c.cmd("BITFIELD", "bf", "GET", "u8", "0", "GET", "u8", "8"), [1, 2])
    check("BITFIELD all GET matches BITFIELD_RO", c.cmd("BITFIELD_RO", "bf", "GET", "u8", "0", "GET", "u8", "8"), [1, 2])
    check("BITFIELD all GET on a missing key", c.cmd("BITFIELD", "bfw-missing", "GET", "u8", "0"), [0])
    check("BITFIELD all GET created nothing", c.cmd("EXISTS", "bfw-missing"), 0)
    check("BITFIELD with no subcommands", c.cmd("BITFIELD", "bfw-missing"), [])

    # i64 round-trips a large negative value.
    check("BITFIELD SET i64 min", c.cmd("BITFIELD", "bfw-i64", "SET", "i64", "0", "-9223372036854775808"), [0])
    check("BITFIELD GET i64 min", c.cmd("BITFIELD", "bfw-i64", "GET", "i64", "0"), [-9223372036854775808])

    # Like Redis, a BITFIELD holding any write grows (or creates) the string up
    # front, even when every write then fails its overflow check.
    check("BITFIELD FAIL still creates the key", c.cmd("BITFIELD", "bfw-fresh", "OVERFLOW", "FAIL", "SET", "u8", "0", "300"), [None])
    check("BITFIELD FAIL created an empty byte", c.cmd("GET", "bfw-fresh"), b"\x00")

    # Errors, verbatim from Redis.
    check("BITFIELD bad type", c.cmd("BITFIELD", "bfw-err", "SET", "u64", "0", "1"),
          pred=lambda g: is_err(g, "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."))
    check("BITFIELD bad offset", c.cmd("BITFIELD", "bfw-err", "SET", "u8", "-1", "1"),
          pred=lambda g: is_err(g, "ERR bit offset is not an integer or out of range"))
    check("BITFIELD bad overflow", c.cmd("BITFIELD", "bfw-err", "OVERFLOW", "BOGUS", "GET", "u8", "0"),
          pred=lambda g: is_err(g, "ERR Invalid OVERFLOW type specified"))
    check("BITFIELD bad value", c.cmd("BITFIELD", "bfw-err", "SET", "u8", "0", "nope"),
          pred=lambda g: is_err(g, "ERR value is not an integer or out of range"))
    check("BITFIELD bad increment", c.cmd("BITFIELD", "bfw-err", "INCRBY", "u8", "0", "1.5"),
          pred=lambda g: is_err(g, "ERR value is not an integer or out of range"))
    check("BITFIELD unknown subcommand", c.cmd("BITFIELD", "bfw-err", "DEL", "u8", "0"),
          pred=lambda g: is_err(g, "ERR syntax error"))
    check("BITFIELD arity", c.cmd("BITFIELD"), pred=lambda g: is_err(g, "ERR wrong number"))
    check("BITFIELD errors write nothing", c.cmd("EXISTS", "bfw-err"), 0)
    check("BITFIELD wrongtype", c.cmd("BITFIELD", "bc-list", "SET", "u8", "0", "1"),
          pred=lambda g: is_err(g, "WRONGTYPE"))
    check("BITFIELD wrongtype read-only", c.cmd("BITFIELD", "bc-list", "GET", "u8", "0"),
          pred=lambda g: is_err(g, "WRONGTYPE"))

    # A write keeps the TTL the key already had, like SETBIT.
    c.cmd("SET", "bfw-ttl", "abc")
    c.cmd("EXPIRE", "bfw-ttl", "100")
    check("BITFIELD SET on a key with a TTL", c.cmd("BITFIELD", "bfw-ttl", "SET", "u8", "0", "65"), [97])
    check("BITFIELD kept the TTL", c.cmd("TTL", "bfw-ttl"), pred=lambda g: isinstance(g, int) and 0 < g <= 100)
    check("BITFIELD wrote the byte", c.cmd("GET", "bfw-ttl"), b"Abc")

    # OBJECT
    check("OBJECT ENCODING string", c.cmd("OBJECT", "ENCODING", "bc"), b"raw")
    check("OBJECT ENCODING list", c.cmd("OBJECT", "ENCODING", "sl"), b"quicklist")
    check("OBJECT ENCODING missing", c.cmd("OBJECT", "ENCODING", "nope"), None)
    check("OBJECT REFCOUNT", c.cmd("OBJECT", "REFCOUNT", "bc"), 1)
    check("OBJECT HELP", c.cmd("OBJECT", "HELP"), pred=lambda g: isinstance(g, list) and len(g) > 3)
    check("OBJECT IDLETIME rejected", c.cmd("OBJECT", "IDLETIME", "bc"), pred=lambda g: is_err(g, "ERR"))

    # MEMORY USAGE
    check("MEMORY USAGE string", c.cmd("MEMORY", "USAGE", "bc"), pred=lambda g: isinstance(g, int) and g > 6)
    check("MEMORY USAGE missing", c.cmd("MEMORY", "USAGE", "nope"), None)

    # SLOWLOG / LATENCY / ACL shims
    check("SLOWLOG GET", c.cmd("SLOWLOG", "GET"), [])
    check("SLOWLOG LEN", c.cmd("SLOWLOG", "LEN"), 0)
    check("SLOWLOG RESET", c.cmd("SLOWLOG", "RESET"), "OK")
    check("LATENCY LATEST", c.cmd("LATENCY", "LATEST"), [])
    check("LATENCY HISTORY", c.cmd("LATENCY", "HISTORY", "command"), [])
    check("LATENCY DOCTOR", c.cmd("LATENCY", "DOCTOR"), pred=lambda g: isinstance(g, bytes) and b"Dave" in g)
    check("ACL WHOAMI", c.cmd("ACL", "WHOAMI"), b"default")
    check("ACL USERS", c.cmd("ACL", "USERS"), [b"default"])
    check("ACL LIST", c.cmd("ACL", "LIST"), pred=lambda g: isinstance(g, list) and g[0].startswith(b"user default on"))
    check("ACL GETUSER default", c.cmd("ACL", "GETUSER", "default"), pred=lambda g: isinstance(g, list) and len(g) == 12)
    check("ACL GETUSER other", c.cmd("ACL", "GETUSER", "nobody"), None)
    check("ACL SETUSER rejected", c.cmd("ACL", "SETUSER", "x"), pred=lambda g: is_err(g, "ERR"))

    # CONFIG GET extras / INFO keyspace
    check("CONFIG GET maxmemory-policy", c.cmd("CONFIG", "GET", "maxmemory-policy"), [b"maxmemory-policy", b"noeviction"])
    check("CONFIG GET *", c.cmd("CONFIG", "GET", "*"), pred=lambda g: isinstance(g, list) and len(g) == 24)
    check("CONFIG GET port", c.cmd("CONFIG", "GET", "port"), [b"port", str(port).encode()])
    info = c.cmd("INFO", "keyspace")
    check("INFO keyspace", info, pred=lambda g: isinstance(g, bytes) and b"# Keyspace" in g and b"db0:keys=" in g)
    info_all = c.cmd("INFO")
    check("INFO default includes keyspace", info_all, pred=lambda g: b"db0:keys=" in g)

    # DUMP / RESTORE for strings, sets, zsets (plus TTL) and regressions for list/hash
    c.cmd("SET", "s1", "hello")
    payload = c.cmd("DUMP", "s1")
    check("DUMP string payload", payload, pred=lambda g: isinstance(g, bytes) and g.startswith(b"MAKO_STRING_DUMP"))
    check("RESTORE string", c.cmd("RESTORE", "s2", "0", payload), "OK")
    check("RESTORE string value", c.cmd("GET", "s2"), b"hello")
    check("RESTORE string ttl", c.cmd("RESTORE", "s3", "5000", payload), "OK")
    check("RESTORE string ttl applied", c.cmd("PTTL", "s3"), pred=lambda g: isinstance(g, int) and 0 < g <= 5000)
    check("RESTORE string ABSTTL", c.cmd("RESTORE", "s4", str(int(time.time() * 1000) + 60000), payload, "ABSTTL"), "OK")
    check("RESTORE string ABSTTL applied", c.cmd("PTTL", "s4"), pred=lambda g: isinstance(g, int) and 50000 < g <= 60000)

    c.cmd("SADD", "st", "a", "b", "c")
    payload = c.cmd("DUMP", "st")
    check("DUMP set payload", payload, pred=lambda g: isinstance(g, bytes) and g.startswith(b"MAKO_SET_DUMP"))
    c.cmd("SET", "st2", "overwritten-string")
    check("RESTORE set replaces", c.cmd("RESTORE", "st2", "0", payload, "REPLACE"), "OK")
    check("RESTORE set members", sorted(c.cmd("SMEMBERS", "st2")), [b"a", b"b", b"c"])
    check("RESTORE set type", c.cmd("TYPE", "st2"), "set")

    c.cmd("ZADD", "z", "1", "a", "2.5", "b")
    payload = c.cmd("DUMP", "z")
    check("DUMP zset payload", payload, pred=lambda g: isinstance(g, bytes) and g.startswith(b"MAKO_ZSET_DUMP"))
    check("RESTORE zset", c.cmd("RESTORE", "z2", "3000", payload), "OK")
    check("RESTORE zset members", c.cmd("ZRANGE", "z2", "0", "-1", "WITHSCORES"), [b"a", b"1", b"b", b"2.5"])
    check("RESTORE zset ttl", c.cmd("PTTL", "z2"), pred=lambda g: isinstance(g, int) and 0 < g <= 3000)

    c.cmd("RPUSH", "l1", "x", "y")
    payload = c.cmd("DUMP", "l1")
    check("RESTORE list (regression)", c.cmd("RESTORE", "l2", "0", payload), "OK")
    check("RESTORE list contents", c.cmd("LRANGE", "l2", "0", "-1"), [b"x", b"y"])
    c.cmd("HSET", "h1", "f", "v")
    payload = c.cmd("DUMP", "h1")
    check("RESTORE hash (regression)", c.cmd("RESTORE", "h2", "0", payload), "OK")
    check("RESTORE hash contents", c.cmd("HGETALL", "h2"), [b"f", b"v"])
    check("RESTORE bad payload", c.cmd("RESTORE", "bad", "0", "garbage"), pred=lambda g: is_err(g, "ERR"))

    # Sharded Pub/Sub on two connections
    sub = Resp(host, port)
    check("SSUBSCRIBE ack", sub.cmd("SSUBSCRIBE", "shard-ch"), [b"ssubscribe", b"shard-ch", 1])
    check("PUBSUB SHARDCHANNELS", c.cmd("PUBSUB", "SHARDCHANNELS"), [b"shard-ch"])
    check("PUBSUB SHARDNUMSUB", c.cmd("PUBSUB", "SHARDNUMSUB", "shard-ch"), [b"shard-ch", 1])
    check("PUBSUB CHANNELS excludes shard-only", c.cmd("PUBSUB", "NUMSUB", "shard-ch"), [b"shard-ch", 1])
    check("SPUBLISH delivers", c.cmd("SPUBLISH", "shard-ch", "hi"), 1)
    check("smessage received", sub.read(), [b"smessage", b"shard-ch", b"hi"])
    check("SUNSUBSCRIBE", sub.cmd("SUNSUBSCRIBE", "shard-ch"), [b"sunsubscribe", b"shard-ch", 0])
    check("PUBSUB SHARDCHANNELS empty", c.cmd("PUBSUB", "SHARDCHANNELS"), [])
    check("subscriber left subscriber mode", sub.cmd("PING"), "PONG")

    # Geo commands (Redis documentation examples: Palermo / Catania / Agrigento)
    def near(got, want, tol):
        return isinstance(got, (int, float)) and abs(got - want) <= tol

    def coord_near(got, lon, lat):
        return (
            isinstance(got, list)
            and len(got) == 2
            and near(float(got[0]), lon, 1e-4)
            and near(float(got[1]), lat, 1e-4)
        )

    check("GEOADD two members", c.cmd("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo",
                                      "15.087269", "37.502669", "Catania"), 2)
    check("GEOADD repeated adds nothing", c.cmd("GEOADD", "Sicily", "13.361389", "38.115556", "Palermo"), 0)
    check("GEOADD CH counts a move", c.cmd("GEOADD", "Sicily", "CH", "13.5", "38.2", "Palermo"), 1)
    check("GEOADD CH restores Palermo", c.cmd("GEOADD", "Sicily", "CH", "13.361389", "38.115556", "Palermo"), 1)
    check("GEOADD NX keeps the old position", c.cmd("GEOADD", "Sicily", "NX", "1", "1", "Palermo"), 0)
    check("GEOADD NX position unchanged", c.cmd("GEOPOS", "Sicily", "Palermo"),
          pred=lambda g: coord_near(g[0], 13.361389, 38.115556))
    check("GEOADD NX adds a new member", c.cmd("GEOADD", "Sicily", "NX", "12.0", "38.0", "Marsala"), 1)
    check("GEOADD XX ignores a new member", c.cmd("GEOADD", "Sicily", "XX", "12.5", "38.5", "Trapani"), 0)
    check("GEOADD XX moves an existing one", c.cmd("GEOADD", "Sicily", "XX", "CH", "12.1", "38.1", "Marsala"), 1)
    check("GEOADD XX did not create Trapani", c.cmd("ZSCORE", "Sicily", "Trapani"), None)
    check("GEOADD cleanup", c.cmd("ZREM", "Sicily", "Marsala"), 1)

    # A geo set is an ordinary sorted set.
    check("TYPE of a geo key", c.cmd("TYPE", "Sicily"), "zset")
    check("ZSCORE Palermo is the 52-bit geohash", c.cmd("ZSCORE", "Sicily", "Palermo"), b"3479099956230698")
    check("ZSCORE Catania is the 52-bit geohash", c.cmd("ZSCORE", "Sicily", "Catania"), b"3479447370796909")

    # GEODIST
    check("GEODIST meters", c.cmd("GEODIST", "Sicily", "Palermo", "Catania"), b"166274.1516")
    check("GEODIST km", c.cmd("GEODIST", "Sicily", "Palermo", "Catania", "km"), b"166.2742")
    check("GEODIST mi", c.cmd("GEODIST", "Sicily", "Palermo", "Catania", "mi"), b"103.3182")
    check("GEODIST ft", c.cmd("GEODIST", "Sicily", "Palermo", "Catania", "ft"),
          pred=lambda g: near(float(g), 166274.1516 / 0.3048, 0.1))
    check("GEODIST missing member", c.cmd("GEODIST", "Sicily", "Palermo", "Foo"), None)
    check("GEODIST missing key", c.cmd("GEODIST", "geo-missing", "a", "b"), None)

    # GEOHASH
    check("GEOHASH strings", c.cmd("GEOHASH", "Sicily", "Palermo", "Catania"),
          [b"sqc8b49rny0", b"sqdtr74hyu0"])
    check("GEOHASH missing member", c.cmd("GEOHASH", "Sicily", "NonExisting"), [None])
    check("GEOHASH missing key", c.cmd("GEOHASH", "geo-missing", "a"), [None])

    # GEOPOS
    pos = c.cmd("GEOPOS", "Sicily", "Palermo", "Catania", "NonExisting")
    check("GEOPOS shape", pos, pred=lambda g: isinstance(g, list) and len(g) == 3 and g[2] is None)
    check("GEOPOS Palermo", pos[0], pred=lambda g: coord_near(g, 13.361389, 38.115556))
    check("GEOPOS Catania", pos[1], pred=lambda g: coord_near(g, 15.087269, 37.502669))
    check("GEOPOS missing key", c.cmd("GEOPOS", "geo-missing", "a"), [None])

    # GEORADIUS (documentation example: centre 15 37, radius 200 km)
    check("GEORADIUS names", sorted(c.cmd("GEORADIUS", "Sicily", "15", "37", "200", "km")),
          [b"Catania", b"Palermo"])
    withdist = c.cmd("GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST")
    dists = {row[0]: float(row[1]) for row in withdist}
    check("GEORADIUS WITHDIST members", sorted(dists), [b"Catania", b"Palermo"])
    check("GEORADIUS WITHDIST Palermo", dists.get(b"Palermo"), pred=lambda g: near(g, 190.4424, 0.01))
    check("GEORADIUS WITHDIST Catania", dists.get(b"Catania"), pred=lambda g: near(g, 56.4413, 0.01))
    withcoord = {row[0]: row[1] for row in c.cmd("GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHCOORD")}
    check("GEORADIUS WITHCOORD Palermo", withcoord.get(b"Palermo"),
          pred=lambda g: coord_near(g, 13.361389, 38.115556))
    check("GEORADIUS WITHCOORD Catania", withcoord.get(b"Catania"),
          pred=lambda g: coord_near(g, 15.087269, 37.502669))
    both = c.cmd("GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST", "WITHCOORD")
    check("GEORADIUS WITHDIST WITHCOORD shape", both,
          pred=lambda g: isinstance(g, list) and len(g) == 2 and all(len(row) == 3 for row in g)
          and all(isinstance(row[2], list) and len(row[2]) == 2 for row in g))
    withhash = {row[0]: row[1] for row in c.cmd("GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHHASH")}
    check("GEORADIUS WITHHASH Palermo", withhash.get(b"Palermo"), 3479099956230698)
    check("GEORADIUS COUNT 1 ASC", c.cmd("GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "1", "ASC"),
          [b"Catania"])
    check("GEORADIUS COUNT 1 picks the closest", c.cmd("GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "1"),
          [b"Catania"])
    check("GEORADIUS DESC", c.cmd("GEORADIUS", "Sicily", "15", "37", "200", "km", "DESC"),
          [b"Palermo", b"Catania"])
    check("GEORADIUS small radius", c.cmd("GEORADIUS", "Sicily", "15", "37", "1", "km"), [])
    check("GEORADIUS missing key", c.cmd("GEORADIUS", "geo-missing", "15", "37", "200", "km"), [])
    check("GEORADIUS_RO", sorted(c.cmd("GEORADIUS_RO", "Sicily", "15", "37", "200", "km")),
          [b"Catania", b"Palermo"])

    # GEOSEARCH
    check("GEOSEARCH BYRADIUS ASC", c.cmd("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37",
                                          "BYRADIUS", "200", "km", "ASC"),
          [b"Catania", b"Palermo"])
    box = c.cmd("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km",
                "ASC", "WITHCOORD", "WITHDIST")
    check("GEOSEARCH BYBOX order", [row[0] for row in box], [b"Catania", b"Palermo"])
    check("GEOSEARCH BYBOX shape", box,
          pred=lambda g: all(len(row) == 3 and isinstance(row[2], list) and len(row[2]) == 2 for row in g))
    check("GEOSEARCH BYBOX Catania distance", float(box[0][1]), pred=lambda g: near(g, 56.4413, 0.01))
    check("GEOSEARCH BYBOX Palermo coord", box[1][2], pred=lambda g: coord_near(g, 13.361389, 38.115556))
    check("GEOSEARCH FROMMEMBER", sorted(c.cmd("GEOSEARCH", "Sicily", "FROMMEMBER", "Palermo",
                                               "BYRADIUS", "200", "km")),
          pred=lambda g: b"Catania" in g and b"Palermo" in g)
    check("GEOSEARCH COUNT 1 ANY", c.cmd("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37",
                                         "BYRADIUS", "200", "km", "COUNT", "1", "ANY"),
          pred=lambda g: isinstance(g, list) and len(g) == 1)
    check("GEOSEARCH DESC", c.cmd("GEOSEARCH", "Sicily", "FROMLONLAT", "15", "37",
                                  "BYRADIUS", "200", "km", "DESC"),
          [b"Palermo", b"Catania"])
    check("GEOSEARCH missing key", c.cmd("GEOSEARCH", "geo-missing", "FROMLONLAT", "15", "37",
                                         "BYRADIUS", "200", "km"), [])
    check("GEOSEARCH FROMMEMBER missing",
          c.cmd("GEOSEARCH", "Sicily", "FROMMEMBER", "NonExisting", "BYRADIUS", "200", "km"),
          pred=lambda g: is_err(g, "ERR") and "could not decode requested zset member" in str(g))

    # GEORADIUSBYMEMBER (documentation example)
    check("GEOADD Agrigento", c.cmd("GEOADD", "Sicily", "13.583333", "37.316667", "Agrigento"), 1)
    check("GEORADIUSBYMEMBER", sorted(c.cmd("GEORADIUSBYMEMBER", "Sicily", "Agrigento", "100", "km")),
          [b"Agrigento", b"Palermo"])
    check("GEORADIUSBYMEMBER_RO", sorted(c.cmd("GEORADIUSBYMEMBER_RO", "Sicily", "Agrigento", "100", "km")),
          [b"Agrigento", b"Palermo"])
    check("GEORADIUSBYMEMBER missing member", c.cmd("GEORADIUSBYMEMBER", "Sicily", "NonExisting", "100", "km"),
          pred=lambda g: is_err(g, "ERR") and "could not decode requested zset member" in str(g))
    # A key that does not exist searches an empty set; only a missing member of
    # an existing key is an error (Redis tests/unit/geo.tcl).
    check("GEORADIUSBYMEMBER missing key", c.cmd("GEORADIUSBYMEMBER", "geo-missing", "member", "100", "km"), [])
    check("GEORADIUSBYMEMBER_RO missing key", c.cmd("GEORADIUSBYMEMBER_RO", "geo-missing", "member", "1", "km"), [])
    check("GEOSEARCH FROMMEMBER missing key",
          c.cmd("GEOSEARCH", "geo-missing", "FROMMEMBER", "member", "BYBOX", "1", "1", "km"), [])

    # Geo error cases
    check("GEOADD invalid coordinates",
          c.cmd("GEOADD", "Sicily", "200", "100", "bad"),
          pred=lambda g: is_err(g, "ERR invalid longitude,latitude pair 200.000000,100.000000"))
    check("GEODIST bad unit", c.cmd("GEODIST", "Sicily", "Palermo", "Catania", "yards"),
          pred=lambda g: is_err(g, "ERR unsupported unit provided. please use M, KM, FT, MI"))
    check("GEORADIUS bad unit", c.cmd("GEORADIUS", "Sicily", "15", "37", "200", "yards"),
          pred=lambda g: is_err(g, "ERR unsupported unit provided. please use M, KM, FT, MI"))
    check("GEORADIUS negative radius", c.cmd("GEORADIUS", "Sicily", "15", "37", "-5", "km"),
          pred=lambda g: is_err(g, "ERR radius cannot be negative"))
    check("GEORADIUS rejects STORE", c.cmd("GEORADIUS", "Sicily", "15", "37", "200", "km", "STORE", "dst"),
          pred=lambda g: is_err(g, "ERR STORE option in GEORADIUS is not supported by this server"))
    check("GEORADIUS rejects STOREDIST", c.cmd("GEORADIUS", "Sicily", "15", "37", "200", "km", "STOREDIST", "dst"),
          pred=lambda g: is_err(g, "ERR STORE option in GEORADIUS is not supported by this server"))
    check("GEOSEARCHSTORE is unknown", c.cmd("GEOSEARCHSTORE", "dst", "Sicily", "FROMLONLAT", "15", "37",
                                             "BYRADIUS", "200", "km"),
          pred=lambda g: is_err(g, "ERR unknown command"))
    c.cmd("SET", "geo-string", "not-a-geo-set")
    check("GEOADD wrongtype", c.cmd("GEOADD", "geo-string", "13.0", "38.0", "x"),
          pred=lambda g: is_err(g, "WRONGTYPE"))
    check("GEOPOS wrongtype", c.cmd("GEOPOS", "geo-string", "x"), pred=lambda g: is_err(g, "WRONGTYPE"))
    check("GEODIST wrongtype", c.cmd("GEODIST", "geo-string", "a", "b"), pred=lambda g: is_err(g, "WRONGTYPE"))
    check("GEOHASH wrongtype", c.cmd("GEOHASH", "geo-string", "x"), pred=lambda g: is_err(g, "WRONGTYPE"))
    check("GEORADIUS wrongtype", c.cmd("GEORADIUS", "geo-string", "15", "37", "200", "km"),
          pred=lambda g: is_err(g, "WRONGTYPE"))
    check("GEORADIUSBYMEMBER wrongtype", c.cmd("GEORADIUSBYMEMBER", "geo-string", "x", "200", "km"),
          pred=lambda g: is_err(g, "WRONGTYPE"))

    # The geo set stayed a plain sorted set throughout.
    check("TYPE Sicily", c.cmd("TYPE", "Sicily"), "zset")
    check("ZCARD Sicily", c.cmd("ZCARD", "Sicily"), 3)
    check("ZRANGE Sicily by geohash score", c.cmd("ZRANGE", "Sicily", "0", "-1"),
          [b"Agrigento", b"Palermo", b"Catania"])

    # ----- Hash field expiration (Redis 7.4 HEXPIRE family) -----

    # Documentation example
    c.cmd("DEL", "hfe")
    check("HSET for hfe", c.cmd("HSET", "hfe", "f1", "v1", "f2", "v2", "f3", "v3"), 3)
    check("HEXPIRE sets one field", c.cmd("HEXPIRE", "hfe", "10", "FIELDS", "1", "f1"), [1])
    check("HTTL mixes ttl, no-ttl and missing", c.cmd("HTTL", "hfe", "FIELDS", "3", "f1", "f2", "nofield"),
          pred=lambda g: len(g) == 3 and 8 <= g[0] <= 10 and g[1] == -1 and g[2] == -2)
    check("HPEXPIRE sets milliseconds", c.cmd("HPEXPIRE", "hfe", "1500", "FIELDS", "1", "f2"), [1])
    check("HPTTL in range", c.cmd("HPTTL", "hfe", "FIELDS", "1", "f2"),
          pred=lambda g: len(g) == 1 and 1000 < g[0] <= 1500)
    now_s = int(time.time())
    check("HEXPIRETIME is an absolute unix second", c.cmd("HEXPIRETIME", "hfe", "FIELDS", "1", "f1"),
          pred=lambda g: len(g) == 1 and now_s + 8 <= g[0] <= now_s + 11)
    check("HPEXPIRETIME is an absolute unix millisecond",
          c.cmd("HPEXPIRETIME", "hfe", "FIELDS", "1", "f1"),
          pred=lambda g: len(g) == 1 and (now_s + 8) * 1000 <= g[0] <= (now_s + 11) * 1000)
    check("HEXPIRETIME and HPEXPIRETIME agree",
          (c.cmd("HEXPIRETIME", "hfe", "FIELDS", "1", "f1")[0],
           c.cmd("HPEXPIRETIME", "hfe", "FIELDS", "1", "f1")[0]),
          pred=lambda g: abs(g[0] * 1000 - g[1]) < 1000)
    check("HPERSIST removes and reports missing", c.cmd("HPERSIST", "hfe", "FIELDS", "2", "f1", "nofield"),
          [1, -2])
    check("HTTL after HPERSIST", c.cmd("HTTL", "hfe", "FIELDS", "1", "f1"), [-1])
    check("HPERSIST without expiration", c.cmd("HPERSIST", "hfe", "FIELDS", "1", "f1"), [-1])

    # Conditions
    c.cmd("DEL", "hfc")
    c.cmd("HSET", "hfc", "f1", "v1", "f2", "v2", "f3", "v3")
    check("HEXPIRE NX on a field without a TTL", c.cmd("HEXPIRE", "hfc", "100", "NX", "FIELDS", "1", "f1"), [1])
    check("HEXPIRE NX on a field with a TTL", c.cmd("HEXPIRE", "hfc", "200", "NX", "FIELDS", "1", "f1"), [0])
    check("HEXPIRE XX on a field with a TTL", c.cmd("HEXPIRE", "hfc", "300", "XX", "FIELDS", "1", "f1"), [1])
    check("HEXPIRE XX on a field without one", c.cmd("HEXPIRE", "hfc", "300", "XX", "FIELDS", "1", "f2"), [0])
    check("HEXPIRE GT raises only a larger time",
          c.cmd("HEXPIRE", "hfc", "400", "GT", "FIELDS", "2", "f1", "f2"), [1, 0])
    check("HEXPIRE GT refuses a smaller time", c.cmd("HEXPIRE", "hfc", "100", "GT", "FIELDS", "1", "f1"), [0])
    check("HEXPIRE LT lowers and sets a field with none",
          c.cmd("HEXPIRE", "hfc", "200", "LT", "FIELDS", "2", "f1", "f2"), [1, 1])
    check("HEXPIRE LT refuses a larger time", c.cmd("HEXPIRE", "hfc", "900", "LT", "FIELDS", "1", "f1"), [0])
    check("HEXPIRE mixes a condition with a missing field",
          c.cmd("HEXPIRE", "hfc", "50", "LT", "FIELDS", "2", "f1", "nofield"), [1, -2])

    # Time in the past deletes the field outright
    check("HEXPIRE 0 deletes the field", c.cmd("HEXPIRE", "hfe", "0", "FIELDS", "1", "f3"), [2])
    check("HEXISTS after HEXPIRE 0", c.cmd("HEXISTS", "hfe", "f3"), 0)
    check("HEXPIREAT in the past deletes the field",
          c.cmd("HEXPIREAT", "hfe", "1", "FIELDS", "1", "f2"), [2])
    check("HGET after a past HEXPIREAT", c.cmd("HGET", "hfe", "f2"), None)
    check("HPEXPIREAT in the past deletes the field",
          c.cmd("HPEXPIREAT", "hfe", "1000", "FIELDS", "1", "f1"), [2])
    check("the hash is gone once its last field went", c.cmd("EXISTS", "hfe"), 0)

    # Lazy expiry: an expired field is absent everywhere
    c.cmd("DEL", "hlazy")
    c.cmd("HSET", "hlazy", "f1", "v1", "f2", "v2", "f3", "v3")
    check("HPEXPIRE for the lazy check", c.cmd("HPEXPIRE", "hlazy", "150", "FIELDS", "1", "f1"), [1])
    time.sleep(0.3)
    check("HGET of an expired field", c.cmd("HGET", "hlazy", "f1"), None)
    check("HEXISTS of an expired field", c.cmd("HEXISTS", "hlazy", "f1"), 0)
    check("HSTRLEN of an expired field", c.cmd("HSTRLEN", "hlazy", "f1"), 0)
    check("HMGET skips an expired field", c.cmd("HMGET", "hlazy", "f1", "f2"), [None, b"v2"])
    check("HGETALL omits an expired field", sorted(c.cmd("HGETALL", "hlazy")),
          sorted([b"f2", b"v2", b"f3", b"v3"]))
    check("HLEN excludes an expired field", c.cmd("HLEN", "hlazy"), 2)
    check("HKEYS omits an expired field", sorted(c.cmd("HKEYS", "hlazy")), [b"f2", b"f3"])
    check("HVALS omits an expired field", sorted(c.cmd("HVALS", "hlazy")), [b"v2", b"v3"])
    check("HSCAN omits an expired field", sorted(c.cmd("HSCAN", "hlazy", "0")[1]),
          sorted([b"f2", b"v2", b"f3", b"v3"]))
    check("HTTL of an expired field reports no field", c.cmd("HTTL", "hlazy", "FIELDS", "1", "f1"), [-2])
    randomized = set()
    for _ in range(20):
        randomized.add(c.cmd("HRANDFIELD", "hlazy"))
    check("HRANDFIELD never returns an expired field", randomized, pred=lambda g: g == {b"f2", b"f3"})
    check("HRANDFIELD with a count omits it too", sorted(c.cmd("HRANDFIELD", "hlazy", "10")),
          [b"f2", b"f3"])

    # The key disappears when its last field expires
    c.cmd("DEL", "hsolo")
    c.cmd("HSET", "hsolo", "only", "v")
    check("HPEXPIRE the only field", c.cmd("HPEXPIRE", "hsolo", "100", "FIELDS", "1", "only"), [1])
    time.sleep(0.3)
    check("EXISTS after the last field expired", c.cmd("EXISTS", "hsolo"), 0)
    check("TYPE after the last field expired", c.cmd("TYPE", "hsolo"), "none")
    check("HGETALL after the last field expired", c.cmd("HGETALL", "hsolo"), [])
    check("HLEN after the last field expired", c.cmd("HLEN", "hsolo"), 0)

    # Writes and the field TTL
    c.cmd("DEL", "hw")
    c.cmd("HSET", "hw", "f1", "v1", "f2", "10")
    c.cmd("HEXPIRE", "hw", "100", "FIELDS", "2", "f1", "f2")
    check("HSET discards the field TTL", c.cmd("HSET", "hw", "f1", "v2"), 0)
    check("HTTL after the overwrite", c.cmd("HTTL", "hw", "FIELDS", "1", "f1"), [-1])
    check("HINCRBY preserves the field TTL", c.cmd("HINCRBY", "hw", "f2", "5"), 15)
    check("HTTL after HINCRBY", c.cmd("HTTL", "hw", "FIELDS", "1", "f2"),
          pred=lambda g: len(g) == 1 and 90 <= g[0] <= 100)
    check("HINCRBYFLOAT preserves the field TTL", c.cmd("HINCRBYFLOAT", "hw", "f2", "0.5"), b"15.5")
    check("HTTL after HINCRBYFLOAT", c.cmd("HTTL", "hw", "FIELDS", "1", "f2"),
          pred=lambda g: len(g) == 1 and 90 <= g[0] <= 100)
    check("HDEL removes the field TTL", c.cmd("HDEL", "hw", "f2"), 1)
    check("HSET recreates the field without a TTL", c.cmd("HSET", "hw", "f2", "v2"), 1)
    check("HTTL of the recreated field", c.cmd("HTTL", "hw", "FIELDS", "1", "f2"), [-1])
    c.cmd("HEXPIRE", "hw", "100", "FIELDS", "1", "f2")
    c.cmd("DEL", "hw")
    c.cmd("HSET", "hw", "f2", "v2")
    check("DEL drops every field TTL", c.cmd("HTTL", "hw", "FIELDS", "1", "f2"), [-1])
    check("HSETNX on a new field has no TTL",
          (c.cmd("HSETNX", "hw", "f9", "v9"), c.cmd("HTTL", "hw", "FIELDS", "1", "f9")), (1, [-1]))
    c.cmd("HEXPIRE", "hw", "100", "FIELDS", "1", "f9")
    check("HSETNX leaves an existing field's TTL alone",
          (c.cmd("HSETNX", "hw", "f9", "other"), c.cmd("HTTL", "hw", "FIELDS", "1", "f9")),
          pred=lambda g: g[0] == 0 and 90 <= g[1][0] <= 100)

    # A write over an expired field replaces it in place: the field counts as
    # new, and the hash stays readable afterwards.
    c.cmd("DEL", "hover")
    c.cmd("HSET", "hover", "f1", "v1", "f2", "keep")
    c.cmd("HPEXPIRE", "hover", "10", "FIELDS", "1", "f1")
    time.sleep(0.05)
    check("HSET over an expired field counts it as new", c.cmd("HSET", "hover", "f1", "v2"), 1)
    check("HGET after writing over an expired field", c.cmd("HGET", "hover", "f1"), b"v2")
    check("HTTL after writing over an expired field", c.cmd("HTTL", "hover", "FIELDS", "1", "f1"), [-1])
    check("HLEN after writing over an expired field", c.cmd("HLEN", "hover"), 2)
    check("HGETALL after writing over an expired field", sorted(c.cmd("HGETALL", "hover")),
          sorted([b"f1", b"v2", b"f2", b"keep"]))
    check("DEL after writing over an expired field", c.cmd("DEL", "hover"), 1)
    c.cmd("DEL", "hover2")
    c.cmd("HSET", "hover2", "f1", "10")
    c.cmd("HPEXPIRE", "hover2", "10", "FIELDS", "1", "f1")
    time.sleep(0.05)
    check("HINCRBY over an expired field restarts from zero", c.cmd("HINCRBY", "hover2", "f1", "1"), 1)
    check("HTTL after HINCRBY over an expired field", c.cmd("HTTL", "hover2", "FIELDS", "1", "f1"), [-1])
    check("EXISTS after HINCRBY over an expired field", c.cmd("EXISTS", "hover2"), 1)
    check("DEL after HINCRBY over an expired field", c.cmd("DEL", "hover2"), 1)
    c.cmd("DEL", "hover3")
    c.cmd("HSET", "hover3", "f1", "10")
    c.cmd("HPEXPIRE", "hover3", "10", "FIELDS", "1", "f1")
    time.sleep(0.05)
    check("HINCRBYFLOAT over an expired field restarts from zero",
          c.cmd("HINCRBYFLOAT", "hover3", "f1", "1.5"), b"1.5")
    check("HSETNX writes over an expired field",
          (c.cmd("HPEXPIRE", "hover3", "10", "FIELDS", "1", "f1"), time.sleep(0.05),
           c.cmd("HSETNX", "hover3", "f1", "fresh"), c.cmd("HGET", "hover3", "f1")),
          pred=lambda g: g[0] == [1] and g[2] == 1 and g[3] == b"fresh")
    check("HLEN after HSETNX over an expired field", c.cmd("HLEN", "hover3"), 1)
    check("DEL after HSETNX over an expired field", c.cmd("DEL", "hover3"), 1)

    # A key-level TTL and field TTLs coexist
    c.cmd("DEL", "hboth")
    c.cmd("HSET", "hboth", "f1", "v1", "f2", "v2")
    check("EXPIRE on the hash", c.cmd("EXPIRE", "hboth", "500"), 1)
    check("HEXPIRE on one of its fields", c.cmd("HEXPIRE", "hboth", "100", "FIELDS", "1", "f1"), [1])
    check("the key TTL is untouched", c.cmd("TTL", "hboth"), pred=lambda g: 490 <= g <= 500)
    check("the field TTL is untouched", c.cmd("HTTL", "hboth", "FIELDS", "2", "f1", "f2"),
          pred=lambda g: 90 <= g[0] <= 100 and g[1] == -1)
    check("PERSIST leaves the field TTLs alone",
          (c.cmd("PERSIST", "hboth"), c.cmd("TTL", "hboth")), (1, -1))
    check("the field TTL survives PERSIST", c.cmd("HTTL", "hboth", "FIELDS", "1", "f1"),
          pred=lambda g: 90 <= g[0] <= 100)

    # Missing key, wrong type and argument errors
    check("HEXPIRE on a missing key", c.cmd("HEXPIRE", "hfe-missing", "10", "FIELDS", "2", "a", "b"),
          [-2, -2])
    check("HTTL on a missing key", c.cmd("HTTL", "hfe-missing", "FIELDS", "1", "a"), [-2])
    check("HPTTL on a missing key", c.cmd("HPTTL", "hfe-missing", "FIELDS", "1", "a"), [-2])
    check("HEXPIRETIME on a missing key", c.cmd("HEXPIRETIME", "hfe-missing", "FIELDS", "1", "a"), [-2])
    check("HPEXPIRETIME on a missing key", c.cmd("HPEXPIRETIME", "hfe-missing", "FIELDS", "1", "a"), [-2])
    check("HPERSIST on a missing key", c.cmd("HPERSIST", "hfe-missing", "FIELDS", "1", "a"), [-2])
    c.cmd("SET", "hfe-string", "plain")
    for name, args in (("HEXPIRE", ("10", "FIELDS", "1", "f")),
                       ("HPEXPIRE", ("10", "FIELDS", "1", "f")),
                       ("HEXPIREAT", (str(int(time.time()) + 100), "FIELDS", "1", "f")),
                       ("HPEXPIREAT", (str(int(time.time() * 1000) + 100000), "FIELDS", "1", "f")),
                       ("HTTL", ("FIELDS", "1", "f")),
                       ("HPTTL", ("FIELDS", "1", "f")),
                       ("HEXPIRETIME", ("FIELDS", "1", "f")),
                       ("HPEXPIRETIME", ("FIELDS", "1", "f")),
                       ("HPERSIST", ("FIELDS", "1", "f"))):
        check("%s wrongtype" % name, c.cmd(name, "hfe-string", *args),
              pred=lambda g: is_err(g, "WRONGTYPE"))
    check("HEXPIRE numfields 0", c.cmd("HEXPIRE", "hfc", "10", "FIELDS", "0", "f1"),
          pred=lambda g: is_err(g, "ERR Parameter `numFields` should be greater than 0"))
    check("HEXPIRE numfields too large", c.cmd("HEXPIRE", "hfc", "10", "FIELDS", "4", "f1", "f2", "f3"),
          pred=lambda g: is_err(g, "ERR The `numfields` parameter must match the number of arguments"))
    check("HEXPIRE numfields too small", c.cmd("HEXPIRE", "hfc", "10", "FIELDS", "2", "f1", "f2", "f3"),
          pred=lambda g: is_err(g, "ERR The `numfields` parameter must match the number of arguments"))
    check("HTTL numfields mismatch", c.cmd("HTTL", "hfc", "FIELDS", "3", "f1", "f2"),
          pred=lambda g: is_err(g, "ERR The `numfields` parameter must match the number of arguments"))
    check("HPERSIST numfields mismatch", c.cmd("HPERSIST", "hfc", "FIELDS", "4", "f1", "f2", "f3"),
          pred=lambda g: is_err(g, "ERR The `numfields` parameter must match the number of arguments"))
    check("HEXPIRE without FIELDS", c.cmd("HEXPIRE", "hfc", "10", "NOTFIELDS", "1", "f1"),
          pred=lambda g: is_err(g, "ERR Mandatory argument FIELDS is missing or not at the right position"))
    check("HTTL without FIELDS", c.cmd("HTTL", "hfc", "COUNT", "1", "f1"),
          pred=lambda g: is_err(g, "ERR Mandatory argument FIELDS is missing or not at the right position"))
    check("HEXPIRE negative time", c.cmd("HEXPIRE", "hfc", "-1", "FIELDS", "1", "f1"),
          pred=lambda g: is_err(g, "ERR invalid expire time, must be >= 0"))
    check("HEXPIRE beyond the expire-time ceiling",
          c.cmd("HEXPIRE", "hfc", str((1 << 48) // 1000), "FIELDS", "1", "f1"),
          pred=lambda g: is_err(g, "ERR invalid expire time in 'hexpire' command"))
    check("HPEXPIRE beyond the expire-time ceiling",
          c.cmd("HPEXPIRE", "hfc", str(1 << 48), "FIELDS", "1", "f1"),
          pred=lambda g: is_err(g, "ERR invalid expire time in 'hpexpire' command"))
    check("HEXPIREAT beyond the expire-time ceiling",
          c.cmd("HEXPIREAT", "hfc", str((1 << 48) // 1000 + int(time.time()) + 100), "FIELDS", "1", "f1"),
          pred=lambda g: is_err(g, "ERR invalid expire time in 'hexpireat' command"))
    check("HPEXPIREAT beyond the expire-time ceiling",
          c.cmd("HPEXPIREAT", "hfc", str((1 << 48) + int(time.time() * 1000) + 100), "FIELDS", "1", "f1"),
          pred=lambda g: is_err(g, "ERR invalid expire time in 'hpexpireat' command"))
    check("HPEXPIRE just below the ceiling",
          c.cmd("HPEXPIRE", "hfc", str((1 << 46) - int(time.time() * 1000) - 1000), "FIELDS", "1", "f1"),
          [1])
    check("HPEXPIRE just above the ceiling",
          c.cmd("HPEXPIRE", "hfc", str((1 << 46) - int(time.time() * 1000) + 100000), "FIELDS", "1", "f1"),
          pred=lambda g: is_err(g, "ERR invalid expire time in 'hpexpire' command"))
    check("HEXPIRE NX with XX", c.cmd("HEXPIRE", "hfc", "10", "NX", "XX", "FIELDS", "1", "f1"),
          pred=lambda g: is_err(g, "ERR NX and XX, GT or LT options at the same time are not compatible"))
    check("HEXPIRE GT with LT", c.cmd("HEXPIRE", "hfc", "10", "GT", "LT", "FIELDS", "1", "f1"),
          pred=lambda g: is_err(g, "ERR GT and LT options at the same time are not compatible"))
    check("HEXPIRE arity", c.cmd("HEXPIRE", "hfc", "10", "FIELDS", "1"),
          pred=lambda g: is_err(g, "ERR wrong number of arguments for 'hexpire' command"))
    check("HPERSIST arity", c.cmd("HPERSIST", "hfc"),
          pred=lambda g: is_err(g, "ERR wrong number of arguments for 'hpersist' command"))

    # Repeated fields are processed in order
    c.cmd("DEL", "hrep")
    c.cmd("HSET", "hrep", "f", "v")
    check("HEXPIRE repeats a field in order",
          c.cmd("HEXPIRE", "hrep", "100", "NX", "FIELDS", "2", "f", "f"), [1, 0])
    check("HTTL repeats a field", c.cmd("HTTL", "hrep", "FIELDS", "2", "f", "f"),
          pred=lambda g: len(g) == 2 and g[0] == g[1] and 90 <= g[0] <= 100)

    # RENAME and COPY move the whole object, so field TTLs travel with it
    c.cmd("DEL", "hcopy", "hdst")
    c.cmd("HSET", "hcopy", "f", "v", "g", "v")
    c.cmd("HEXPIRE", "hcopy", "100", "FIELDS", "1", "f")
    c.cmd("COPY", "hcopy", "hdst")
    check("COPY carries the field TTL to the destination",
          c.cmd("HTTL", "hdst", "FIELDS", "2", "f", "g"),
          pred=lambda g: 90 <= g[0] <= 100 and g[1] == -1)
    check("COPY keeps the source field TTL", c.cmd("HTTL", "hcopy", "FIELDS", "1", "f"),
          pred=lambda g: 90 <= g[0] <= 100)
    c.cmd("DEL", "hren", "hren2")
    c.cmd("HSET", "hren", "f", "v", "g", "v")
    c.cmd("HEXPIRE", "hren", "100", "FIELDS", "1", "f")
    c.cmd("RENAME", "hren", "hren2")
    check("RENAME carries the field TTL", c.cmd("HTTL", "hren2", "FIELDS", "2", "f", "g"),
          pred=lambda g: 90 <= g[0] <= 100 and g[1] == -1)
    check("RENAME leaves nothing behind", c.cmd("EXISTS", "hren"), 0)
    c.cmd("DEL", "hdump")
    c.cmd("HSET", "hdump", "f", "v")
    c.cmd("HEXPIRE", "hdump", "100", "FIELDS", "1", "f")
    payload = c.cmd("DUMP", "hdump")
    c.cmd("DEL", "hdump")
    c.cmd("RESTORE", "hdump", "0", payload)
    check("RESTORE brings back a hash without field TTLs",
          c.cmd("HTTL", "hdump", "FIELDS", "1", "f"), [-1])
    c.cmd("DEL", "hsortsrc", "hsortdst")
    c.cmd("RPUSH", "hsortsrc", "2", "1")
    c.cmd("HSET", "hsortdst", "f", "v")
    c.cmd("HEXPIRE", "hsortdst", "100", "FIELDS", "1", "f")
    check("SORT STORE replaces the hash", c.cmd("SORT", "hsortsrc", "STORE", "hsortdst"), 2)
    check("SORT STORE dropped the field TTLs", c.cmd("TYPE", "hsortdst"), "list")
    c.cmd("DEL", "hsortdst")
    c.cmd("HSET", "hsortdst", "f", "v")
    check("the recreated hash has no leftover field TTL",
          c.cmd("HTTL", "hsortdst", "FIELDS", "1", "f"), [-1])
    c.cmd("DEL", "hflush")
    c.cmd("HSET", "hflush", "f", "v")
    c.cmd("HEXPIRE", "hflush", "100", "FIELDS", "1", "f")
    c.cmd("FLUSHALL")
    c.cmd("HSET", "hflush", "f", "v")
    check("FLUSHALL dropped the field TTLs", c.cmd("HTTL", "hflush", "FIELDS", "1", "f"), [-1])
    c.cmd("SET", "t1", "v")
    c.cmd("SADD", "s1", "a")

    # Untouched basics still work
    check("SET/GET regression", c.cmd("GET", "t1"), b"v")
    check("EXISTS regression", c.cmd("EXISTS", "t1", "s1"), 2)

    print("phase2 checks: %d passed, %d failed" % (PASSED, len(FAILED)))
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
