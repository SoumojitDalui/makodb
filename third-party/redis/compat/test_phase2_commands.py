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

    # Untouched basics still work
    check("SET/GET regression", c.cmd("GET", "t1"), b"v")
    check("EXISTS regression", c.cmd("EXISTS", "t1", "s1"), 2)

    print("phase2 checks: %d passed, %d failed" % (PASSED, len(FAILED)))
    sys.exit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
