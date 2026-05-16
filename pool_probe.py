#!/usr/bin/env python3
"""
pool_probe.py — Detect akoyapool's exact MessagePack wire format.
Connects to pool, sends candidate frames, logs raw hex responses.

Usage:
    python3 pool_probe.py
"""
import asyncio, struct, sys, json
try:
    import msgpack
except ImportError:
    sys.exit("pip install msgpack")

HOST = "pool.akoyapool.com"
PORT = 3333
WALLET = "prl1pvxf2ljgw6xw32fzwjftt660m7jny6hl2lp7n5c3dq6w5a8maekpqwjpge8"
WORKER = "probe1"

def hex_dump(data: bytes, label="") -> str:
    h = data.hex()
    pairs = [h[i:i+2] for i in range(0, len(h), 2)]
    rows = [" ".join(pairs[i:i+16]) for i in range(0, len(pairs), 16)]
    printable = "".join(chr(b) if 32 <= b < 127 else "." for b in data)
    out = [f"\n{'─'*60}", f"  {label} ({len(data)} bytes)"]
    for i, row in enumerate(rows):
        asc = printable[i*16:(i+1)*16]
        out.append(f"  {row:<48}  {asc}")
    return "\n".join(out)

def make_frame(body: bytes) -> bytes:
    return struct.pack(">I", len(body)) + body

def make_body_int32(type_id: int, payload_bytes: bytes) -> bytes:
    """Build frame BODY only (no length prefix) with int32-encoded type_id."""
    # Correct format: fixarray[2] + int32(type_id) + payload
    # probe() will add the 4-byte length prefix via make_frame()
    return b'\x92' + b'\xd2' + struct.pack('>i', type_id) + payload_bytes

async def probe(name: str, body: bytes, timeout=4):
    print(f"\n{'='*60}")
    print(f"  PROBE: {name}")
    print(hex_dump(make_frame(body), "→ SENDING"))
    try:
        r, w = await asyncio.open_connection(HOST, PORT)
        w.write(make_frame(body))
        await w.drain()
        raw = b""
        try:
            raw = await asyncio.wait_for(r.read(4096), timeout=timeout)
        except asyncio.TimeoutError:
            print("  ← (no response within timeout)")
        if raw:
            print(hex_dump(raw, "← RECEIVED"))
            # Try decode as msgpack
            try:
                dec = msgpack.unpackb(raw[4:] if len(raw) > 4 else raw, raw=False)
                print(f"  ← msgpack decoded (skip 4b header): {dec}")
            except Exception as e:
                print(f"  ← msgpack decode failed: {e}")
            # Try decode as utf-8
            try:
                print(f"  ← as text: {raw.decode('utf-8', errors='replace')[:200]}")
            except: pass
        w.close()
    except Exception as e:
        print(f"  ERROR: {e}")

async def main():
    wallet = WALLET
    worker = WORKER
    ver    = "akoya-miner/1.0.0"

    print("\n🔑 KEY INSIGHT: Pool sends type_id as int32 (d2 XXXXXXXX), not fixint!")
    print("   Testing with int32-encoded type_id...\n")

    # ── All tests use int32 type_id encoding ─────────────────────────────────
    def p(payload):
        # make_body_int32 returns body ONLY, probe() adds length prefix
        return make_body_int32(0, msgpack.packb(payload, use_bin_type=True))

    await probe("INT32-A: type_id=int32, payload=[wallet,worker,ver]",
        p([wallet, worker, ver]))

    await probe("INT32-B: type_id=int32, payload=[wallet,worker]",
        p([wallet, worker]))

    await probe("INT32-C: type_id=int32, payload={0:w,1:wk,2:v} int-map",
        p({0: wallet, 1: worker, 2: ver}))

    await probe("INT32-D: type_id=int32, payload={str-keys dict}",
        p({"wallet": wallet, "worker": worker, "version": ver}))

    await probe("INT32-E: type_id=int32, payload=[ver,wallet,worker] ver-first",
        p([ver, wallet, worker]))

    await probe("INT32-F: type_id=int32, payload={'address','worker','software'}",
        p({"address": wallet, "worker": worker, "software": ver}))

    # ── Extra: try different type_id values in case 0 is wrong ───────────────
    print("\n🔍 Testing type_id=1 (in case RegisterRequest=1, not 0)...")
    await probe("INT32-G: type_id=1, payload=[wallet,worker,ver]",
        make_body_int32(1, msgpack.packb([wallet, worker, ver], use_bin_type=True)))

    print("\n" + "="*60)
    print("  ✅ Format that gets RegisterResponse (type=1) = CORRECT!")
    print("  ❌ PoolError code=1 = wrong payload structure")
    print("  ❌ PoolError code=2 = wrong type_id")
    print("  ❌ PoolError code=8 = InternalError/other")


asyncio.run(main())
