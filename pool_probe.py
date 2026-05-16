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

    # ── Format variants to test ──────────────────────────────────────────────
    # Pool confirmed: uses [type_id, payload] outer structure
    # PoolError payload = [code, msg, bool] → ARRAY format
    # So RegisterRequest payload is likely also an ARRAY

    await probe("A: [0, [wallet, worker, ver]]",
        msgpack.packb([0, [wallet, worker, ver]], use_bin_type=True))

    await probe("B: [0, [wallet, worker]]",
        msgpack.packb([0, [wallet, worker]], use_bin_type=True))

    await probe("C: [0, {0:wallet, 1:worker, 2:ver}] int-map",
        msgpack.packb([0, {0: wallet, 1: worker, 2: ver}], use_bin_type=True))

    await probe("D: [0, {0:wallet, 1:worker}] int-map no ver",
        msgpack.packb([0, {0: wallet, 1: worker}], use_bin_type=True))

    await probe("E: [0, {str-keys}] dict",
        msgpack.packb([0, {"wallet": wallet, "worker": worker, "version": ver}], use_bin_type=True))

    await probe("F: [0, [ver, wallet, worker]] ver-first",
        msgpack.packb([0, [ver, wallet, worker]], use_bin_type=True))

    await probe("G: [0, {'address':wallet,'worker':worker,'software':ver}]",
        msgpack.packb([0, {"address": wallet, "worker": worker, "software": ver}], use_bin_type=True))

    print("\n" + "="*60)
    print("  Probe complete. Format that gets RegisterResponse = correct one!")
    print("  Format that gets PoolError code=1 (InvalidRegistration) = wrong payload.")
    print("  Format that gets PoolError code=2 = wrong type_id.")

asyncio.run(main())
