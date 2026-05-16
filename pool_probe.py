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
    reg_str_keys = {"wallet": WALLET, "worker": WORKER, "version": "1.0.0"}
    reg_int_keys = {0: WALLET, 1: WORKER, 2: "1.0.0"}

    # Hypothesis A: [type_id, {str_keys}]
    await probe("Union [0, str_keys]",
        msgpack.packb([0, reg_str_keys], use_bin_type=True))

    # Hypothesis B: [type_id, {int_keys}]
    await probe("Union [0, int_keys]",
        msgpack.packb([0, reg_int_keys], use_bin_type=True))

    # Hypothesis C: 1-byte type + msgpack map
    body_c = bytes([0]) + msgpack.packb(reg_str_keys, use_bin_type=True)
    await probe("1-byte-type + str_map", body_c)

    # Hypothesis D: pure msgpack map (no type prefix)
    await probe("Pure str_map",
        msgpack.packb(reg_str_keys, use_bin_type=True))

    # Hypothesis E: msgpack map with "type" field
    await probe("Map with 'type' field",
        msgpack.packb({"type": 0, **reg_str_keys}, use_bin_type=True))

    # Hypothesis F: msgpack map with 'type' = 'RegisterRequest'
    await probe("Map with 'type'=RegisterRequest",
        msgpack.packb({"type": "RegisterRequest", **reg_str_keys}, use_bin_type=True))

    print("\n" + "="*60)
    print("  Probe complete. Check output above to determine correct format.")

asyncio.run(main())
