#!/usr/bin/env python3
"""
capture_server.py — TCP capture server
Listens on port 3333, logs ALL raw bytes received.
Point akoya-miner.bin at 127.0.0.1:3333 to capture its exact wire format.

Usage:
    python3 capture_server.py &
    # Create config: {"pool":{"url":"127.0.0.1:3333","wallet":"...","worker":"..."}}
    ./akoya-miner.bin --config /tmp/capture.json
"""
import asyncio, sys, struct
try:
    import msgpack
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False

def hexdump(data: bytes, label="") -> str:
    h = data.hex()
    pairs = [h[i:i+2] for i in range(0, len(h), 2)]
    asc = "".join(chr(b) if 32<=b<127 else "." for b in data)
    out = [f"\n{'─'*64}", f"  {label} ({len(data)} bytes)"]
    for i in range(0, len(pairs), 16):
        row = " ".join(pairs[i:i+16])
        a   = asc[i:i+16]
        out.append(f"  {row:<48}  {a}")
    return "\n".join(out)

async def handle(reader, writer):
    peer = writer.get_extra_info("peername")
    print(f"\n{'='*64}")
    print(f"  ✅ akoya-miner connected from {peer}")
    print(f"{'='*64}\n", flush=True)

    buf = b""
    try:
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                break
            buf += chunk
            print(hexdump(chunk, f"→ RAW from miner ({len(chunk)} bytes)"), flush=True)

            # Try decode as length-prefixed msgpack
            while len(buf) >= 4:
                length = struct.unpack(">I", buf[:4])[0]
                print(f"  [PARSE] 4-byte length prefix = {length} (0x{length:08x})")
                if length > 1024*1024:
                    print(f"  [PARSE] ⚠️  Length {length} too large — not 4-byte-prefixed format?")
                    buf = b""
                    break
                if len(buf) < 4 + length:
                    print(f"  [PARSE] Waiting for more data ({len(buf)}/{4+length})...")
                    break
                body = buf[4:4+length]
                buf  = buf[4+length:]
                print(hexdump(body, f"→ FRAME BODY ({length} bytes)"))
                if HAS_MSGPACK:
                    try:
                        decoded = msgpack.unpackb(body, raw=False)
                        print(f"  [MSGPACK] ✅ Decoded: {decoded}", flush=True)
                        # Show type_id if array[2]
                        if isinstance(decoded, (list,tuple)) and len(decoded)==2:
                            tid, payload = decoded
                            names = {0:"RegisterRequest",1:"RegisterResponse",2:"JobAssignment",
                                     3:"PlainProofShare",4:"ShareResult",5:"Heartbeat",
                                     6:"HeartbeatAck",7:"DifficultyAdjust",8:"PoolError"}
                            print(f"  [MSGPACK] type_id={tid} ({names.get(tid,'?')})")
                            print(f"  [MSGPACK] payload={payload}")
                    except Exception as e:
                        print(f"  [MSGPACK] ❌ Decode failed: {e}")
                        # Try skipping 4-byte prefix within body
                        for skip in [0,1,2,4,5]:
                            try:
                                dec2 = msgpack.unpackb(body[skip:], raw=False)
                                print(f"  [MSGPACK] ✅ Decoded with skip={skip}: {dec2}")
                                break
                            except: pass
                print(flush=True)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        print(f"\n  🔌 Disconnected: {peer}\n")
        writer.close()

async def main():
    server = await asyncio.start_server(handle, "0.0.0.0", 3333)
    print("="*64)
    print("  📡 Capture Server listening on 0.0.0.0:3333")
    print("  Point akoya-miner.bin here to capture wire format")
    print()
    print("  Step 1: Create config file:")
    print('  echo \'{"pool":{"url":"127.0.0.1:3333","wallet":"YOUR_WALLET","worker":"amax1"},"logging":{"level":"info"},"devices":"all"}\' > /tmp/capture.json')
    print()
    print("  Step 2: Run akoya-miner.bin:")
    print("  LD_LIBRARY_PATH=./lib ./akoya-miner.bin --config /tmp/capture.json")
    print("="*64)
    async with server:
        await server.serve_forever()

asyncio.run(main())
