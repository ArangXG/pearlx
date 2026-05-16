#!/usr/bin/env python3
"""
mitm_proxy.py — Man-in-the-Middle proxy to capture REAL pool messages.
Sits between akoya-miner and pool.akoyapool.com:3333, logging everything.
This reveals: exact Heartbeat, HeartbeatAck, ShareResult formats.

Usage:
    python3 mitm_proxy.py > /tmp/mitm.log 2>&1 &
    # Then run akoya-miner pointing to 127.0.0.1:3333
"""
import asyncio, struct, sys, time
try:
    import msgpack
except ImportError:
    sys.exit("pip install msgpack")

LISTEN_HOST  = "0.0.0.0"
LISTEN_PORT  = 3333
POOL_HOST    = "pool.akoyapool.com"
POOL_PORT    = 3333

MSG_NAMES = {
    0:"RegisterRequest", 1:"RegisterResponse", 2:"JobAssignment",
    3:"PlainProofShare",  4:"ShareResult",      5:"Heartbeat",
    6:"HeartbeatAck",    7:"DifficultyAdjust", 8:"PoolError",
    9:"BlockSubmissionResult", 10:"MerkleProofData",
}

def ts():
    return time.strftime("%H:%M:%S")

def decode_frame_safe(data: bytes):
    """Decode msgpack frame, return (type_id, payload) or (None, raw_hex)."""
    try:
        msg = msgpack.unpackb(data, raw=False)
        if isinstance(msg, (list, tuple)) and len(msg) == 2:
            type_id = int(msg[0])
            payload = msg[1]
            return type_id, payload
    except Exception:
        pass
    return None, data.hex()

def summarize(payload, max_bytes=32):
    """Summarize payload for logging."""
    if isinstance(payload, list):
        parts = []
        for item in payload:
            if isinstance(item, bytes):
                parts.append(f"bytes({len(item)})[{item[:max_bytes].hex()}...]")
            else:
                parts.append(repr(item))
        return f"[{', '.join(parts)}]"
    elif isinstance(payload, bytes):
        return f"bytes({len(payload)})[{payload[:max_bytes].hex()}...]"
    return repr(payload)

async def read_frame(reader) -> bytes | None:
    """Read one length-prefixed frame. Returns raw body bytes."""
    try:
        hdr = await reader.readexactly(4)
        length = struct.unpack('>I', hdr)[0]
        if length > 50 * 1024 * 1024:
            return None
        body = await reader.readexactly(length)
        return body
    except asyncio.IncompleteReadError:
        return None

def log_message(direction: str, body: bytes):
    type_id, payload = decode_frame_safe(body)
    if type_id is not None:
        name = MSG_NAMES.get(type_id, f"UNKNOWN({type_id})")
        summary = summarize(payload)
        print(f"[{ts()}] {direction} [{name}] type_id={type_id}", flush=True)
        # For Heartbeat and HeartbeatAck, show full payload
        if type_id in (5, 6):
            print(f"  *** HEARTBEAT/ACK payload={repr(payload)}", flush=True)
            print(f"  *** raw hex: {body.hex()}", flush=True)
        # For ShareResult, show full payload
        elif type_id == 4:
            print(f"  *** SHARE RESULT payload={repr(payload)}", flush=True)
            print(f"  *** raw hex: {body.hex()}", flush=True)
        else:
            print(f"  payload summary: {summary[:200]}", flush=True)
    else:
        print(f"[{ts()}] {direction} RAW ({len(body)} bytes): {body[:64].hex()}...", flush=True)

async def proxy_direction(reader, writer, direction: str, done_event: asyncio.Event):
    """Relay bytes from reader→writer, logging each frame."""
    try:
        while not done_event.is_set():
            body = await asyncio.wait_for(read_frame(reader), timeout=120)
            if body is None:
                break
            log_message(direction, body)
            frame = struct.pack('>I', len(body)) + body
            writer.write(frame)
            await writer.drain()
    except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError):
        pass
    finally:
        done_event.set()

async def handle(miner_reader, miner_writer):
    peer = miner_writer.get_extra_info('peername')
    print(f"\n[{ts()}] ✅ Miner connected: {peer}", flush=True)
    print(f"[{ts()}] 🌐 Connecting to {POOL_HOST}:{POOL_PORT}...", flush=True)

    try:
        pool_reader, pool_writer = await asyncio.open_connection(POOL_HOST, POOL_PORT)
    except Exception as e:
        print(f"[{ts()}] ❌ Failed to connect to pool: {e}", flush=True)
        miner_writer.close()
        return

    print(f"[{ts()}] ✅ Connected to pool!", flush=True)
    done = asyncio.Event()

    # Bidirectional relay
    await asyncio.gather(
        proxy_direction(miner_reader, pool_writer,  "MINER→POOL", done),
        proxy_direction(pool_reader,  miner_writer, "POOL→MINER", done),
        return_exceptions=True,
    )

    print(f"[{ts()}] 🔌 Session ended", flush=True)
    pool_writer.close()
    miner_writer.close()

async def main():
    server = await asyncio.start_server(handle, LISTEN_HOST, LISTEN_PORT)
    print("="*64)
    print("  🕵️  MitM Proxy — Pearl Mining")
    print(f"  Listen: {LISTEN_HOST}:{LISTEN_PORT}")
    print(f"  Forward → {POOL_HOST}:{POOL_PORT}")
    print()
    print("  Run akoya-miner with --config /tmp/capture.json")
    print("  (capture.json must point pool url to 127.0.0.1:3333)")
    print("="*64, flush=True)
    async with server:
        await server.serve_forever()

asyncio.run(main())
