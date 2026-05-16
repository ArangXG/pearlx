#!/usr/bin/env python3
"""
fake_pool.py — Fake pool server to capture ALL messages from akoya-miner.bin
Sends proper RegisterResponse + JobAssignment, then logs every message.
This reveals exact Heartbeat format + PlainProofShare format!

Usage:
    python3 fake_pool.py > /tmp/fake.log 2>&1 &
    sleep 1
    LD_LIBRARY_PATH=/opt/akoya-miner/lib /opt/akoya-miner/akoya-miner.bin --config /tmp/capture.json
"""
import asyncio, struct, time, uuid, sys
try:
    import msgpack
except ImportError:
    sys.exit("pip install msgpack")

HOST = "0.0.0.0"
PORT = 3333

MSG_NAMES = {
    0:"RegisterRequest", 1:"RegisterResponse", 2:"JobAssignment",
    3:"PlainProofShare",  4:"ShareResult",      5:"Heartbeat",
    6:"HeartbeatAck",    7:"DifficultyAdjust", 8:"PoolError",
    9:"BlockSubmissionResult", 10:"MerkleProofData",
}

def hexdump(data: bytes, label=""):
    h = data.hex()
    pairs = [h[i:i+2] for i in range(0, len(h), 2)]
    asc = "".join(chr(b) if 32<=b<127 else "." for b in data)
    out = [f"\n  {label} ({len(data)} bytes)"]
    for i in range(0, len(pairs), 16):
        row = " ".join(pairs[i:i+16])
        a = asc[i:i+16]
        out.append(f"  {row:<48}  {a}")
    return "\n".join(out)

def build_frame(type_id: int, payload) -> bytes:
    """Build a valid pool frame: 4-byte length + [int32(type_id), payload]"""
    type_id_bytes = b'\xd2' + struct.pack('>i', type_id)
    payload_bytes = msgpack.packb(payload, use_bin_type=True)
    body = b'\x92' + type_id_bytes + payload_bytes
    return struct.pack('>I', len(body)) + body

async def recv_frame(reader):
    hdr = await reader.readexactly(4)
    length = struct.unpack('>I', hdr)[0]
    if length > 10*1024*1024:
        raise ValueError(f"Frame too large: {length}")
    body = await reader.readexactly(length)
    data = msgpack.unpackb(body, raw=False)
    if isinstance(data, (list, tuple)) and len(data) == 2:
        return int(data[0]), data[1]
    return -1, data

async def handle(reader, writer):
    peer = writer.get_extra_info('peername')
    print(f"\n{'='*64}", flush=True)
    print(f"  ✅ akoya-miner connected: {peer}", flush=True)

    session_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())

    try:
        # Step 1: Receive RegisterRequest
        print("\n⏳ Waiting for RegisterRequest...", flush=True)
        type_id, payload = await asyncio.wait_for(recv_frame(reader), timeout=10)
        name = MSG_NAMES.get(type_id, f"?({type_id})")
        print(f"\n📨 Received [{name}] type_id={type_id}", flush=True)
        print(f"   payload={payload}", flush=True)

        if type_id != 0:
            print(f"   ⚠️  Expected RegisterRequest(0), got {type_id}")

        # Step 2: Send RegisterResponse
        # Format confirmed: [True, session_uuid, block_height, job_uuid_str]
        reg_resp = [True, session_id, 453115904, job_id]
        frame = build_frame(1, reg_resp)
        print(f"\n📤 Sending RegisterResponse: {reg_resp}", flush=True)
        writer.write(frame)
        await writer.drain()
        await asyncio.sleep(0.1)

        # Step 3: Send JobAssignment
        # Format from captured pool data:
        # [job_uuid, matrix_bytes(76), height, diff, merkle(32), empty, nonce_start]
        matrix_bytes = bytes(76)   # 76 zeros as placeholder
        merkle_bytes  = bytes(32)  # 32 zeros as placeholder
        job_payload = [
            job_id,
            matrix_bytes,
            453115904,  # block height
            53520,       # difficulty
            merkle_bytes,
            b'',         # empty
            403151537,   # nonce start
        ]
        frame = build_frame(2, job_payload)
        print(f"\n📤 Sending JobAssignment: job_id={job_id}", flush=True)
        writer.write(frame)
        await writer.drain()

        # Step 4: Listen and log ALL incoming messages (Heartbeat, PlainProofShare, etc.)
        print(f"\n🎯 Now listening for messages from akoya-miner...", flush=True)
        print(f"   (Heartbeat expected within ~30s, PlainProofShare after mining)", flush=True)
        print(flush=True)

        msg_count = 0
        while True:
            try:
                type_id, payload = await asyncio.wait_for(recv_frame(reader), timeout=120)
                msg_count += 1
                name = MSG_NAMES.get(type_id, f"UNKNOWN({type_id})")
                ts = time.strftime("%H:%M:%S")

                print(f"\n[{ts}] #{msg_count} ← [{name}] type_id={type_id}", flush=True)
                print(f"  payload type: {type(payload).__name__}", flush=True)

                if isinstance(payload, list):
                    print(f"  payload length: {len(payload)} elements", flush=True)
                    for i, v in enumerate(payload):
                        if isinstance(v, bytes):
                            print(f"  [{i}] bytes({len(v)}): {v.hex()}")
                        else:
                            print(f"  [{i}] {type(v).__name__}: {v!r}")
                else:
                    print(f"  payload: {payload!r}", flush=True)

                print(flush=True)

                # If Heartbeat, send HeartbeatAck
                if type_id == 5:  # T_HEARTBEAT
                    ack = build_frame(6, [])  # HeartbeatAck
                    writer.write(ack)
                    await writer.drain()
                    print(f"  📤 Sent HeartbeatAck", flush=True)

                # If PlainProofShare, send ShareResult (accepted)
                elif type_id == 3:  # T_PLAIN_PROOF_SHARE
                    share_result = [0, "Accepted"]  # outcome=0=Accepted
                    result_frame = build_frame(4, share_result)
                    writer.write(result_frame)
                    await writer.drain()
                    print(f"  📤 Sent ShareResult(Accepted)", flush=True)

            except asyncio.TimeoutError:
                print(f"\n  ⏰ No message for 120s, closing.", flush=True)
                break

    except asyncio.IncompleteReadError:
        print(f"\n  🔌 akoya-miner disconnected", flush=True)
    except Exception as e:
        print(f"\n  ❌ Error: {e}", flush=True)
        import traceback; traceback.print_exc()
    finally:
        writer.close()
        print(f"\n{'='*64}\n  Session ended. Check log above for Heartbeat format!", flush=True)

async def main():
    server = await asyncio.start_server(handle, HOST, PORT)
    print("="*64)
    print("  🎭 Fake Pool Server")
    print(f"  Listening on {HOST}:{PORT}")
    print()
    print("  Run akoya-miner:")
    print("  LD_LIBRARY_PATH=/opt/akoya-miner/lib /opt/akoya-miner/akoya-miner.bin --config /tmp/capture.json")
    print("="*64, flush=True)
    async with server:
        await server.serve_forever()

asyncio.run(main())
