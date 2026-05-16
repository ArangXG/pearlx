#!/usr/bin/env python3
"""
stratum_bridge.py — Stratum ↔ MessagePack Bridge
────────────────────────────────────────────────────
Translates alpha-miner (Stratum JSON) → pool.akoyapool.com (MessagePack binary)

Usage:
    python3 stratum_bridge.py [--port 13333]

Then run alpha-miner:
    ./alpha-miner --pool stratum+tcp://127.0.0.1:13333 \\
                  --address YOUR_WALLET --worker YOUR_WORKER

Requirements:
    pip install msgpack
"""

import asyncio
import json
import logging
import struct
import sys
import time
import argparse
import uuid
from typing import Any, Optional, Tuple

try:
    import msgpack
except ImportError:
    sys.exit("❌ Run: pip install msgpack")

# ── Logging ───────────────────────────────────────────────────────────────────
class ColorLog(logging.Formatter):
    C = {'DEBUG':'\033[36m','INFO':'\033[32m','WARNING':'\033[33m','ERROR':'\033[31m'}
    R = '\033[0m'
    def format(self, r):
        r.levelname = f"{self.C.get(r.levelname,'')}{r.levelname:<7}{self.R}"
        return super().format(r)

h = logging.StreamHandler(sys.stdout)
h.setFormatter(ColorLog('%(asctime)s %(levelname)s %(message)s', '%H:%M:%S'))
log = logging.getLogger("bridge")
log.addHandler(h)
log.setLevel(logging.DEBUG)
log.propagate = False

# ── Config ────────────────────────────────────────────────────────────────────
POOL_HOST     = "pool.akoyapool.com"
POOL_PORT     = 3333
LOCAL_PORT    = 13333
MINER_VERSION = "akoya-miner/1.0.0"
MAX_FRAME     = 8192

# ── Message Type IDs (enum order from binary analysis) ────────────────────────
T_REGISTER_REQUEST        = 0
T_REGISTER_RESPONSE       = 1
T_JOB_ASSIGNMENT          = 2
T_PLAIN_PROOF_SHARE       = 3
T_SHARE_RESULT            = 4
T_HEARTBEAT               = 5
T_HEARTBEAT_ACK           = 6
T_DIFFICULTY_ADJUST       = 7
T_POOL_ERROR              = 8
T_BLOCK_SUBMISSION_RESULT = 9
T_MERKLE_PROOF_DATA       = 10

MSG_NAMES = {
    0:"RegisterRequest", 1:"RegisterResponse", 2:"JobAssignment",
    3:"PlainProofShare",  4:"ShareResult",      5:"Heartbeat",
    6:"HeartbeatAck",    7:"DifficultyAdjust", 8:"PoolError",
    9:"BlockSubmissionResult", 10:"MerkleProofData",
}

# Share outcomes
OUTCOME_ACCEPTED  = 0   # Accepted / BlockSubmission
OUTCOME_REJECTED  = 1
OUTCOME_STALE     = 2
OUTCOME_DUPLICATE = 3
OUTCOME_INVALID   = 4

# ── Frame Encoder / Decoder ───────────────────────────────────────────────────
# Wire format: [4-byte big-endian uint32 length][msgpack body]
# Body format: [type_id, {fields}]  (MessagePack union array)

def encode_frame(type_id: int, payload: Any) -> bytes:
    # CONFIRMED: pool uses int32 (d2 XXXXXXXX) for type_id, NOT fixint
    # Pool sends: 92 d2 00000008 [payload] for PoolError
    # We must send: 92 d2 00000000 [payload] for RegisterRequest
    #
    # Build manually: fixarray[2] + int32(type_id) + msgpack(payload)
    type_id_bytes = b'\xd2' + struct.pack('>i', type_id)  # int32 big-endian
    payload_bytes = msgpack.packb(payload, use_bin_type=True)
    body = b'\x92' + type_id_bytes + payload_bytes        # 0x92 = fixarray[2]
    return struct.pack('>I', len(body)) + body

async def recv_frame(reader: asyncio.StreamReader) -> Tuple[int, Any]:
    hdr = await reader.readexactly(4)
    length = struct.unpack(">I", hdr)[0]
    if length > 1024 * 1024:
        raise ValueError(f"Frame too large: {length}")
    raw = await reader.readexactly(length)
    log.debug(f"  raw hex: {raw[:64].hex()}{'...' if len(raw)>64 else ''}")
    data = msgpack.unpackb(raw, raw=False)
    if isinstance(data, (list, tuple)) and len(data) == 2:
        return int(data[0]), data[1]
    # fallback: maybe it's just a map
    return -1, data

def hex_dump(data: bytes) -> str:
    return data.hex() if data else "(empty)"

# ── Pool Connection (MessagePack side) ────────────────────────────────────────
class PoolConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.current_job = None
        self.difficulty = 1.0
        self.registered = False

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        log.info(f"🌐 Connected to pool {self.host}:{self.port}")

    async def send(self, type_id: int, payload: Any):
        name = MSG_NAMES.get(type_id, str(type_id))
        frame = encode_frame(type_id, payload)
        log.debug(f"→ POOL [{name}] {json.dumps(payload) if isinstance(payload, dict) else payload}")
        self.writer.write(frame)
        await self.writer.drain()

    async def recv(self) -> Tuple[int, Any]:
        type_id, payload = await recv_frame(self.reader)
        name = MSG_NAMES.get(type_id, f"UNKNOWN({type_id})")
        log.debug(f"← POOL [{name}] {payload}")
        return type_id, payload

    async def register(self, wallet: str, worker: str):
        # ✅ CONFIRMED FORMAT from capture_server.py sniffing akoya-miner.bin:
        # [type_id=0, [uuid, wallet, worker, gpu_name, common_dim, version, git_sha]]
        client_id  = str(uuid.uuid4())
        gpu_name   = self._detect_gpu()
        common_dim = 2048        # K dimension akoya-miner uses
        version    = "1.0.0"
        git_sha    = "4fb978d0e2894a14d8652c79bd9aa0ab0ccf124c"  # akoya-miner git SHA

        payload = [client_id, wallet, worker, gpu_name, common_dim, version, git_sha]
        await self.send(T_REGISTER_REQUEST, payload)
        log.info(f"📤 RegisterRequest sent:")
        log.info(f"   uuid={client_id}")
        log.info(f"   wallet={wallet[:20]}...")
        log.info(f"   worker={worker}")
        log.info(f"   gpu={gpu_name}")
        log.info(f"   dim={common_dim} ver={version}")

    def _detect_gpu(self) -> str:
        """Try to get GPU name from nvidia-smi, fallback to generic."""
        try:
            import subprocess
            r = subprocess.run(
                ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=3
            )
            name = r.stdout.strip().split("\n")[0].strip()
            if name:
                return name
        except Exception:
            pass
        return "NVIDIA GeForce RTX 5070 Ti"  # fallback


    async def submit_share(self, job_id, proof_data: dict):
        await self.send(T_PLAIN_PROOF_SHARE, proof_data)
        log.info(f"⛏️  PlainProofShare submitted for job {job_id}")

    async def send_heartbeat(self):
        await self.send(T_HEARTBEAT, {})

    def close(self):
        if self.writer:
            self.writer.close()

# ── Stratum Server (JSON side for alpha-miner) ────────────────────────────────
class StratumSession:
    def __init__(self, reader, writer, pool: PoolConnection):
        self.reader = reader
        self.writer = writer
        self.pool = pool
        self.wallet = ""
        self.worker = ""
        self.session_id = "deadbeef"
        self.pending: dict = {}    # id → method
        self.job_id = ""
        self.submit_count = 0

    # ── Send helpers ──────────────────────────────────────────────────────────

    async def send(self, obj: dict):
        line = json.dumps(obj) + "\n"
        log.debug(f"→ MINER {line.strip()}")
        self.writer.write(line.encode())
        await self.writer.drain()

    async def send_result(self, id_, result, error=None):
        await self.send({"id": id_, "result": result, "error": error})

    async def send_notify(self, job):
        """Send mining.notify to alpha-miner with job from pool.
        JobAssignment = [job_id, matrix_bytes, height, diff_bits, merkle_root, extra, nonce_start]
        """
        if isinstance(job, list):
            job_id      = str(job[0])                                               # UUID
            matrix_hex  = job[1].hex() if isinstance(job[1], bytes) else str(job[1])  # challenge data
            height      = job[2] if len(job) > 2 else 0                            # block height
            diff_bits   = job[3] if len(job) > 3 else 0                            # difficulty
            merkle_hex  = job[4].hex() if len(job) > 4 and isinstance(job[4], bytes) else ""
            nonce_start = job[6] if len(job) > 6 else 0                            # nonce start
        else:
            job_id      = str(job.get("job_id", job.get("id", "0")))
            matrix_hex  = job.get("matrix", job.get("seed", ""))
            height      = job.get("height", 0)
            diff_bits   = job.get("diff_bits", 0)
            merkle_hex  = job.get("merkle", "")
            nonce_start = job.get("nonce_start", 0)

        self.job_id        = job_id
        self.current_job   = job
        self.matrix_hex    = matrix_hex
        self.height        = height
        self.merkle_hex    = merkle_hex

        # Stratum mining.notify params for pearl/v1:
        # [job_id, height_hex, matrix_data_hex, merkle_root_hex, nonce_start_hex, diff_bits, clean_jobs]
        params = [
            job_id,
            f"{height:08x}",
            matrix_hex,
            merkle_hex,
            f"{nonce_start:08x}",
            diff_bits,
            True,   # clean jobs
        ]
        await self.send({
            "id": None,
            "method": "mining.notify",
            "params": params,
        })
        log.info(f"📢 mining.notify job_id={job_id} height={height} diff={diff_bits}")


    async def send_difficulty(self, diff: float):
        await self.send({
            "id": None,
            "method": "mining.set_difficulty",
            "params": [diff],
        })

    # ── Stratum message handlers ──────────────────────────────────────────────

    async def handle_configure(self, id_, params):
        """mining.configure — respond with pearl/v1 support."""
        extensions = params[0] if params else []
        result = {}
        if "pearl/v1" in extensions:
            # Advertise mining shape parameters (from alpha-miner logs)
            result["pearl/v1"] = {
                "m": 131072, "n": 131072, "k": 4096, "rank": 128
            }
        await self.send_result(id_, result)
        log.info("⚙️  mining.configure → pearl/v1 ACK")

    async def handle_subscribe(self, id_, params):
        """mining.subscribe — return session info."""
        result = [
            [["mining.notify", self.session_id]],
            self.session_id,
            4,
        ]
        await self.send_result(id_, result)
        log.info("📡 mining.subscribe → session established")

    async def handle_authorize(self, id_, params):
        """mining.authorize — RegisterRequest already sent at connect time."""
        await self.send_result(id_, True)
        log.info(f"🔑 mining.authorize → ACK (already registered with pool)")

    async def handle_submit(self, id_, params):
        """mining.submit — translate proof to PlainProofShare."""
        # Stratum params: [worker, job_id, proof_hex, ...]
        self.submit_count += 1
        worker  = params[0] if len(params) > 0 else self.worker
        job_id  = params[1] if len(params) > 1 else self.job_id
        # params[2..] = proof data (format depends on alpha-miner implementation)
        proof_raw = params[2] if len(params) > 2 else ""
        log.info(f"📤 mining.submit #{self.submit_count} job={job_id} proof_len={len(proof_raw)}")

        # Try decode proof_raw as hex → binary bincode
        try:
            proof_bytes = bytes.fromhex(proof_raw) if isinstance(proof_raw, str) else bytes(proof_raw)
        except Exception:
            proof_bytes = proof_raw.encode() if isinstance(proof_raw, str) else b""

        # Build PlainProofShare
        # The proof_bytes is the PlainProofBincode from pearl_capi_plain_proof_pack
        # We also extract individual fields if params contain them
        share_payload = {
            "job_id":            job_id,
            "worker":            worker,
            "plain_proof_bincode": proof_bytes,
        }

        # If alpha-miner sends extended params (individual fields)
        if len(params) > 3:
            extra = params[3] if isinstance(params[3], dict) else {}
            if "sigma"     in extra: share_payload["sigma"]      = bytes.fromhex(extra["sigma"])
            if "claimed_hash" in extra: share_payload["claimed_hash"] = bytes.fromhex(extra["claimed_hash"])
            if "hash_a"    in extra: share_payload["hash_a"]     = bytes.fromhex(extra["hash_a"])
            if "hash_b"    in extra: share_payload["hash_b"]     = bytes.fromhex(extra["hash_b"])

        await self.pool.submit_share(job_id, share_payload)

        # Optimistically ack (pool will confirm)
        await self.send_result(id_, True)

    # ── Main Stratum receive loop ─────────────────────────────────────────────

    async def run(self):
        while True:
            try:
                raw = await self.reader.readline()
                if not raw:
                    break
                line = raw.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    log.warning(f"Bad JSON from miner: {line!r}")
                    continue

                log.debug(f"← MINER {line}")
                id_     = msg.get("id")
                method  = msg.get("method", "")
                params  = msg.get("params", [])

                if method == "mining.configure":
                    await self.handle_configure(id_, params)
                elif method == "mining.subscribe":
                    await self.handle_subscribe(id_, params)
                elif method == "mining.authorize":
                    await self.handle_authorize(id_, params)
                elif method == "mining.submit":
                    await self.handle_submit(id_, params)
                else:
                    log.warning(f"Unknown Stratum method: {method}")

            except asyncio.IncompleteReadError:
                break
            except Exception as e:
                log.error(f"Stratum loop error: {e}")
                break
        log.info("🔌 Miner disconnected")

# ── Pool Receive Loop ─────────────────────────────────────────────────────────
async def pool_recv_loop(pool: PoolConnection, session: StratumSession):
    """Receive messages from pool and translate back to Stratum for miner."""
    heartbeat_interval = 30
    last_heartbeat = time.time()

    while True:
        try:
            # Check heartbeat
            if time.time() - last_heartbeat > heartbeat_interval:
                await pool.send_heartbeat()
                last_heartbeat = time.time()

            # Non-blocking recv with timeout
            try:
                type_id, payload = await asyncio.wait_for(pool.recv(), timeout=5)
            except asyncio.TimeoutError:
                continue

            name = MSG_NAMES.get(type_id, f"?({type_id})")

            if type_id == T_REGISTER_RESPONSE:
                pool.registered = True
                log.info(f"✅ RegisterResponse: {payload}")
                # payload = [success, session_id, block_height, job_uuid]
                if isinstance(payload, list):
                    success    = payload[0] if len(payload) > 0 else False
                    session_id = payload[1] if len(payload) > 1 else ""
                    # diff/height from pool, use default stratum diff 1
                    if not success:
                        log.error("❌ Pool rejected registration!")
                        break
                else:
                    success = payload.get("success", True) if isinstance(payload, dict) else True
                await session.send_difficulty(1.0)
                log.info(f"✅ Registered! session={payload[1] if isinstance(payload,list) else ''}")

            elif type_id == T_JOB_ASSIGNMENT:
                log.info(f"📋 JobAssignment: {payload}")
                pool.current_job = payload
                await session.send_notify(payload)

            elif type_id == T_SHARE_RESULT:
                outcome = payload.get("outcome", payload.get("result", -1)) if isinstance(payload, dict) else payload
                if outcome == OUTCOME_ACCEPTED or outcome == 0:
                    log.info(f"✅ Share ACCEPTED! {payload}")
                else:
                    outcome_names = {1:"Rejected", 2:"Stale", 3:"Duplicate", 4:"Invalid", 5:"RateLimited"}
                    log.warning(f"❌ Share {outcome_names.get(outcome,'Unknown')}({outcome}): {payload}")

            elif type_id == T_DIFFICULTY_ADJUST:
                diff = payload.get("difficulty", pool.difficulty) if isinstance(payload, dict) else float(payload)
                pool.difficulty = diff
                log.info(f"⚡ DifficultyAdjust → {diff}")
                await session.send_difficulty(diff)

            elif type_id == T_HEARTBEAT_ACK:
                log.debug("💓 HeartbeatAck")

            elif type_id == T_POOL_ERROR:
                code = payload.get("code", "?") if isinstance(payload, dict) else payload
                msg_ = payload.get("message", "") if isinstance(payload, dict) else ""
                log.error(f"🚨 PoolError code={code}: {msg_}")
                # If UnsupportedProtocol, log hint
                if code in ("UnsupportedProtocol", 2):
                    log.error("  → Protocol format mismatch. Run pool_probe.py to detect correct format.")

            elif type_id == T_BLOCK_SUBMISSION_RESULT:
                log.info(f"🎉 BlockSubmission: {payload}")

            elif type_id == T_MERKLE_PROOF_DATA:
                log.debug(f"MerkleProofData: {payload}")

            else:
                log.warning(f"Unhandled pool message [{name}]: {payload}")

        except asyncio.IncompleteReadError:
            log.error("Pool disconnected!")
            break
        except Exception as e:
            log.error(f"Pool recv loop error: {e}")
            import traceback; traceback.print_exc()
            break

# ── Session Handler ───────────────────────────────────────────────────────────
async def handle_miner(reader, writer, pool_host, pool_port, wallet_hint, worker_hint):
    peer = writer.get_extra_info("peername")
    log.info(f"🔌 alpha-miner connected from {peer}")

    pool = PoolConnection(pool_host, pool_port)
    pool.wallet_hint = wallet_hint
    pool.worker_hint = worker_hint

    try:
        await pool.connect()
        # ✅ Send RegisterRequest IMMEDIATELY — pool has 10s timeout!
        await pool.register(wallet_hint, worker_hint)
    except Exception as e:
        log.error(f"Cannot connect to pool: {e}")
        writer.close()
        return

    session = StratumSession(reader, writer, pool)
    session.wallet = wallet_hint
    session.worker = worker_hint

    await asyncio.gather(
        session.run(),
        pool_recv_loop(pool, session),
        return_exceptions=True
    )

    pool.close()
    log.info("Session ended")

# ── Main ─────────────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="Stratum↔MessagePack bridge for alpha-miner → akoyapool")
    parser.add_argument("--port",     type=int, default=LOCAL_PORT,  help="Local Stratum listen port")
    parser.add_argument("--upstream", type=str, default=POOL_HOST,   help="Pool host")
    parser.add_argument("--up-port",  type=int, default=POOL_PORT,   help="Pool port")
    parser.add_argument("--wallet",   type=str, default="",          help="Your Pearl wallet address")
    parser.add_argument("--worker",   type=str, default="worker1",   help="Worker name")
    parser.add_argument("--debug",    action="store_true",           help="Show debug logs")
    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    server = await asyncio.start_server(
        lambda r, w: handle_miner(r, w, args.upstream, args.up_port, args.wallet, args.worker),
        "0.0.0.0", args.port
    )

    print()
    print("  ╔══════════════════════════════════════════════════════╗")
    print("  ║    Stratum ↔ MessagePack Bridge — Pearl Mining       ║")
    print("  ╠══════════════════════════════════════════════════════╣")
    print(f"  ║  Listen  : 0.0.0.0:{args.port:<6}  (alpha-miner connects here) ║")
    print(f"  ║  Pool    : {args.upstream}:{args.up_port}           ║")
    print(f"  ║  Wallet  : {(args.wallet[:30] + '...' if len(args.wallet)>30 else args.wallet or '(from miner auth)'):<42} ║")
    print("  ╠══════════════════════════════════════════════════════╣")
    print("  ║  Run alpha-miner:                                    ║")
    print(f"  ║    ./alpha-miner --pool stratum+tcp://127.0.0.1:{args.port}   ║")
    print(f"  ║      --address YOUR_WALLET --worker {args.worker:<18} ║")
    print("  ╠══════════════════════════════════════════════════════╣")
    print("  ║  TIP: Run pool_probe.py first to verify frame format ║")
    print("  ╚══════════════════════════════════════════════════════╝")
    print()

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bridge stopped.")
