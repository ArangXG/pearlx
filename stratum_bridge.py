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
DEFAULT_WALLET = "prl1p4jkvta2rrj2w87tv6e4x38rspn92ucvztv8tywr6y4p772gu5n4stuf3kk"

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
    if length > 4 * 1024 * 1024:
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
        # Heartbeat payload must be array (like all other messages), not dict
        await self.send(T_HEARTBEAT, [])


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
        self.configured = False    # set after mining.configure ACK
        self.subscribed  = False
        self.authorized  = False
        self.job_buffer  = []
        self.pool_retry  = False   # set True to trigger pool reconnect

    # ── Send helpers ──────────────────────────────────────────────────────────

    async def send(self, obj: dict):
        line = json.dumps(obj) + "\n"
        log.debug(f"→ MINER {line.strip()}")
        self.writer.write(line.encode())
        await self.writer.drain()

    async def send_result(self, id_, result, error=None):
        await self.send({"id": id_, "result": result, "error": error})

    async def send_notify(self, job):
        """Send mining.notify to alpha-miner.
        JobAssignment: [job_uuid, sigma(76B), target_nbits, height, merkle(32B), extra, net_nbits]
        """
        if isinstance(job, list):
            job_id      = str(job[0])
            sigma_hex   = job[1].hex() if isinstance(job[1], bytes) else str(job[1])
            target_bits = job[2] if len(job) > 2 else 0
            height      = job[3] if len(job) > 3 else 0
            merkle_hex  = job[4].hex() if len(job) > 4 and isinstance(job[4], bytes) else ""
            net_bits    = job[6] if len(job) > 6 else 0
        else:
            job_id = str(job.get("job_id", "0"))
            sigma_hex = job.get("sigma", "")
            target_bits = job.get("target_bits", 0)
            height = job.get("height", 0)
            merkle_hex = job.get("merkle", "")
            net_bits = 0

        self.job_id      = job_id
        self.current_job = job

        # pearl/v1 notify: [job_id, sigma_hex, merkle_hex, height_hex, nbits_hex, net_hex, clean]
        # height as hex string ("0000d117") not integer
        params = [
            job_id,
            sigma_hex,
            merkle_hex,
            f"{height:08x}",
            f"{target_bits:08x}",
            f"{net_bits:08x}",
            True,
        ]
        await self.send({
            "id": None,
            "method": "mining.notify",
            "params": params,
        })
        log.info(f"📢 mining.notify job={job_id} height={height} nbits={target_bits:#010x}")


    async def send_difficulty(self, diff: float):
        await self.send({
            "id": None,
            "method": "mining.set_difficulty",
            "params": [diff],
        })

    async def send_pearl_mining_params(self):
        """✅ CONFIRMED from strings: alpha-miner waits for this before notify."""
        # Use pool dimensions (k=2048) so proof format matches pool
        await self.send({
            "id": None,
            "method": "pearl.set_mining_params",
            "params": {
                "m": 8192, "n": 32768, "k": 2048,
                "rank": 128, "mpp": 10,
                "rows": 2, "cols": 64,
            },
        })
        log.info("⛏️  pearl.set_mining_params sent (k=2048)")

    # ── Stratum message handlers ──────────────────────────────────────────────

    async def handle_configure(self, id_, params):
        """mining.configure — respond with pearl/v1 support."""
        extensions = params[0] if params else []
        result = {}
        if "pearl/v1" in extensions:
            result["pearl/v1"] = True
        await self.send_result(id_, result)
        self.configured = True
        log.info("⚙️  mining.configure → pearl/v1:true ACK")
        # ✅ Send pearl.set_mining_params IMMEDIATELY after configure ACK
        # Alpha-miner waits for this before sending subscribe
        await self.send_pearl_mining_params()
        # mining.notify will be sent after subscribe

    async def handle_subscribe(self, id_, params):
        """mining.subscribe — return session info."""
        result = [
            [["mining.notify", self.session_id]],
            self.session_id,
            4,
        ]
        await self.send_result(id_, result)
        self.subscribed = True
        log.info("📡 mining.subscribe → ACK")
        # alpha-miner waits for subscribe ACK before accepting params+notify
        await self._flush_job_buffer()

    async def _flush_job_buffer(self):
        """Send notify once subscribed (after subscribe ACK)."""
        if not self.subscribed:
            return
        if self.job_buffer:
            job = self.job_buffer[-1]
            self.job_buffer.clear()
            log.info("📬 Sending buffered job (post-subscribe)")
            await self.send_difficulty(1.0)
            await self.send_notify(job)

    async def handle_authorize(self, id_, params):
        """mining.authorize — RegisterRequest already sent at connect time."""
        await self.send_result(id_, True)
        self.authorized = True
        log.info(f"🔑 mining.authorize → ACK (already registered with pool)")
        await self._flush_job_buffer()

    async def handle_submit(self, id_, params):
        """mining.submit — translate proof to PlainProofShare.
        ✅ CONFIRMED from strings: alpha-miner sends params as DICT:
           {"plain_proof": "hex...", "mining_job": {"job_id": ...}}
        """
        self.submit_count += 1

        # Handle both dict params (pearl/v1) and array params (legacy)
        if isinstance(params, dict):
            proof_hex = params.get("plain_proof", "")
            job_info  = params.get("mining_job", {})
            job_id    = job_info.get("job_id", self.job_id) if isinstance(job_info, dict) else self.job_id
            worker    = self.worker
        elif isinstance(params, list) and len(params) > 0 and isinstance(params[0], dict):
            # params=[{"plain_proof":..., "mining_job":...}]
            d         = params[0]
            proof_hex = d.get("plain_proof", "")
            job_info  = d.get("mining_job", {})
            job_id    = job_info.get("job_id", self.job_id) if isinstance(job_info, dict) else self.job_id
            worker    = self.worker
        else:
            # fallback: [worker, job_id, proof_hex]
            worker    = params[0] if len(params) > 0 else self.worker
            job_id    = params[1] if len(params) > 1 else self.job_id
            proof_hex = params[2] if len(params) > 2 else ""

        log.info(f"📤 mining.submit #{self.submit_count} job={job_id} proof_len={len(proof_hex)}")

        try:
            proof_bytes = bytes.fromhex(proof_hex) if isinstance(proof_hex, str) and proof_hex else b""
        except Exception:
            proof_bytes = b""

        # Build PlainProofShare — reconstruct binary list format
        # Pool expects: [share_uuid, sigma, ...matrix_slices...]
        # For now forward as raw binary blob; pool will validate
        share_uuid = str(uuid.uuid4())
        job        = self.pool.current_job or []
        sigma      = job[1] if isinstance(job, list) and len(job) > 1 else b""

        # Construct proof list matching pool's expected PlainProofShare format
        share_payload = [share_uuid, sigma, proof_bytes]
        await self.pool.submit_share(job_id, share_payload)
        # Optimistically ack; pool ShareResult will confirm
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
    # Heartbeat: pool doesn't disconnect for 90s+ without HB (confirmed via MitM).
    # Akoya-miner itself never sends HBs in 90s window either.
    # Disabled until we capture exact format to avoid immediate disconnect.
    heartbeat_interval = 999999
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
                if isinstance(payload, list) and len(payload) > 0 and not payload[0]:
                    log.error("❌ Pool rejected registration!")
                    break
                log.info(f"✅ Registered! session={payload[1] if isinstance(payload,list) else ''}")
                # DO NOT send set_difficulty yet — wait until we send a job

            elif type_id == T_JOB_ASSIGNMENT:
                log.info(f"📋 JobAssignment height={payload[3] if isinstance(payload,list) and len(payload)>3 else '?'}")
                pool.current_job = payload
                # pearl/v1: wait for subscribe before sending notify
                if session.subscribed:
                    await session.send_difficulty(1.0)
                    await session.send_notify(payload)
                else:
                    session.job_buffer = [payload]  # keep latest
                    log.info(f"   ⏳ Buffered height={payload[3] if isinstance(payload,list) and len(payload)>3 else '?'} (waiting for subscribe)")

            elif type_id == T_SHARE_RESULT:
                # ✅ CONFIRMED FORMAT: [share_uuid, outcome_code, message_str]
                # e.g. ['5960d0f8-...', 0, 'Accepted']
                if isinstance(payload, list) and len(payload) >= 2:
                    share_id = payload[0]
                    outcome  = payload[1]
                    msg_str  = payload[2] if len(payload) > 2 else ""
                else:
                    share_id = ""
                    outcome  = payload.get("outcome", -1) if isinstance(payload, dict) else -1
                    msg_str  = ""

                if outcome == OUTCOME_ACCEPTED:
                    log.info(f"✅ Share ACCEPTED! id={share_id} msg={msg_str}")
                    # Notify miner of accepted share (find pending submit id)
                    await session.send_result(None, True)
                else:
                    outcome_names = {1:"Rejected", 2:"Stale", 3:"Duplicate", 4:"Invalid"}
                    log.warning(f"❌ Share {outcome_names.get(outcome,str(outcome))}: id={share_id} msg={msg_str}")
                    await session.send_result(None, False)

            elif type_id == T_DIFFICULTY_ADJUST:
                # ✅ CONFIRMED FORMAT: [new_difficulty_int]  e.g. [453019458]
                if isinstance(payload, list) and len(payload) > 0:
                    diff = float(payload[0])
                elif isinstance(payload, dict):
                    diff = float(payload.get("difficulty", pool.difficulty))
                else:
                    diff = float(payload)
                pool.difficulty = diff
                log.info(f"⚡ DifficultyAdjust → {diff}")
                # Send as stratum difficulty (normalize to reasonable range)
                await session.send_difficulty(1.0)

            elif type_id == T_HEARTBEAT_ACK:
                log.debug("💓 HeartbeatAck")

            elif type_id == T_POOL_ERROR:
                # ✅ CONFIRMED FORMAT: [error_code, message_str, is_fatal_bool]
                if isinstance(payload, list):
                    code  = payload[0] if len(payload) > 0 else "?"
                    msg_  = payload[1] if len(payload) > 1 else ""
                    fatal = payload[2] if len(payload) > 2 else False
                else:
                    code  = payload.get("code", "?") if isinstance(payload, dict) else payload
                    msg_  = payload.get("message", "") if isinstance(payload, dict) else ""
                    fatal = False
                log.error(f"🚨 PoolError code={code}: {msg_} (fatal={fatal})")
                if fatal:
                    # Code 9 = rebalancing — signal reconnect instead of drop
                    if code == 9 or "rebalancing" in str(msg_).lower():
                        session.pool_retry = True
                    break

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

    async def connect_pool():
        for attempt in range(10):
            p = PoolConnection(pool_host, pool_port)
            try:
                await p.connect()
                await p.register(wallet_hint, worker_hint)
                return p
            except Exception as e:
                log.warning(f"Pool connect attempt {attempt+1}: {e}")
                await asyncio.sleep(3)
        return None

    pool = await connect_pool()
    if not pool:
        log.error("❌ Cannot connect to pool")
        writer.close()
        return

    session = StratumSession(reader, writer, pool)
    session.wallet = wallet_hint
    session.worker = worker_hint

    while True:
        session.pool_retry = False
        await asyncio.gather(
            session.run(),
            pool_recv_loop(pool, session),
            return_exceptions=True,
        )
        if session.pool_retry:
            log.info("🔄 Pool rebalancing — reconnecting in 3s...")
            pool.close()
            await asyncio.sleep(3)
            pool = await connect_pool()
            if not pool:
                log.error("❌ Reconnect failed")
                break
            session.pool = pool
            session.subscribed = False
            session.job_buffer = []
            log.info("✅ Reconnected to pool!")
        else:
            break

    pool.close()
    log.info("Session ended")

# ── Main ─────────────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="Stratum↔MessagePack bridge for alpha-miner → akoyapool")
    parser.add_argument("--port",     type=int, default=LOCAL_PORT,  help="Local Stratum listen port")
    parser.add_argument("--upstream", type=str, default=POOL_HOST,   help="Pool host")
    parser.add_argument("--up-port",  type=int, default=POOL_PORT,   help="Pool port")
    parser.add_argument("--wallet",   type=str, default=DEFAULT_WALLET, help="Your Pearl wallet address")
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
