#!/usr/bin/env python3
"""
Stratum Proxy: alpha-miner → pool.akoyapool.com
─────────────────────────────────────────────────
Menjembatani alpha-miner (protokol lama) ke pool Akoya (protokol baru)
dengan memodifikasi Stratum handshake secara transparan.

Cara pakai:
  1. Jalankan proxy ini:
       python3 stratum_proxy.py

  2. Arahkan alpha-miner ke proxy lokal:
       ./alpha-miner \
         --pool stratum+tcp://127.0.0.1:13333 \
         --address YOUR_WALLET \
         --worker YOUR_WORKER
"""

import asyncio
import json
import logging
import sys
import argparse
from datetime import datetime

# ── Konfigurasi ──────────────────────────────────────────────────────────────
UPSTREAM_HOST = "pool.akoyapool.com"
UPSTREAM_PORT = 3333
LOCAL_PORT    = 13333
AGENT_SPOOF   = "akoya-miner/1.0.0"   # Agent yang dikirim ke pool (pura-pura akoya-miner)

# ── Logging setup ─────────────────────────────────────────────────────────────
class ColorFormatter(logging.Formatter):
    COLORS = {
        'DEBUG':    '\033[36m',   # cyan
        'INFO':     '\033[32m',   # green
        'WARNING':  '\033[33m',   # yellow
        'ERROR':    '\033[31m',   # red
        'CRITICAL': '\033[35m',   # magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        color = self.COLORS.get(record.levelname, '')
        record.levelname = f"{color}{record.levelname:<8}{self.RESET}"
        return super().format(record)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(ColorFormatter(
    fmt='%(asctime)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S'
))
log = logging.getLogger("proxy")
log.addHandler(handler)
log.setLevel(logging.DEBUG)
log.propagate = False

# ── Statistik ─────────────────────────────────────────────────────────────────
stats = {
    "connected_at": None,
    "msg_miner": 0,
    "msg_pool": 0,
    "shares_submitted": 0,
    "shares_accepted": 0,
}

# ── Modifikasi pesan dari miner sebelum dikirim ke pool ──────────────────────
def patch_miner_message(msg: dict) -> dict:
    method = msg.get("method", "")

    # mining.subscribe → ganti agent string ke akoya-miner
    if method == "mining.subscribe":
        params = msg.get("params", [])
        original_agent = params[0] if params else "(none)"
        if isinstance(params, list) and len(params) > 0:
            params[0] = AGENT_SPOOF
        else:
            params = [AGENT_SPOOF]
        msg["params"] = params
        log.warning(f"  🔧 PATCH subscribe: agent '{original_agent}' → '{AGENT_SPOOF}'")

    # mining.authorize → log wallet info
    if method == "mining.authorize":
        params = msg.get("params", [])
        if params:
            log.info(f"  👤 Auth: worker={params[0]}")

    # mining.submit → hitung shares
    if method == "mining.submit":
        stats["shares_submitted"] += 1
        log.info(f"  ⛏️  Share #{stats['shares_submitted']} submitted")

    return msg

# ── Modifikasi respons dari pool sebelum dikirim ke miner ────────────────────
def patch_pool_message(msg: dict) -> dict:
    # Deteksi share accepted
    if msg.get("id") is not None and msg.get("result") is True and msg.get("error") is None:
        stats["shares_accepted"] += 1
        log.info(f"  ✅ Share accepted! ({stats['shares_accepted']}/{stats['shares_submitted']})")

    # Deteksi reject
    if msg.get("error") and msg.get("id") is not None:
        log.warning(f"  ❌ Pool error: {msg.get('error')}")

    return msg

# ── Forward stream dalam satu arah ───────────────────────────────────────────
async def forward(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    direction: str,
    patch_fn=None,
):
    arrow = "→" if "MINER" in direction else "←"
    color = "\033[36m" if "MINER" in direction else "\033[35m"
    reset = "\033[0m"

    try:
        while True:
            raw = await reader.readline()
            if not raw:
                log.info(f"[{direction}] connection closed by peer")
                break

            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                continue

            try:
                msg = json.loads(line)
                log.debug(f"{color}[{arrow} {direction}]{reset} {json.dumps(msg)}")
                if patch_fn:
                    msg = patch_fn(msg)
                raw = (json.dumps(msg) + "\n").encode("utf-8")
            except json.JSONDecodeError:
                log.debug(f"{color}[{arrow} {direction}]{reset} (non-JSON) {line!r}")

            # Update stats
            if "MINER" in direction:
                stats["msg_miner"] += 1
            else:
                stats["msg_pool"] += 1

            writer.write(raw)
            await writer.drain()

    except (asyncio.IncompleteReadError, ConnectionResetError):
        log.info(f"[{direction}] disconnected")
    except Exception as e:
        log.error(f"[{direction}] error: {e}")
    finally:
        try:
            writer.close()
        except Exception:
            pass

# ── Handler untuk setiap client (alpha-miner) yang connect ───────────────────
async def handle_client(
    local_reader: asyncio.StreamReader,
    local_writer: asyncio.StreamWriter,
):
    peer = local_writer.get_extra_info("peername")
    log.info(f"🔌 alpha-miner connected from {peer}")
    stats["connected_at"] = datetime.now()

    # Hubungkan ke pool upstream
    try:
        up_reader, up_writer = await asyncio.open_connection(UPSTREAM_HOST, UPSTREAM_PORT)
        log.info(f"🌐 Connected to upstream {UPSTREAM_HOST}:{UPSTREAM_PORT}")
    except Exception as e:
        log.error(f"❌ Cannot connect to upstream: {e}")
        local_writer.close()
        return

    # Jalankan dua arah secara bersamaan
    await asyncio.gather(
        forward(local_reader, up_writer,    "MINER→POOL", patch_miner_message),
        forward(up_reader,    local_writer, "POOL→MINER", patch_pool_message),
    )

    elapsed = ""
    if stats["connected_at"]:
        secs = (datetime.now() - stats["connected_at"]).seconds
        elapsed = f" (session: {secs}s)"

    log.info(
        f"🔌 Session ended{elapsed} | "
        f"shares: {stats['shares_accepted']}/{stats['shares_submitted']} accepted | "
        f"msgs: ↑{stats['msg_miner']} ↓{stats['msg_pool']}"
    )

# ── Main ─────────────────────────────────────────────────────────────────────
async def main():
    global UPSTREAM_HOST, UPSTREAM_PORT, AGENT_SPOOF

    parser = argparse.ArgumentParser(description="Stratum proxy: alpha-miner → akoyapool")
    parser.add_argument("--port",     type=int, default=LOCAL_PORT,    help=f"Local listen port (default: {LOCAL_PORT})")
    parser.add_argument("--upstream", type=str, default=UPSTREAM_HOST, help=f"Upstream pool host (default: {UPSTREAM_HOST})")
    parser.add_argument("--up-port",  type=int, default=UPSTREAM_PORT, help=f"Upstream pool port (default: {UPSTREAM_PORT})")
    parser.add_argument("--agent",    type=str, default=AGENT_SPOOF,   help=f"Agent string to spoof (default: {AGENT_SPOOF})")
    args = parser.parse_args()

    UPSTREAM_HOST = args.upstream
    UPSTREAM_PORT = args.up_port
    AGENT_SPOOF   = args.agent

    server = await asyncio.start_server(handle_client, "0.0.0.0", args.port)

    print()
    print("  ╔══════════════════════════════════════════════════╗")
    print("  ║         Stratum Proxy — alpha-miner bridge       ║")
    print("  ╠══════════════════════════════════════════════════╣")
    print(f"  ║  Listen  : 0.0.0.0:{args.port}                          ║")
    print(f"  ║  Upstream: {args.upstream}:{args.up_port}           ║")
    print(f"  ║  Agent   : {args.agent}                  ║")
    print("  ╠══════════════════════════════════════════════════╣")
    print("  ║  Jalankan alpha-miner dengan:                    ║")
    print(f"  ║  --pool stratum+tcp://127.0.0.1:{args.port}            ║")
    print("  ╚══════════════════════════════════════════════════╝")
    print()

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Proxy stopped by user.")
