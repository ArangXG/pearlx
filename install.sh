#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
#  Akoya Miner — One-line installer & updater for Linux + NVIDIA GPU
#
#  Fresh install:
#    curl -sSL https://get.akoyapool.com/install.sh | sudo bash
#
#  Update to latest:
#    curl -sSL https://get.akoyapool.com/install.sh | sudo bash
#    (same command — it detects an existing install and upgrades in-place)
#
#  What it does:
#    1. Checks your NVIDIA driver and GPU
#    2. Downloads the pre-built miner (skips if already up-to-date)
#    3. Creates a config file (asks for your wallet address — skipped on update)
#    4. Installs a systemd service / PID-based launcher
#    5. Starts mining
#
#  Uninstall:
#    akoya-miner uninstall
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Configurable defaults ────────────────────────────────────────────────────
INSTALL_DIR="/opt/akoya-miner"
CONFIG_FILE="/etc/akoya-miner/config.json"
SERVICE_NAME="akoya-miner"
WRAPPER_PATH="/usr/local/bin/akoya-miner"
PID_FILE="/var/run/akoya-miner.pid"
LOG_DIR="/var/log/akoya-miner"
STATS_FILE="/tmp/akoya-miner-stats.json"
DOWNLOAD_BASE="https://get.akoyapool.com/releases"
DEFAULT_VERSION="1.0.0"
DEFAULT_WORKER_NAME="worker1"
LATEST_VERSION_URL="${LATEST_VERSION_URL:-${DOWNLOAD_BASE}/latest.txt}"
VERSION="${AKOYA_VERSION:-}"

# Colors (disabled if not a terminal)
if [[ -t 1 ]]; then
    BOLD='\033[1m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    RED='\033[0;31m'
    CYAN='\033[0;36m'
    RESET='\033[0m'
else
    BOLD='' GREEN='' YELLOW='' RED='' CYAN='' RESET=''
fi

info()  { echo -e "${GREEN}✓${RESET} $*"; }
warn()  { echo -e "${YELLOW}⚠${RESET} $*"; }
error() { echo -e "${RED}✗${RESET} $*"; }
step()  { echo -e "\n${BOLD}${CYAN}[$1/$TOTAL_STEPS]${RESET} ${BOLD}$2${RESET}"; }

fetch_url() {
    local url="$1"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "$url"
    else
        return 127
    fi
}

download_file() {
    local url="$1"
    local dest="$2"
    if command -v wget >/dev/null 2>&1; then
        wget -q --show-progress -O "$dest" "$url"
    elif command -v curl >/dev/null 2>&1; then
        curl -fSL --progress-bar -o "$dest" "$url"
    else
        return 127
    fi
}

resolve_version() {
    if [[ -n "$VERSION" ]]; then
        info "Using requested version ${VERSION}"
        return 0
    fi

    local resolved=""
    resolved=$(fetch_url "$LATEST_VERSION_URL" 2>/dev/null | tr -d '[:space:]' || true)
    if [[ -z "$resolved" ]]; then
        warn "Could not resolve latest version from $LATEST_VERSION_URL — falling back to ${DEFAULT_VERSION}"
        VERSION="$DEFAULT_VERSION"
        return 0
    fi

    if [[ ! "$resolved" =~ ^[0-9]+(\.[0-9]+)*([-.][A-Za-z0-9._-]+)?$ ]]; then
        warn "Ignoring invalid version '${resolved}' from $LATEST_VERSION_URL — falling back to ${DEFAULT_VERSION}"
        VERSION="$DEFAULT_VERSION"
        return 0
    fi

    VERSION="$resolved"
    info "Resolved latest version ${VERSION}"
}

resolve_version

# ── Detect existing install ──────────────────────────────────────────────────
IS_UPGRADE=false
OLD_VERSION=""
if [[ -f "$INSTALL_DIR/VERSION" ]]; then
    OLD_VERSION=$(cat "$INSTALL_DIR/VERSION" 2>/dev/null || echo "unknown")
    IS_UPGRADE=true
fi

if $IS_UPGRADE; then
    TOTAL_STEPS=4
else
    TOTAL_STEPS=5
fi

# ── Must run as root (or with sudo) ─────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
    error "This installer needs root access to install to /opt and create a systemd service."
    echo "  Please run:  curl -sSL https://get.akoyapool.com/install.sh | sudo bash"
    exit 1
fi

echo ""
if $IS_UPGRADE; then
    if [[ "$OLD_VERSION" == "$VERSION" ]]; then
        echo -e "${BOLD}  ╔══════════════════════════════════════════════╗${RESET}"
        echo -e "${BOLD}  ║        Akoya Miner — Reinstall / Repair      ║${RESET}"
        echo -e "${BOLD}  ║              v${VERSION}                          ║${RESET}"
        echo -e "${BOLD}  ╚══════════════════════════════════════════════╝${RESET}"
    else
        echo -e "${BOLD}  ╔══════════════════════════════════════════════╗${RESET}"
        echo -e "${BOLD}  ║             Akoya Miner — Update             ║${RESET}"
        echo -e "${BOLD}  ║          ${OLD_VERSION} → ${VERSION}                       ║${RESET}"
        echo -e "${BOLD}  ╚══════════════════════════════════════════════╝${RESET}"
    fi
    echo ""
    info "Existing installation detected"
    echo "  Your config will be preserved."
else
    echo -e "${BOLD}  ╔══════════════════════════════════════════════╗${RESET}"
        echo -e "${BOLD}  ║             Akoya Miner — Installer          ║${RESET}"
    echo -e "${BOLD}  ║              v${VERSION}                          ║${RESET}"
    echo -e "${BOLD}  ╚══════════════════════════════════════════════╝${RESET}"
fi
echo ""

# ── Step 1: Check NVIDIA driver ─────────────────────────────────────────────
step 1 "Checking your GPU..."

if ! command -v nvidia-smi >/dev/null 2>&1; then
    error "nvidia-smi not found — NVIDIA driver doesn't seem to be installed."
    echo ""
    echo "  Install it with:"
    echo "    Ubuntu/Debian: sudo apt install nvidia-driver-545"
    echo "    Then reboot and try again."
    exit 1
fi

# Get CUDA version from driver
cuda_version=$(nvidia-smi 2>/dev/null | grep -oP 'CUDA Version: \K[0-9]+\.[0-9]+' || true)
if [[ -z "$cuda_version" ]]; then
    error "Could not read CUDA version from nvidia-smi."
    echo "  Make sure your NVIDIA driver is working: nvidia-smi"
    exit 1
fi

cuda_major="${cuda_version%.*}"
cuda_minor="${cuda_version#*.}"

if (( cuda_major < 12 )) || (( cuda_major == 12 && cuda_minor < 4 )); then
    error "Your NVIDIA driver supports CUDA $cuda_version, but Pearl needs CUDA 12.4+."
    echo ""
    echo "  Update your driver:"
    echo "    Ubuntu/Debian: sudo apt install nvidia-driver-545"
    echo "    Then reboot and try again."
    exit 1
fi

info "NVIDIA driver OK (CUDA $cuda_version)"

# Detect GPU names and compute capabilities
gpu_names=$(nvidia-smi --query-gpu=name --format=csv,noheader,nounits 2>/dev/null || true)
gpu_count=$(echo "$gpu_names" | wc -l)
sm_versions=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader,nounits 2>/dev/null | sort -u || true)

# Check all GPUs are sm_80+
unsupported=0
for sm in $sm_versions; do
    sm_int=$(echo "$sm" | tr -d '.')
    if (( sm_int < 80 )); then
        unsupported=1
    fi
done

if [[ $unsupported -eq 1 ]]; then
    warn "Some GPUs have compute capability < 8.0 (older than RTX 3060)."
    echo "  Those GPUs will be skipped. Pearl requires RTX 30-series or newer."
fi

echo "  Found $gpu_count GPU(s):"
echo "$gpu_names" | while read -r name; do
    echo "    • $name"
done

# Decide on H100-optimized vs portable build
has_hopper=false
for sm in $sm_versions; do
    sm_int=$(echo "$sm" | tr -d '.')
    if (( sm_int >= 90 )); then
        has_hopper=true
        break
    fi
done

if $has_hopper; then
    info "Detected Hopper GPU — will use optimized H100 GEMM kernel"
else
    info "Will use portable GEMM kernel (RTX 30xx / 40xx / 50xx)"
fi

# ── Step 2: Download ────────────────────────────────────────────────────────

# On upgrade, stop the running miner first so we can replace files
if $IS_UPGRADE; then
    # Stop via wrapper if available, otherwise try systemctl/pidfile.
    if command -v akoya-miner >/dev/null 2>&1; then
        akoya-miner stop 2>/dev/null || true
    elif command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        systemctl stop "$SERVICE_NAME" 2>/dev/null || true
    elif [[ -f "$PID_FILE" ]]; then
        pid=$(cat "$PID_FILE")
        kill "$pid" 2>/dev/null || true
        sleep 2
    fi
    info "Stopped running miner for update"
fi

step 2 "Downloading Akoya Miner..."

TARBALL="akoya-miner-${VERSION}-portable.tar.gz"
DOWNLOAD_URL="${DOWNLOAD_BASE}/${VERSION}/${TARBALL}"

# Create install directory
mkdir -p "$INSTALL_DIR"

# Download with progress
if ! download_file "$DOWNLOAD_URL" "/tmp/$TARBALL"; then
    if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
        error "Neither curl nor wget found. Install one and try again."
    else
        error "Download failed. Check your internet connection."
        echo "  URL: $DOWNLOAD_URL"
    fi
    exit 1
fi

# Verify SHA256 if available
SHA_URL="${DOWNLOAD_URL}.sha256"
expected_sha=$(curl -sSfL "$SHA_URL" 2>/dev/null | awk '{print $1}' || true)
if [[ -n "$expected_sha" ]]; then
    actual_sha=$(sha256sum "/tmp/$TARBALL" | awk '{print $1}')
    if [[ "$expected_sha" != "$actual_sha" ]]; then
        error "Download verification failed! The file may be corrupted."
        echo "  Expected: $expected_sha"
        echo "  Got:      $actual_sha"
        rm -f "/tmp/$TARBALL"
        exit 1
    fi
    info "Download verified (SHA256 OK)"
else
    warn "Could not verify download (SHA256 file not found)"
fi

# Extract
tar -xzf "/tmp/$TARBALL" -C "$INSTALL_DIR" --strip-components=1
rm -f "/tmp/$TARBALL"

chmod +x "$INSTALL_DIR/akoya-miner" "$INSTALL_DIR/akoya-miner.bin" 2>/dev/null || true

# Create GPU auto-detect script for GEMM kernel selection
cat > "$INSTALL_DIR/detect-gpu.sh" <<'DETECT'
#!/usr/bin/env bash
# Auto-detect GPU and symlink the optimal GEMM kernel
LIB_DIR="/opt/akoya-miner/lib"
SYMLINK="$LIB_DIR/libpearl_gemm_capi.so"
[[ -L "$SYMLINK" ]] && exit 0
CC=$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader,nounits 2>/dev/null | head -1)
MAJOR="${CC%%.*}"
if [[ "$MAJOR" == "9" ]] && [[ -f "$LIB_DIR/libpearl_gemm_capi_h100.so" ]]; then
    ln -sf libpearl_gemm_capi_h100.so "$SYMLINK"
else
    ln -sf libpearl_gemm_capi_portable.so "$SYMLINK"
fi
DETECT
chmod +x "$INSTALL_DIR/detect-gpu.sh"

info "Installed to $INSTALL_DIR"

# ── Step 3: Configure (skipped on upgrade) ──────────────────────────────────
CURRENT_STEP=3

if $IS_UPGRADE; then
    info "Config preserved at $CONFIG_FILE"
    wallet_address="(existing config)"
else
    step $CURRENT_STEP "Setting up your miner..."

    mkdir -p "$(dirname "$CONFIG_FILE")"
    echo ""
    echo -e "  ${BOLD}You need a Pearl wallet address to receive mining rewards.${RESET}"
    echo "  It starts with 'prl1...' — get one from the Pearl wallet app."
    echo ""

    # When piped via `curl | bash`, stdin is the curl stream, not the terminal.
    # We need to read from /dev/tty instead.
    if [[ -t 0 ]] || [[ -e /dev/tty ]]; then
        read -rp "  Your Pearl wallet address: " wallet_address </dev/tty

        while [[ -z "$wallet_address" ]]; do
            echo -e "  ${RED}Wallet address cannot be empty.${RESET}"
            read -rp "  Your Pearl wallet address: " wallet_address </dev/tty
        done

        # Ask for a worker name (optional)
        read -rp "  Worker name [$DEFAULT_WORKER_NAME]: " worker_name </dev/tty
        worker_name="${worker_name:-$DEFAULT_WORKER_NAME}"

        # Ask for pool (with default)
        default_pool="pool.akoyapool.com:3333"
        read -rp "  Pool server [$default_pool]: " pool_url </dev/tty
        pool_url="${pool_url:-$default_pool}"
    else
        # Truly non-interactive (no terminal at all) — write placeholder config
        wallet_address="YOUR_WALLET_ADDRESS_HERE"
        worker_name="$DEFAULT_WORKER_NAME"
        pool_url="pool.akoyapool.com:3333"
        warn "Non-interactive mode — edit $CONFIG_FILE with your wallet address before starting."
    fi

    cat > "$CONFIG_FILE" <<EOF
{
  "pool": {
    "url": "${pool_url}",
    "wallet": "${wallet_address}",
    "worker": "${worker_name}"
  },
  "logging": {
    "level": "info"
  },
  "devices": "all"
}
EOF

    info "Config written to $CONFIG_FILE"
fi

# ── Step N: Install service / wrapper ────────────────────────────────────────
CURRENT_STEP=$((CURRENT_STEP + 1))
step $CURRENT_STEP "Setting up auto-start..."

SERVICE_LD_LIBRARY_PATH="${INSTALL_DIR}/lib"
if [[ -d /usr/lib/wsl/lib ]]; then
    SERVICE_LD_LIBRARY_PATH="${SERVICE_LD_LIBRARY_PATH}:/usr/lib/wsl/lib"
fi

cat > "/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=Akoya Miner
After=network-online.target nvidia-persistenced.service
Wants=network-online.target

[Service]
Type=simple
Environment="LD_LIBRARY_PATH=${SERVICE_LD_LIBRARY_PATH}"
Environment="AKOYA_HIVEOS_STATS_PATH=${STATS_FILE}"
ExecStartPre=${INSTALL_DIR}/detect-gpu.sh
ExecStart=${INSTALL_DIR}/akoya-miner.bin --config ${CONFIG_FILE}
Restart=on-failure
RestartSec=10
Nice=-5

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${INSTALL_DIR} ${LOG_DIR} /tmp

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=akoya-miner

# Allow GPU access
SupplementaryGroups=video render

[Install]
WantedBy=multi-user.target
EOF

# Create log directory
mkdir -p "$LOG_DIR"

# Create convenience wrapper
cat > "$WRAPPER_PATH" <<'WRAPPER'
#!/usr/bin/env bash
# akoya-miner — convenience wrapper
# Works with systemd (native Linux) or PID file (WSL / no-systemd)
set -euo pipefail

SERVICE="akoya-miner"
CONFIG="/etc/akoya-miner/config.json"
INSTALL="/opt/akoya-miner"
PIDFILE="/var/run/akoya-miner.pid"
LOGFILE="/var/log/akoya-miner/miner.log"

# Detect whether systemd is the active init (canonical sd_booted() check).
# We deliberately do NOT use `systemctl is-system-running`: on WSL2 it often
# returns "degraded" or "offline" (non-zero exit) even when systemd is up
# and managing units, which would falsely route us to the PID-file branch.
has_systemd() {
    [[ -d /run/systemd/system ]]
}

# True if a systemd unit file is installed for our service.
has_unit() {
    [[ -f "/etc/systemd/system/${SERVICE}.service" ]] \
      || [[ -f "/lib/systemd/system/${SERVICE}.service" ]] \
      || [[ -f "/usr/lib/systemd/system/${SERVICE}.service" ]]
}

# Check if miner process is alive. We check BOTH systemd (if a unit exists)
# AND the pidfile, so that a service started by either path is detectable.
is_running() {
    if has_systemd && has_unit; then
        systemctl is-active --quiet "$SERVICE" 2>/dev/null && return 0
    fi
    if [[ -f "$PIDFILE" ]]; then
        local pid
        pid=$(cat "$PIDFILE" 2>/dev/null) || return 1
        [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && return 0
    fi
    # Last-resort fall-back: locate by exe path. Survives lost pidfiles
    # and the WSL "systemd partially up" case.
    if [[ -x "$INSTALL/akoya-miner.bin" ]]; then
        local found
        found=$(pgrep -f "$INSTALL/akoya-miner.bin" 2>/dev/null | head -1) || true
        [[ -n "${found:-}" ]] && return 0
    fi
    return 1
}

do_start() {
    if is_running; then
        echo "✓ Akoya Miner is already running"
        return 0
    fi

    if [[ ! -f "$CONFIG" ]]; then
        echo "✗ No config file found at $CONFIG"
        echo "  Run: akoya-miner config"
        return 1
    fi

    if has_systemd && has_unit; then
        sudo systemctl start "$SERVICE"
    else
        mkdir -p "$(dirname "$LOGFILE")" "$(dirname "$PIDFILE")"
        # Auto-detect GPU for GEMM kernel
        "$INSTALL/detect-gpu.sh" 2>/dev/null || true
        echo "  Starting Akoya Miner (logging to $LOGFILE)..."
        # Include WSL CUDA path if present
        local wsl_lib=""
        [[ -d /usr/lib/wsl/lib ]] && wsl_lib="/usr/lib/wsl/lib"
        export LD_LIBRARY_PATH="${INSTALL}/lib${wsl_lib:+:$wsl_lib}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
        # On non-HiveOS, write stats to /tmp
        export AKOYA_HIVEOS_STATS_PATH="${AKOYA_HIVEOS_STATS_PATH:-/tmp/akoya-miner-stats.json}"
        nohup "$INSTALL/akoya-miner.bin" --config "$CONFIG" >> "$LOGFILE" 2>&1 &
        echo $! > "$PIDFILE"
    fi

    # Wait briefly and verify it's alive
    sleep 2
    if is_running; then
        echo "✓ Akoya Miner started"
        echo "  View logs: akoya-miner log"
    else
        echo "✗ Akoya Miner failed to start — check logs:"
        echo "  akoya-miner log"
        return 1
    fi
}

do_stop() {
    if ! is_running; then
        echo "Akoya Miner is not running"
        return 0
    fi

    # Try every shutdown path we know about — a miner started by one
    # mechanism may be visible only to another (e.g. systemd-started but
    # is_running matched via pgrep fall-back).
    local stopped=0

    if has_systemd && has_unit && systemctl is-active --quiet "$SERVICE" 2>/dev/null; then
        sudo systemctl stop "$SERVICE" && stopped=1
    fi

    if [[ -f "$PIDFILE" ]]; then
        local pid
        pid=$(cat "$PIDFILE" 2>/dev/null || true)
        if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            for i in $(seq 1 10); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
            stopped=1
        fi
        rm -f "$PIDFILE" 2>/dev/null || sudo rm -f "$PIDFILE" 2>/dev/null || true
    fi

    # Catch orphaned processes (lost pidfile, sudo-started by another shell, …).
    if [[ -x "$INSTALL/akoya-miner.bin" ]]; then
        local pids
        pids=$(pgrep -f "$INSTALL/akoya-miner.bin" 2>/dev/null || true)
        if [[ -n "$pids" ]]; then
            for p in $pids; do
                kill "$p" 2>/dev/null || sudo kill "$p" 2>/dev/null || true
            done
            sleep 1
            pids=$(pgrep -f "$INSTALL/akoya-miner.bin" 2>/dev/null || true)
            for p in $pids; do
                kill -9 "$p" 2>/dev/null || sudo kill -9 "$p" 2>/dev/null || true
            done
            stopped=1
        fi
    fi

    if [[ "$stopped" == "1" ]]; then
        echo "✓ Akoya Miner stopped"
    else
        echo "Akoya Miner was not running"
    fi
}

case "${1:-status}" in
    start)
        do_start
        ;;
    stop)
        do_stop
        ;;
    restart)
        do_stop
        do_start
        ;;
    status)
        if is_running; then
            echo "✓ Akoya Miner is running"
            if [[ -f "$PIDFILE" ]] && ! has_systemd; then
                echo "  PID: $(cat "$PIDFILE")"
            fi
        else
            echo "✗ Akoya Miner is not running"
        fi
        echo ""
        # Show quick stats
        stats_file="/tmp/akoya-miner-stats.json"
        if [[ -f "$stats_file" ]] && command -v python3 >/dev/null 2>&1; then
            python3 -c "
import json, sys, time, os
try:
    s = json.load(open('$stats_file'))
    sh = s.get('shares', {})
    gpus = s.get('gpus', [])
    up_h = s.get('uptime_seconds', 0) / 3600
    age = time.time() - os.path.getmtime('$stats_file')
    stale = ' (stale — last updated {:.0f}s ago)'.format(age) if age > 30 else ''
    print(f'  Uptime:    {up_h:.1f} hours{stale}')
    print(f'  Shares:    {sh.get(\"accepted\", 0)} accepted, {sh.get(\"rejected\", 0)} rejected')
    print(f'  GPUs:      {len(gpus)}')
    for g in gpus:
        print(f'    GPU {g[\"index\"]}: {g.get(\"temp_c\", \"?\")}°C  {g.get(\"fan_pct\", \"?\")}% fan  {g.get(\"power_w\", \"?\")}W')
except: pass
"
        elif [[ ! -f "$stats_file" ]]; then
            echo "  (no stats yet — miner may still be starting)"
        fi
        ;;
    log|logs)
        if has_systemd; then
            journalctl -u "$SERVICE" -f --no-pager -n 50
        elif [[ -f "$LOGFILE" ]]; then
            tail -f -n 50 "$LOGFILE"
        else
            echo "No log file found at $LOGFILE"
            echo "  Start the miner first: akoya-miner start"
        fi
        ;;
    config)
        if [[ ! -f "$CONFIG" ]]; then
            mkdir -p "$(dirname "$CONFIG")"
            cat > "$CONFIG" <<DEFCONF
{
  "pool": {
    "url": "pool.akoyapool.com:3333",
    "wallet": "YOUR_WALLET_ADDRESS_HERE",
    "worker": "${DEFAULT_WORKER_NAME}"
  },
  "logging": { "level": "info" },
  "devices": "all"
}
DEFCONF
            echo "Created default config at $CONFIG"
        fi
        if [[ -t 0 ]]; then
            ${EDITOR:-nano} "$CONFIG"
            echo ""
            echo "Config saved. Run 'akoya-miner restart' to apply."
        else
            cat "$CONFIG"
        fi
        ;;
    uninstall)
        echo "Uninstalling Akoya Miner..."
        do_stop 2>/dev/null || true
        if has_systemd; then
            sudo systemctl disable "$SERVICE" 2>/dev/null || true
            sudo rm -f "/etc/systemd/system/${SERVICE}.service"
            sudo systemctl daemon-reload
        fi
        sudo rm -rf "$INSTALL"
        sudo rm -rf "$CONFIG" "$(dirname "$CONFIG")"
        sudo rm -f "$PIDFILE" "$LOGFILE"
        sudo rm -f "/usr/local/bin/akoya-miner"
        echo "✓ Akoya Miner uninstalled"
        ;;
    version)
        wsl_lib=""
        [[ -d /usr/lib/wsl/lib ]] && wsl_lib="/usr/lib/wsl/lib"
        LD_LIBRARY_PATH="${INSTALL}/lib${wsl_lib:+:$wsl_lib}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
            "$INSTALL/akoya-miner.bin" version 2>/dev/null \
            || echo "akoya-miner v$(cat "$INSTALL/VERSION" 2>/dev/null || echo unknown)"
        ;;
    help|--help|-h)
        echo "Akoya Miner"
        echo ""
        echo "Usage: akoya-miner <command>"
        echo ""
        echo "Commands:"
        echo "  start      Start mining"
        echo "  stop       Stop mining"
        echo "  restart    Restart the miner"
        echo "  status     Show miner status and GPU stats"
        echo "  log        Follow live miner logs"
        echo "  config     Edit your wallet/pool settings"
        echo "  version    Show miner version"
        echo "  uninstall  Remove Akoya Miner from this system"
        echo ""
        echo "Config file: $CONFIG"
        ;;
    *)
        echo "Unknown command: $1"
        echo "Run 'akoya-miner help' for usage."
        exit 1
        ;;
esac
WRAPPER

chmod +x "$WRAPPER_PATH"

# Also handle non-systemd (WSL) — check before calling systemctl
if command -v systemctl >/dev/null 2>&1 && [[ -d /run/systemd/system ]]; then
    systemctl daemon-reload
    systemctl enable "$SERVICE_NAME" >/dev/null 2>&1
    info "Systemd service installed (starts on boot)"
else
    info "Wrapper installed (no systemd — use 'akoya-miner start/stop')"
fi

# ── Final step: Start mining! ───────────────────────────────────────────────
CURRENT_STEP=$((CURRENT_STEP + 1))
step $CURRENT_STEP "Starting the miner..."

if [[ "$wallet_address" == "YOUR_WALLET_ADDRESS_HERE" ]]; then
    warn "Placeholder wallet — edit your config first:"
    echo ""
    echo "  akoya-miner config"
    echo "  akoya-miner start"
else
    # Use the wrapper — it handles both systemd and PID-file modes
    "$WRAPPER_PATH" start 2>/dev/null || {
        warn "Miner may have failed to start. Check logs:"
        echo "  akoya-miner log"
    }
fi

# ── Done! ────────────────────────────────────────────────────────────────────
echo ""
if $IS_UPGRADE; then
    if [[ "$OLD_VERSION" == "$VERSION" ]]; then
        echo -e "${BOLD}  ╔══════════════════════════════════════════════╗${RESET}"
        echo -e "${BOLD}  ║          ✓ Reinstall complete!               ║${RESET}"
        echo -e "${BOLD}  ╚══════════════════════════════════════════════╝${RESET}"
    else
        echo -e "${BOLD}  ╔══════════════════════════════════════════════╗${RESET}"
        echo -e "${BOLD}  ║          ✓ Update complete!                  ║${RESET}"
        echo -e "${BOLD}  ║          ${OLD_VERSION} → ${VERSION}                       ║${RESET}"
        echo -e "${BOLD}  ╚══════════════════════════════════════════════╝${RESET}"
    fi
else
    echo -e "${BOLD}  ╔══════════════════════════════════════════════╗${RESET}"
    echo -e "${BOLD}  ║          ✓ Installation complete!            ║${RESET}"
    echo -e "${BOLD}  ╚══════════════════════════════════════════════╝${RESET}"
fi
echo ""
echo "  Useful commands:"
echo ""
echo "    akoya-miner status    — see if it's running + GPU stats"
echo "    akoya-miner log       — watch live mining output"
echo "    akoya-miner config    — change wallet, pool, or worker name"
echo "    akoya-miner restart   — apply config changes"
echo "    akoya-miner stop      — stop mining"
echo "    akoya-miner uninstall — remove everything"
if ! $IS_UPGRADE; then
    echo ""
    echo "  Your mining rewards go to:"
    echo "    ${wallet_address}"
fi
echo ""
echo "  To update in the future, just run this script again."
echo "  Happy mining! ⛏️"
echo ""

# ── Final line: wallet-prefilled dashboard URL ───────────────────────────────
# This is the last stdout line on purpose — it's the link the user opens to see
# their hashrate and pending balance. If you scrape this script's output, this
# is the line to grab.
DASHBOARD_BASE="${AKOYA_DASHBOARD_URL:-https://akoyapool.com/dashboard}"
if [[ -n "$wallet_address" && "$wallet_address" != "YOUR_WALLET_ADDRESS_HERE" && "$wallet_address" != "(existing config)" ]]; then
    echo "  Open your dashboard:"
    echo "    ${DASHBOARD_BASE}?w=${wallet_address}"
else
    echo "  Open your dashboard once your wallet is set:"
    echo "    ${DASHBOARD_BASE}"
fi
echo ""
