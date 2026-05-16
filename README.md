# pearlx 🌊⛏️

Tools dan utilities untuk mining **Pearl (PRL)** di pool [akoyapool.com](https://akoyapool.com).

---

## 📦 Isi Repo

| File | Deskripsi |
|------|-----------|
| `stratum_proxy.py` | Stratum proxy untuk menjembatani `alpha-miner` ke pool Akoya |
| `install.sh` | Installer resmi Akoya Miner untuk Linux + NVIDIA GPU |

---

## 🔧 stratum_proxy.py

Proxy Stratum yang memungkinkan `alpha-miner` (versi lama) terhubung ke `pool.akoyapool.com:3333` dengan cara memodifikasi agent string pada handshake secara transparan.

### Cara Download & Pakai di Linux

```bash
# Download langsung
wget https://raw.githubusercontent.com/$(gh api user -q .login)/pearlx/main/stratum_proxy.py

# Atau dengan curl
curl -fsSL https://raw.githubusercontent.com/$(gh api user -q .login)/pearlx/main/stratum_proxy.py -o stratum_proxy.py

# Jalankan proxy di background
python3 stratum_proxy.py &

# Arahkan alpha-miner ke proxy lokal
./alpha-miner \
  --pool stratum+tcp://127.0.0.1:13333 \
  --address YOUR_WALLET_ADDRESS \
  --worker YOUR_WORKER_NAME
```

### Opsi CLI

```
python3 stratum_proxy.py [OPTIONS]

  --port PORT        Port lokal proxy (default: 13333)
  --upstream HOST    Upstream pool host (default: pool.akoyapool.com)
  --up-port PORT     Upstream pool port (default: 3333)
  --agent AGENT      Agent string yang dikirim ke pool (default: akoya-miner/1.0.0)
```

### Contoh dengan opsi custom

```bash
python3 stratum_proxy.py \
  --port 13333 \
  --upstream pool.akoyapool.com \
  --up-port 3333 \
  --agent "akoya-miner/1.0.0"
```

---

## ⛏️ install.sh — Akoya Miner Installer

Installer resmi untuk Linux dengan NVIDIA GPU (CUDA 12.4+).

```bash
curl -sSL https://get.akoyapool.com/install.sh | sudo bash
```

**Requirements:**
- Ubuntu/Debian Linux
- NVIDIA GPU RTX 30xx atau lebih baru
- CUDA 12.4+

---

## 📋 Requirements

- Python 3.7+ (untuk `stratum_proxy.py`)
- `alpha-miner` binary (Linux x86-64)
- NVIDIA GPU dengan CUDA 12.4+

---

## 📄 License

MIT
