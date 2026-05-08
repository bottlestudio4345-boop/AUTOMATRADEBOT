"""
╔══════════════════════════════════════════════════════════════════════════╗
║  BOT AUTO TRADE — BINANCE LIVE × PA + S&D + S/R + BTC MULTI-TF ENGINE   ║
║  v2 UPDATE:                                                              ║
║  ✅ RALLY EXHAUSTION GATE: Blok LONG saat BTC kehabisan bensin          ║
║     • is_rally_exhausted(): Stoch RSI %K > 78 + gap mengecil +          ║
║       upper wick rejection candle BTC 1H                                ║
║     • Toggle: EXHAUSTION_ENABLED = True/False                           ║
║  ✅ AVERAGE MARKET RSI GATE: Patokan kondisi market dari semua pair     ║
║     • Dihitung SETELAH scan selesai (bukan per-pair isolated)           ║
║     • Avg RSI > 70 → blok semua LONG | < 30 → blok semua SHORT         ║
║     • Print di setiap scan: "📊 Average Market RSI: xx.xx"             ║
║                                                                          ║
║  SIGNAL METHOD:                                                          ║
║  ✅ HTF Trend Bias (4H/1D) — via swing high/low structure               ║
║  ✅ Price Action Patterns: Pin Bar, Engulfing, Inside Bar               ║
║  ✅ Supply & Demand Zones — fresh zone detection (unmitigated)          ║
║  ✅ Support & Resistance Levels — swing-based + touch-count filter      ║
║  ✅ Scoring System (max 100+ pts) — fire at ≥ 60 pts (semua tier, hard minimum)║
║  ✅ Volume: gradasi 1.2x/1.5x/2x/3x confirmation                       ║
║  ✅ Session: London/New York +5pts | London-NY Overlap +8pts            ║
║  ✅ BTC Multi-TF: Daily→H4→H1 | Stoch RSI 5,3,3 | Divergence detect    ║
║  ✅ BTC Gate: Daily RANGING → skip semua posisi (tidak ada setup)       ║
║  ✅ BTC BULLISH → Long alt OK | BTC BEARISH → Short alt OK             ║
║  ✅ BTCD naik + BTC bearish → Short OK (alt bleeding)                  ║
║  ✅ Divergence H1/H4 BTC → bonus score entry timing                    ║
║  ✅ RR ≥ 1.5 required — Risk Management enforced                        ║
║  ✅ Multi-mode scan: INTRADAY (1d→1h) + SCALPING (4h→1h)               ║
║     LOW_TF: HTF 1h → entry 15m (RELAXED, sinyal lebih sering)          ║
║     LTF_30M: HTF 1h → entry 30m (RELAXED tier)                         ║
║                                                                          ║
║  EXECUTION:                                                              ║
║  ✅ Auto trading Binance Futures LIVE/DEMO (toggle via command)         ║
║  ✅ Dynamic risk, leverage, SL/TP order, trailing stop                  ║
║  ✅ Max open trades, drawdown protection                                 ║
║  ✅ Telegram alerts + daily summary + hourly report                     ║
║  ✅ Sinkron posisi pre-existing (sudah ada sebelum bot start)            ║
║  ✅ Telegram commands via polling                                        ║
║       /pnl              — lihat unrealized PnL semua posisi             ║
║       /status           — status bot + posisi aktif                     ║
║       /changemargin <symbol> <ISOLATED|CROSSED>                         ║
║       /changelev <symbol> <leverage>                                    ║
║       /resumeorpause    — toggle pause/resume bot                       ║
║       /closeallposition — tutup semua posisi aktif                      ║
║       /changeliveordemo — toggle LIVE ↔ DEMO (mainnet/testnet)         ║
║       /setmarginratio <persen>  — set max SL loss % per trade (1% default)       ║
║       /setfixedlev <leverage>   — leverage tetap semua trade (override tier)║
║       /resetmm                  — reset ke MM dinamis (risk % + auto-tier)  ║
║       /setscoreupto <score>     — filter min score sinyal (1–100, 0=reset) ║
║       /togglebtcfilter          — toggle filter korelasi BTC+BTC.D ON/OFF   ║
║       /backtest <SYMBOL> <TF> [DAYS] — backtest sinyal PA+S&D+S/R      ║
║       /scalpingonly             — aktifkan hanya LOW_TF + LTF_30M (15m/30m) ║
║       /intradayonly             — aktifkan hanya INTRADAY + SCALPING (4h→1h) ║
║       /allmode                  — kembali ke semua mode scan (default)      ║
║       /settp1profit <persen>    — set TP1 sebagai % fixed dari entry price  ║
║       /resettp1                 — reset TP1 ke otomatis (struktur market)   ║
║       /settp1partial <persen>   — set % lot yang di-close di TP1          ║
║       /maxdailyloss <persen>    — set max daily loss % (0=off, auto-pause) ║
║       /maxdailywin <persen>     — set max daily win % (0=off, auto-pause)  ║
║       /resetdailylimit          — reset baseline PnL harian ke saldo kini  ║
║       /longonly                 — hanya proses sinyal LONG (BULLISH)       ║
║       /shortonly                — hanya proses sinyal SHORT (BEARISH)      ║
║       /resetdirection           — reset filter arah (LONG + SHORT)         ║
║       /superscalpermode         — toggle Super Scalper Mode ON/OFF         ║
║         ON: 25 pair likuid | 15m/30m | score≥35 | TP1=0.8% | RR≥1.2      ║
║                                                                          ║
║  BTC SITUATIONAL AWARENESS (always active):                              ║
║  ✅ BTC near demand + LTF bounce → SHORT alt diblok (snap-back risk)    ║
║  ✅ BTC near supply + LTF reject → LONG alt diblok (rejection risk)     ║
║  ✅ BTC strong trend → bonus score untuk arah searah                    ║
║  ✅ BTC ranging/flat → penalty score (hati-hati semua sinyal alt)       ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import time
import hmac
import hashlib
import math
import threading
import requests
import pandas as pd
from datetime import datetime, timezone
from urllib.parse import urlencode
from typing import Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 0 — PERSISTENCE (State disimpan ke disk, survive Railway restart)
# ═══════════════════════════════════════════════════════════════════════════
#
# Railway adalah stateless container — semua variabel in-memory hilang saat restart.
# Solusi: simpan state penting ke file JSON di direktori /data (Railway Volume)
# atau fallback ke direktori file ini jika Volume tidak di-mount.
#
# State yang di-persist:
#   • BOT_MODE, MARGIN_RATIO, FIXED_LEVERAGE, MAX_OPEN_TRADES, _user_set_max_trades
#   • bot_paused, MIN_SCORE_CUSTOM, MIN_SCORE_RELAXED_CUSTOM
#   • BTC_CORR_FILTER_ON, _ACTIVE_MODE_FILTER
#   • TP1_PROFIT_PCT, TP1_PARTIAL
#   • DAILY_LOSS_LIMIT_PCT, DAILY_WIN_LIMIT_PCT
#   • bot_state (wins/losses/streaks/PnL tracking)
#   • _daily_limit_state
#   • _signal_hashes (cooldown dedup sinyal)
#   • _cooldown_map
#
# Cara setup Railway Volume:
#   1. Railway Dashboard → project → Add Volume
#   2. Mount Path: /data
#   3. Deploy ulang — bot otomatis pakai /data/bot_state.json

# ── Tentukan direktori penyimpanan ──────────────────────────────────────────
def _get_data_dir() -> str:
    """
    Prioritas:
    1. /data  (Railway Volume — persist lintas restart)
    2. Direktori file main.py (fallback lokal)
    """
    if os.path.isdir("/data"):
        return "/data"
    return os.path.dirname(os.path.abspath(__file__))

_STATE_FILE = os.path.join(_get_data_dir(), "bot_state.json")
_state_lock = threading.Lock()

def _serialize_state() -> dict:
    """Kumpulkan semua state global yang perlu di-persist ke dict JSON-safe."""
    # Konversi _signal_hashes: datetime → ISO string
    sig_hashes_serial = {
        k: v.isoformat() for k, v in _signal_hashes.items()
    } if "_signal_hashes" in dir() else {}

    # Konversi _cooldown_map: datetime → ISO string
    cooldown_serial = {
        k: v.isoformat() for k, v in _cooldown_map.items()
    } if "_cooldown_map" in dir() else {}

    # bot_state: konversi datetime fields
    bs = dict(bot_state)
    if isinstance(bs.get("start_time"), datetime):
        bs["start_time"] = bs["start_time"].isoformat()

    # _daily_limit_state: sudah string/float/None — safe
    dls = dict(_daily_limit_state)

    # Konversi pending_limit_orders: datetime → ISO string agar JSON-safe
    # Field "mode" dan "signal" mungkin ada nested object non-JSON — handle dengan try
    pending_serial = {}
    for _sym, _pend in pending_limit_orders.items():
        try:
            _p = dict(_pend)
            if isinstance(_p.get("placed_at"), datetime):
                _p["placed_at"] = _p["placed_at"].isoformat()
            # Pastikan semua field JSON-serializable (skip field yang tidak bisa)
            json.dumps(_p)   # test serialize
            pending_serial[_sym] = _p
        except Exception:
            # Simpan hanya field minimal yang pasti JSON-safe
            try:
                pending_serial[_sym] = {
                    "order_id":  _pend.get("order_id"),
                    "direction": _pend.get("direction"),
                    "entry":     _pend.get("entry"),
                    "sl":        _pend.get("sl"),
                    "tp1":       _pend.get("tp1"),
                    "tp2":       _pend.get("tp2"),
                    "lot":       _pend.get("lot"),
                    "tp1_size":  _pend.get("tp1_size"),
                    "tp2_size":  _pend.get("tp2_size"),
                    "side":      _pend.get("side"),
                    "sl_side":   _pend.get("sl_side"),
                    "position_side": _pend.get("position_side", "BOTH"),
                    "tick_size": _pend.get("tick_size"),
                    "eff_prec":  _pend.get("eff_prec"),
                    "step_size": _pend.get("step_size"),
                    "mkt_step_size": _pend.get("mkt_step_size"),
                    "min_qty":   _pend.get("min_qty"),
                    "placed_at": _pend["placed_at"].isoformat() if isinstance(_pend.get("placed_at"), datetime) else _pend.get("placed_at"),
                }
            except Exception:
                pass  # skip jika benar-benar tidak bisa serialize

    return {
        "BOT_MODE":                  BOT_MODE,
        "MAX_SL_LOSS_PCT":           MAX_SL_LOSS_PCT,
        "MARGIN_RATIO":              MAX_SL_LOSS_PCT,   # backward-compat
        "FIXED_LEVERAGE":            FIXED_LEVERAGE,
        "MAX_OPEN_TRADES":           MAX_OPEN_TRADES,
        "_user_set_max_trades":      _user_set_max_trades,
        "bot_paused":                bot_paused,
        "MIN_SCORE_CUSTOM":          MIN_SCORE_CUSTOM,
        "MIN_SCORE_RELAXED_CUSTOM":  MIN_SCORE_RELAXED_CUSTOM,
        "BTC_CORR_FILTER_ON":        BTC_CORR_FILTER_ON,
        "_ACTIVE_MODE_FILTER":       _ACTIVE_MODE_FILTER,
        "_DIRECTION_FILTER":         _DIRECTION_FILTER,
        "_SUPER_SCALPER_MODE":       _SUPER_SCALPER_MODE,
        "_PRE_SUPERSCALPER_SNAPSHOT": _PRE_SUPERSCALPER_SNAPSHOT,
        "TP1_PROFIT_PCT":            TP1_PROFIT_PCT,
        "TP1_PARTIAL":               TP1_PARTIAL,
        "DAILY_LOSS_LIMIT_PCT":      DAILY_LOSS_LIMIT_PCT,
        "DAILY_WIN_LIMIT_PCT":       DAILY_WIN_LIMIT_PCT,
        "bot_state":                 bs,
        "_daily_limit_state":        dls,
        "_signal_hashes":            sig_hashes_serial,
        "_cooldown_map":             cooldown_serial,
        "_pending_limit_orders":     pending_serial,
        "_saved_at":                 datetime.now(timezone.utc).isoformat(),
    }

def save_state():
    """
    Simpan state bot ke disk (thread-safe).
    Dipanggil setiap kali ada perubahan setting atau setelah trade.
    """
    try:
        with _state_lock:
            data = _serialize_state()
            tmp  = _STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            os.replace(tmp, _STATE_FILE)   # atomic replace — tidak corrupt jika crash
        print(f"💾 State tersimpan → {_STATE_FILE}")
    except Exception as e:
        print(f"⚠️  Gagal simpan state: {e}")

def load_state():
    """
    Load state dari disk saat startup.
    Jika file tidak ada atau rusak → pakai default (fresh start).
    Dipanggil SEKALI di awal main() sebelum telegram polling.
    """
    global BOT_MODE, MAX_SL_LOSS_PCT, MARGIN_RATIO, FIXED_LEVERAGE, MAX_OPEN_TRADES
    global _user_set_max_trades, bot_paused, MIN_SCORE_CUSTOM, MIN_SCORE_RELAXED_CUSTOM
    global BTC_CORR_FILTER_ON, _ACTIVE_MODE_FILTER, _DIRECTION_FILTER, TP1_PROFIT_PCT, TP1_PARTIAL
    global DAILY_LOSS_LIMIT_PCT, DAILY_WIN_LIMIT_PCT
    global bot_state, _daily_limit_state, _signal_hashes, _cooldown_map
    global _SUPER_SCALPER_MODE, _PRE_SUPERSCALPER_SNAPSHOT

    if not os.path.exists(_STATE_FILE):
        print(f"ℹ️  State file tidak ditemukan ({_STATE_FILE}) — mulai fresh.")
        return

    try:
        with open(_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        saved_at = data.get("_saved_at", "?")
        print(f"📂 Memuat state dari disk... (disimpan: {saved_at})")

        # ── Setting variables ──────────────────────────────────────────────
        BOT_MODE                 = data.get("BOT_MODE",                 BOT_MODE)
        # Load MAX_SL_LOSS_PCT — fallback ke MARGIN_RATIO lama untuk backward-compat
        _loaded_sl = data.get("MAX_SL_LOSS_PCT", data.get("MARGIN_RATIO", MAX_SL_LOSS_PCT))
        MAX_SL_LOSS_PCT          = _loaded_sl
        MARGIN_RATIO             = _loaded_sl   # alias
        FIXED_LEVERAGE           = data.get("FIXED_LEVERAGE",           FIXED_LEVERAGE)
        MAX_OPEN_TRADES          = data.get("MAX_OPEN_TRADES",          MAX_OPEN_TRADES)
        _user_set_max_trades     = data.get("_user_set_max_trades",     _user_set_max_trades)
        bot_paused               = data.get("bot_paused",               bot_paused)
        MIN_SCORE_CUSTOM         = data.get("MIN_SCORE_CUSTOM",         MIN_SCORE_CUSTOM)
        MIN_SCORE_RELAXED_CUSTOM = data.get("MIN_SCORE_RELAXED_CUSTOM", MIN_SCORE_RELAXED_CUSTOM)
        BTC_CORR_FILTER_ON       = data.get("BTC_CORR_FILTER_ON",       BTC_CORR_FILTER_ON)
        _ACTIVE_MODE_FILTER      = data.get("_ACTIVE_MODE_FILTER",      _ACTIVE_MODE_FILTER)
        _DIRECTION_FILTER        = data.get("_DIRECTION_FILTER",        _DIRECTION_FILTER)
        _SUPER_SCALPER_MODE      = data.get("_SUPER_SCALPER_MODE",      _SUPER_SCALPER_MODE)
        _PRE_SUPERSCALPER_SNAPSHOT = data.get("_PRE_SUPERSCALPER_SNAPSHOT", _PRE_SUPERSCALPER_SNAPSHOT)
        TP1_PROFIT_PCT           = data.get("TP1_PROFIT_PCT",           TP1_PROFIT_PCT)
        TP1_PARTIAL              = data.get("TP1_PARTIAL",              TP1_PARTIAL)
        DAILY_LOSS_LIMIT_PCT     = data.get("DAILY_LOSS_LIMIT_PCT",     DAILY_LOSS_LIMIT_PCT)
        DAILY_WIN_LIMIT_PCT      = data.get("DAILY_WIN_LIMIT_PCT",      DAILY_WIN_LIMIT_PCT)

        # ── bot_state ──────────────────────────────────────────────────────
        if "bot_state" in data:
            loaded_bs = data["bot_state"]
            # Restore datetime field
            if isinstance(loaded_bs.get("start_time"), str):
                try:
                    loaded_bs["start_time"] = datetime.fromisoformat(loaded_bs["start_time"])
                except Exception:
                    loaded_bs["start_time"] = None
            bot_state.update(loaded_bs)

        # ── _daily_limit_state ─────────────────────────────────────────────
        if "_daily_limit_state" in data:
            _daily_limit_state.update(data["_daily_limit_state"])

        # ── _signal_hashes (ISO string → datetime) ─────────────────────────
        if "_signal_hashes" in data:
            now = datetime.now(timezone.utc)
            for k, v_str in data["_signal_hashes"].items():
                try:
                    exp = datetime.fromisoformat(v_str)
                    if exp > now:   # buang yang sudah expired
                        _signal_hashes[k] = exp
                except Exception:
                    pass

        # ── _cooldown_map (ISO string → datetime) ─────────────────────────
        if "_cooldown_map" in data:
            now = datetime.now(timezone.utc)
            for k, v_str in data["_cooldown_map"].items():
                try:
                    until = datetime.fromisoformat(v_str)
                    if until > now:   # buang yang sudah expired
                        _cooldown_map[k] = until
                except Exception:
                    pass

        # ── _pending_limit_orders — restore agar tidak hilang saat restart ────
        # Ini mencegah posisi terbuka tanpa SL/TP karena restart terjadi
        # saat limit order sudah FILLED tapi _activate_position_from_limit belum dipanggil.
        if "_pending_limit_orders" in data:
            now = datetime.now(timezone.utc)
            from datetime import timedelta
            restored_pending = 0
            for sym, p in data["_pending_limit_orders"].items():
                try:
                    # Konversi placed_at ISO string → datetime
                    placed_at_raw = p.get("placed_at")
                    placed_at = None
                    if isinstance(placed_at_raw, str):
                        placed_at = datetime.fromisoformat(placed_at_raw)
                    # Buang pending yang sudah expired (> LIMIT_ORDER_TIMEOUT_MINUTES)
                    if placed_at and (now - placed_at).total_seconds() / 60 > LIMIT_ORDER_TIMEOUT_MINUTES:
                        print(f"  ⏰ [{sym}] Pending limit expired saat restart — skip restore")
                        continue
                    p["placed_at"] = placed_at or now
                    pending_limit_orders[sym] = p
                    restored_pending += 1
                    print(f"  📥 Restore pending limit: {sym} {p.get('direction')} | Entry:{p.get('entry')} | OrderID:{p.get('order_id')}")
                except Exception as _rpe:
                    print(f"  ⚠️  Gagal restore pending [{sym}]: {_rpe}")
            if restored_pending:
                print(f"  📦 {restored_pending} pending limit orders di-restore dari disk")

        print(
            f"✅ State berhasil dimuat:\n"
            f"   BOT_MODE={BOT_MODE} | bot_paused={bot_paused} | "
            f"MAX_SL={MAX_SL_LOSS_PCT*100:.1f}% | LEV_FIXED={FIXED_LEVERAGE} | "
            f"MAX_TRADES={MAX_OPEN_TRADES}\n"
            f"   Score={MIN_SCORE_CUSTOM} | BTC_FILTER={BTC_CORR_FILTER_ON} | "
            f"MODE_FILTER={_ACTIVE_MODE_FILTER}\n"
            f"   Signal hashes aktif: {len(_signal_hashes)} | "
            f"Cooldown aktif: {len(_cooldown_map)}"
        )

    except Exception as e:
        print(f"⚠️  Gagal load state ({e}) — pakai default.")

def _auto_save_loop():
    """
    Background thread: auto-save state setiap 5 menit.
    Ini jaga-jaga agar state tersimpan walau tidak ada event command/trade.
    """
    while True:
        time.sleep(300)   # setiap 5 menit
        try:
            save_state()
        except Exception as e:
            print(f"⚠️  Auto-save error: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 1 — CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

# ── API Credentials — LIVE (Binance Mainnet) ────────────────────────────────
# Isi dengan API key dari https://www.binance.com → API Management
API_KEY_LIVE    = "V7TXYtkmziw5mN11LL0FaBVWr4LiOz6LqABfejA1hWC9HDxDWanhtUGjub0uif03"
API_SECRET_LIVE = "VDHPdLSErRx5exzgyvSHilSUcZH7vrWkEiV2rG4Hx3fxgjwBVo1bVykojIfemb2z"

# ── API Credentials — DEMO (Binance Testnet) ────────────────────────────────
# Isi dengan API key dari https://testnet.binancefuture.com
API_KEY_DEMO    = "2EXxAcETLympIvC0D4GGl1pHE62LWOHnny8Hb9xOzKgRMnETo0zSZgxi7MNKFhxV"
API_SECRET_DEMO = "ssOh069UilOtzhfitWUWY7D5uwkhL5YzEUf4NMSNt8DCIOJQEYxtNEG6CTYGBMky"

def get_api_credentials() -> tuple[str, str]:
    """Kembalikan (API_KEY, API_SECRET) yang aktif sesuai BOT_MODE."""
    if BOT_MODE == "LIVE":
        return API_KEY_LIVE, API_SECRET_LIVE
    return API_KEY_DEMO, API_SECRET_DEMO

TELEGRAM_TOKEN   = "8678661391:AAFNTftTI5a2lgl6wwcSxPrlMcsTL9szmbM"
TELEGRAM_CHAT_ID = "8688554062"

# ── Bot Mode: LIVE vs DEMO ───────────────────────────────────────────────────
# DEMO = testnet (tidak ada uang asli), LIVE = mainnet (uang asli!)
# Default: DEMO untuk keamanan. Ganti via /changeliveordemo di Telegram.
BOT_MODE = "DEMO"   # "DEMO" atau "LIVE"
BASE_URL_LIVE = "https://fapi.binance.com"
BASE_URL_DEMO = "https://testnet.binancefuture.com"

def get_base_url() -> str:
    """Kembalikan BASE_URL yang aktif sesuai BOT_MODE."""
    return BASE_URL_LIVE if BOT_MODE == "LIVE" else BASE_URL_DEMO

# ── Trade Execution ─────────────────────────────────────────────────────────
AUTO_TRADING     = True
RISK_PER_TRADE   = 0.30          # 30% balance per trade — modal $50: 30%×15x=$225 notional ✅ penuhi minNotional Binance ($100)

# ── Max SL Loss Per Trade ────────────────────────────────────────────────────
# Setiap trade, lot disesuaikan agar kerugian maksimal (jika kena SL) tidak
# melebihi MAX_SL_LOSS_PCT × total_balance.
# Rumus: max_loss_usdt = MAX_SL_LOSS_PCT × balance
#        lot = max_loss_usdt / sl_distance_per_unit
# Contoh: balance $10.000, max SL 1%, SL jarak 2% dari entry →
#         max_loss=$100, lot=$100/($200×2%)=$100/$4=25 unit
# Lot boleh berapa saja selama potensi loss jika SL kena ≤ MAX_SL_LOSS_PCT × balance.
# Ganti persentase di bawah atau via /setmarginratio (misal 1 → maks rugi 1% dari balance)
MAX_SL_LOSS_PCT  = 0.01              # 1% dari total balance sebagai max loss per trade jika SL kena
MARGIN_RATIO     = MAX_SL_LOSS_PCT   # alias backward-compat (dipakai di beberapa tempat lain)
MAX_OPEN_TRADES  = 1             # modal <$100 → max 1 posisi saja. Naik otomatis saat saldo bertambah
BALANCE_TO_USE   = 1.0           # gunakan 100% available balance
TP1_PARTIAL      = 0.5   # Porsi lot yang di-close di TP1 (0.25 = 25%, 0.5 = 50%). Ubah via /settp1partial
TRAILING_TRIGGER = 1.2
MAX_DRAWDOWN     = 0.2

# ── TP1 Fixed Profit Target ──────────────────────────────────────────────────
# Jika TP1_PROFIT_PCT > 0.0 → TP1 dihitung sebagai persentase fixed dari entry price
# (bukan dari struktur market). Berlaku untuk semua trade baru.
# Contoh: TP1_PROFIT_PCT = 1.5 → TP1 = entry × (1 + 1.5/100) untuk LONG
#                                 TP1 = entry × (1 - 1.5/100) untuk SHORT
# Set ke 0.0 untuk kembali ke mode otomatis (TP1 dari struktur swing/S&D/S/R)
# Ganti nilai di sini ATAU via command Telegram /settp1profit <persen>
TP1_PROFIT_PCT: float = 0.0   # 0.0 = otomatis (default). Contoh: 1.5 = ambil profit 1.5%

# ── Dynamic Max Trades — scale otomatis sesuai saldo ────────────────────────
# Saldo naik → max posisi otomatis bertambah, proporsional dengan kapasitas margin.
# Tier ini bisa disesuaikan sesuai risk appetite kamu.
DYNAMIC_TRADE_TIERS = [
    # (min_balance_usd, max_open_trades)
    (0,     1),    # < $100        → 1 posisi (margin cukup untuk minNotional $100)
    (100,   2),    # $100–$200     → 2 posisi
    (200,   3),    # $200–$500     → 3 posisi
    (500,   4),    # $500–$1000    → 4 posisi
    (1000,  5),    # $1000–$3000   → 5 posisi
    (3000,  8),    # $3000+        → 8 posisi
]

# Flag: True jika user sudah set /maxopentrade secara manual → MAX_OPEN_TRADES jadi hard cap
_user_set_max_trades: bool = False

def get_dynamic_max_trades() -> int:
    """
    Hitung max posisi berdasarkan saldo saat ini (dynamic scaling).

    Jika user sudah set /maxopentrade secara manual (_user_set_max_trades=True):
      → langsung return MAX_OPEN_TRADES (skip dynamic tier sepenuhnya)
    Jika tidak (default):
      → pilih tier tertinggi yang saldo-nya terpenuhi dari DYNAMIC_TRADE_TIERS
    """
    # ── User sudah set manual → pakai nilai itu langsung, abaikan dynamic tier ──
    if _user_set_max_trades:
        return MAX_OPEN_TRADES

    # ── Dynamic: naik otomatis sesuai saldo ──────────────────────────────────
    try:
        bal = get_total_balance()
    except Exception:
        return MAX_OPEN_TRADES   # fallback ke nilai default
    result = MAX_OPEN_TRADES
    for min_bal, max_trades in DYNAMIC_TRADE_TIERS:
        if bal >= min_bal:
            result = max_trades
    return result

# ── Leverage Dinamis per Tier Harga ─────────────────────────────────────────
# Prinsip: harga makin murah → volatilitas % makin tinggi → leverage lebih rendah
# Ini mencegah lot meledak untuk pair seperti PEPE/SHIB (harga nano)
LEVERAGE_TIERS = [
    # (min_price, max_price, leverage)
    # BTC dinaikkan ke 20x agar modal kecil bisa penuhi minNotional Binance ($100)
    # Contoh: balance $10, margin 6%=$0.6, 20x → notional=$12 (masih kurang, tapi lebih baik)
    # Untuk balance $50+: margin 6%=$3, 20x → notional=$60 (mendekati minNotional)
    # Gunakan /setfixedlev untuk override manual jika perlu
    (10_000,  float("inf"), 20),   # BTC range          → 20x (naik dari 10x)
    (1_000,   10_000,       20),   # ETH range          → 20x
    (100,     1_000,        20),   # BNB/SOL range      → 20x
    (10,      100,          20),   # LTC/BCH range      → 20x
    (1,       10,           20),   # LINK/ATOM range    → 20x
    (0.1,     1,            15),   # ADA/XRP range      → 15x
    (0.01,    0.1,          10),   # DOGE range         → 10x
    (0.001,   0.01,         10),   # low price range    → 10x
    (0,       0.001,        5),    # micro price (PEPE) → 5x
]

def get_leverage_for_price(price: float) -> int:
    """Pilih leverage berdasarkan harga entry."""
    for min_p, max_p, lev in LEVERAGE_TIERS:
        if min_p <= price < max_p:
            return lev
    return 5  # fallback konservatif

# ── Lot Size Safety ─────────────────────────────────────────────────────────
# Untuk modal kecil ($50-$100):
# MAX_RISK_PER_TRADE_USDT_RATIO dinaikkan agar notional bisa penuhi minNotional.
# MAX_NOTIONAL_RATIO dinaikkan agar budget lot tidak terlalu kecil.
MAX_RISK_PER_TRADE_USDT_RATIO = 0.80   # max 80% balance sebagai margin 1 trade — modal $50 butuh ini agar notional >= $100
# Naikan ratio notional agar bisa penuhi minNotional di semua pair
MAX_NOTIONAL_RATIO   = 0.80            # 80% dari kapasitas leverage (naik dari 25%)
# Minimum SL distance untuk cegah lot blow-up
MIN_SL_DISTANCE_PCT  = 0.008          # 0.8% minimum SL — naik dari 0.5%, hindari SL terlalu tipis kena noise

# ── Bot pause state ─────────────────────────────────────────────────────────
# Bot selalu mulai dalam kondisi PAUSED — kirim /start untuk mulai trading
bot_paused = True

# ── Daily Loss / Win Limit ───────────────────────────────────────────────────
# Set via /maxdailyloss dan /maxdailywin (dalam % dari total portfolio)
# 0.0 = tidak aktif (tidak ada limit)
# Contoh: 5.0 = bot auto-pause jika daily PnL mencapai -5% (loss) atau +5% (win)
DAILY_LOSS_LIMIT_PCT: float = 0.0   # % max daily loss dari total portfolio (0 = off)
DAILY_WIN_LIMIT_PCT:  float = 0.0   # % max daily win dari total portfolio (0 = off)

# State untuk tracking daily limit
# "date"          : tanggal UTC terakhir cek (format "YYYY-MM-DD")
# "balance_open"  : balance awal hari (UTC 00:00) untuk kalkulasi daily PnL
# "paused_by"     : None | "DAILY_LOSS" | "DAILY_WIN" — penyebab auto-pause limit
# "auto_started"  : apakah bot sudah auto-start hari ini setelah ganti hari
_daily_limit_state: dict = {
    "date":         None,
    "balance_open": 0.0,
    "paused_by":    None,
    "auto_started": False,
}

# ── Fixed Margin & Leverage Override ────────────────────────────────────────
# Jika FIXED_LEVERAGE > 0     → setiap open posisi pakai leverage tetap X
#                                 (override tier dinamis berdasarkan harga)
# Set ke 0 untuk kembali ke leverage auto-tier (/resetmm)
FIXED_LEVERAGE: int      = 0     # 0 = off (pakai leverage tier otomatis)

# ── Hedge Mode detection ─────────────────────────────────────────────────────
_hedge_mode: bool | None = None   # None = belum dideteksi

def is_hedge_mode() -> bool:
    """Cek apakah akun pakai Hedge Mode (dualSidePosition=true). Cache hasilnya."""
    global _hedge_mode
    if _hedge_mode is not None:
        return _hedge_mode
    try:
        data = api_get("/fapi/v1/positionSide/dual", signed=True)
        _hedge_mode = bool(data.get("dualSidePosition", False))
        print(f"  ℹ️  Position mode: {'HEDGE' if _hedge_mode else 'ONE-WAY'}")
    except Exception as e:
        print(f"  ⚠️  Gagal deteksi position mode: {e} — asumsi ONE-WAY")
        _hedge_mode = False
    return _hedge_mode

# ── Scan Modes ──────────────────────────────────────────────────────────────
_ALL_MODES = [
    # SWING dihapus — terlalu lambat, stuck di 4h
    {"label": "INTRADAY", "htf_tf": "1d",  "entry_tf": "1h",  "ref_tf": "4h",  "tier": "FULL"},
    {"label": "SCALPING", "htf_tf": "4h",  "entry_tf": "1h",  "ref_tf": None,  "tier": "FULL"},
    {"label": "LOW_TF",   "htf_tf": "1h",  "entry_tf": "15m", "ref_tf": None,  "tier": "RELAXED"},
    # ── LTF 30M: sinyal Lower Time Frame via HTF 1h → entry 30m ──────────────
    {"label": "LTF_30M",  "htf_tf": "1h",  "entry_tf": "30m", "ref_tf": None,  "tier": "RELAXED"},
]

# ── Active Modes — diubah via /scalpingonly / /intradayonly ──────────────────
# "ALL"      = semua mode aktif (default)
# "SCALPING" = hanya LOW_TF + LTF_30M  (entry 15m/30m — scalping sejati)
# "INTRADAY" = INTRADAY + SCALPING     (entry 1h/4h — swing-intraday)
_ACTIVE_MODE_FILTER: str = "ALL"   # diubah via command

# ── Direction Filter — diubah via /longonly / /shortonly / /resetdirection ──
# "ALL"   = semua arah (default)
# "LONG"  = hanya sinyal BULLISH
# "SHORT" = hanya sinyal BEARISH
_DIRECTION_FILTER: str = "ALL"   # diubah via /longonly, /shortonly, /resetdirection

# ── Super Scalper Mode — diaktifkan via /superscalpermode ───────────────────
# Ketika ON:
#   • Hanya pair HIGH-LIQUIDITY (Tier 1 + beberapa Tier 2 dengan volume terbesar)
#   • Hanya mode LOW_TF (15m) + LTF_30M (30m) entry
#   • Score threshold diturunkan ke 35 agar sinyal lebih banyak
#   • Cooldown setelah SL dipersingkat ke 1 jam
#   • BTC correlation filter di-OFF agar tidak block sinyal momentum
#   • SCAN_INTERVAL dikurangi ke 20 detik (lebih cepat respons)
#   • TP1 fixed 0.8%, TP2 1.5% (scalp profit kecil tapi sering)
#   • RR minimum diturunkan ke 1.2 (scalper lebih sering keluar cepat)
_SUPER_SCALPER_MODE: bool = False

# Pair list khusus super scalper — HANYA coin dengan:
#   • Open Interest Binance Futures > $200M
#   • 24h Volume Futures > $500M rata-rata
#   • Spread tight bahkan di jam sepi (Asia session)
#   • Cukup volatil di 15m/30m untuk menghasilkan sinyal bersih
#
# Sumber referensi: Binance Futures top volume + OI ranking (Apr 2025)
# DIBUANG dari versi lama: HYPE (OI kecil), RUNE (spread lebar),
#   NEAR/OP/ARB/APT (volume drop di Asia session), TON (kurang liquid futures)
_SUPER_SCALPER_PAIRS = [
    # ── Tier S: Absolute Top — volume & OI terbesar, spread paling tight ────
    "BTCUSDT",   "ETHUSDT",   "SOLUSDT",   "XRPUSDT",   "BNBUSDT",
    # ── Tier A: High OI + Volume — konsisten 24 jam ──────────────────────────
    "DOGEUSDT",  "ADAUSDT",   "AVAXUSDT",  "LINKUSDT",   "LTCUSDT",
    "DOTUSDT",   "UNIUSDT",   "AAVEUSDT",  "SUIUSDT",    "1000PEPEUSDT",
    # ── Tier B: Volume cukup, volatilitas baik untuk 15m/30m ─────────────────
    "TRXUSDT",   "XLMUSDT",   "INJUSDT",   "ENAUSDT",    "1000SHIBUSDT",
]

# Snapshot settings sebelum super scalper aktif (untuk restore)
_PRE_SUPERSCALPER_SNAPSHOT: dict = {}

def get_active_modes() -> list:
    """Return daftar mode yang aktif berdasarkan _ACTIVE_MODE_FILTER atau Super Scalper."""
    # Super Scalper: hanya LOW_TF + LTF_30M (15m/30m) — paksa override
    if _SUPER_SCALPER_MODE:
        return [m for m in _ALL_MODES if m["label"] in ("LOW_TF", "LTF_30M")]
    if _ACTIVE_MODE_FILTER == "SCALPING":
        # Scalping sejati: entry TF rendah (15m / 30m)
        return [m for m in _ALL_MODES if m["label"] in ("LOW_TF", "LTF_30M")]
    if _ACTIVE_MODE_FILTER == "INTRADAY":
        # Intraday: termasuk SCALPING (4h→1h) karena masuk kategori intraday
        return [m for m in _ALL_MODES if m["label"] in ("INTRADAY", "SCALPING")]
    return list(_ALL_MODES)


def get_active_pairs() -> list:
    """Return pair list aktif — dipersempit saat Super Scalper Mode ON."""
    if _SUPER_SCALPER_MODE:
        return list(_SUPER_SCALPER_PAIRS)
    return list(PAIR_LIST)

MODES = _ALL_MODES   # alias — dipakai di seluruh kode (tidak perlu ubah referensi lain)

# ── SL Distance Filter ───────────────────────────────────────────────────────
# Sinyal dengan SL > MAX_SL_DISTANCE_PCT dari entry = SKIP (tidak cocok futures).
# SL > 3% → kategori Intraday/Swing → terlalu besar untuk futures leverage.
# Set ke 0 untuk non-aktif (tidak ada filter SL distance).
# Filter per mode:
#   LOW_TF / LTF_30M  → max 1.5% (entry TF 15m/30m, SL harus ketat)
#   SCALPING           → max 3.0% (entry TF 1h)
#   INTRADAY           → max 3.0% (global default)
MAX_SL_DISTANCE_PCT: float = 0.03   # 3% max SL global (INTRADAY & SCALPING)

# ── Per-Mode SL & TP Cap ─────────────────────────────────────────────────────
# SL cap sudah ada di filter akhir, tapi calculate_rr juga perlu tahu mode-nya
# agar SL yang dihasilkan langsung proporsional (bukan overshooting dulu lalu dipotong).
# TP cap memastikan scalping tidak dapat TP belasan % yang mustahil tercapai dalam TF kecil.
SL_TP_CAPS: dict = {
    # label      : (max_sl_pct, max_tp_pct)
    # LOW_TF/LTF_30M: SL diperketat ke 1.2% — scalping 15m/30m butuh SL sempit.
    # SL 3% di leverage 15x = -45% modal per trade → terlalu besar untuk scalper.
    # TP 2.4% (1.2% SL × RR 2.0) masih realistis dalam 2-4 candle 15m.
    "LOW_TF"  : (0.012, 0.030),   # entry 15m → SL max 1.2%, TP max 3.0%
    "LTF_30M" : (0.015, 0.035),   # entry 30m → SL max 1.5%, TP max 3.5%
    "SCALPING": (0.040, 0.080),   # entry 1h  → SL max 4%, TP max 8%
    "INTRADAY": (0.050, 0.120),   # entry 1h  → SL max 5%, TP max 12%
}

SCAN_INTERVAL = 30           # seconds between full scans — 15 terlalu cepat, bisa kena 418

# ── Pair List ───────────────────────────────────────────────────────────────
PAIR_LIST = [
    # ── Tier 1: Top Market Cap ───────────────────────────────────────────────
    "BTCUSDT",  "ETHUSDT",   "XRPUSDT",  "SOLUSDT",   "BNBUSDT",
    "DOGEUSDT", "ADAUSDT",   "TRXUSDT",  "AVAXUSDT",  "LINKUSDT",
    # ── Tier 2: Large Cap ────────────────────────────────────────────────────
    "DOTUSDT",  "LTCUSDT",   "BCHUSDT",  "NEARUSDT",  "UNIUSDT",
    "ETCUSDT",  "ARBUSDT",   "ATOMUSDT", "AAVEUSDT",  "XLMUSDT",
    "HBARUSDT", "ENAUSDT",   "SUIUSDT",  "APTUSDT",   "OPUSDT",
    "INJUSDT",  "FILUSDT",   "ICPUSDT",  "STXUSDT",   "VETUSDT",
    # ── Tier 3: Mid Cap / High Volume ────────────────────────────────────────
    "RENDERUSDT","GRTUSDT",  "SANDUSDT", "MANAUSDT",  "THETAUSDT",
    "ALGOUSDT",  "AXSUSDT",  "FLOWUSDT", "CHZUSDT",   "GALAUSDT",
    "KAVAUSDT",  "ZILUSDT",  "QNTUSDT",  "IMXUSDT",   "1000PEPEUSDT",
    # ── Tier 4: DeFi & Ecosystem ─────────────────────────────────────────────
    "1000SHIBUSDT",  "TONUSDT",  "EGLDUSDT", "HYPEUSDT",  "RUNEUSDT",
    "LDOUSDT",   "SNXUSDT",  "CRVUSDT",   "COMPUSDT",
]




# ── Price Action + Supply & Demand + S/R Detection Parameters ───────────────
SWING_WINDOW          = 5        # candle kiri/kanan untuk swing high/low
SR_LOOKBACK           = 80       # candle lookback untuk deteksi S/R level
SR_TOUCH_MIN          = 2        # minimum touch agar level valid
SR_PROXIMITY_PCT      = 0.003    # 0.3% proximity — harga dianggap "di zona S/R"
ZONE_LOOKBACK         = 60       # candle lookback untuk Supply/Demand zone
ZONE_MITIGATED_LIMIT  = 2        # max berapa kali zona boleh disentuh sebelum invalid
ZONE_PROXIMITY_PCT    = 0.005    # 0.5% toleransi price di dalam zona
MIN_ZONE_STRENGTH_PCT = 0.20     # impulse minimal 0.20% untuk zone valid
WICK_BODY_RATIO_MIN   = 1.5      # pin bar: wick harus >= 1.5x body
ATR_PERIOD            = 14
PA_LOOKBACK           = 5        # candle terakhir untuk cek Price Action pattern

# ── Scoring Weights (PA + S&D + S/R Flow) ────────────────────────────────────
# HTF Bias
SCORE_HTF_ALIGNED     = 15       # HTF trend sejalan dengan arah trade (disamakan signal bot)
SCORE_HTF_PENALTY     = -10      # HTF berlawanan arah

# Price Action Pattern (pin bar, engulfing, inside bar breakout)
SCORE_PA_STRONG       = 25       # engulfing (sinyal reversal/continuation kuat)
SCORE_PA_MEDIUM       = 15       # pin bar (rejection dari level)
SCORE_PA_WEAK         = 8        # inside bar breakout (compression → expansi)
SCORE_PA_NONE         = -5       # tidak ada PA pattern

# Supply & Demand Zone
SCORE_SD_FRESH        = 20       # zona fresh (belum pernah disentuh / baru 1x)
SCORE_SD_TESTED       = 10       # zona sudah pernah disentuh 1x
SCORE_SD_NONE         = 0        # tidak ada zona S&D valid di sekitar harga

# Support & Resistance Level
SCORE_SR_STRONG       = 15       # harga di dekat S/R dengan ≥3 touches
SCORE_SR_MEDIUM       = 8        # harga di dekat S/R dengan 2 touches
SCORE_SR_NONE         = 0        # tidak ada S/R dekat

# Volume confirmation
SCORE_VOLUME          = 5        # base (1.5x avg)
# Session
SCORE_SESSION         = 5
# Macro
SCORE_MACRO_ALIGNED   = 8
SCORE_MACRO_CONFLICT  = -8

SIGNAL_MODE           = "NORMAL"
MIN_SCORE_FULL_NORMAL = 55   # disamakan dengan signal bot (21)
MIN_SCORE_FULL_SNIPER = 75
MIN_SCORE_RELAXED     = 40   # disamakan dengan signal bot (21)
MIN_SCORE = MIN_SCORE_FULL_NORMAL if SIGNAL_MODE == "NORMAL" else MIN_SCORE_FULL_SNIPER

# ── Custom Score Override via /setscoreupto ──────────────────────────────────
# Jika > 0, nilai ini OVERRIDE MIN_SCORE (FULL tier) DAN MIN_SCORE_RELAXED (RELAXED tier).
# PENTING: RELAXED tier (LOW_TF/LTF_30M) juga kena gate yang SAMA — tidak ada diskon 60%.
# Set ke 0 untuk kembali ke default scoring threshold.
# Contoh: /setscoreupto 60  → semua tier (FULL & RELAXED) wajib score ≥ 60
MIN_SCORE_CUSTOM: int          = 55  # disamakan dengan signal bot (21)
MIN_SCORE_RELAXED_CUSTOM: int  = 40  # disamakan dengan signal bot (21)

RR_MIN               = 1.5   # minimum RR — swing struktural terdekat yang realistis
RR_GRADE_A           = 2.5   # TP2 target (swing lebih jauh)
RR_GRADE_B           = 1.5   # Grade B = threshold minimum (sama dengan RR_MIN)

# RR per mode — scalping TF kecil pakai threshold lebih longgar
RR_MIN_PER_MODE: dict = {
    "LOW_TF"  : 1.5,   # 15m entry
    "LTF_30M" : 1.5,   # 30m entry
    "SCALPING": 1.5,   # 1h entry
    "INTRADAY": 1.5,   # 1h HTF entry
}
RR_GRADE_A_PER_MODE: dict = {
    "LOW_TF"  : 2.0,
    "LTF_30M" : 2.0,
    "SCALPING": 2.5,
    "INTRADAY": 2.5,
}

def get_rr_min_for_mode(mode_label: str) -> float:
    """
    Return RR minimum untuk mode tertentu.
    Saat Super Scalper Mode ON:
      → RR 1.8 untuk LOW_TF/LTF_30M.
      Alasan: SL diperketat ke 1.2-1.5%, sehingga TP target 2.2-2.7% masih sangat
      realistis dalam 2-4 candle 15m/30m di coin high-liquidity.
      RR 1.2 terlalu longgar (TP cuma 1.44% dengan SL 1.2% = hampir tidak worth).
    """
    if _SUPER_SCALPER_MODE:
        return 1.8
    return RR_MIN_PER_MODE.get(mode_label, RR_MIN)

RELAXED_VOL_RATIO_MIN = 1.8

# ── Macro ───────────────────────────────────────────────────────────────────
BTC_BIAS_TF        = "1d"   # BTC bias tetap dihitung, sebagai info saja

# ── BTC + BTC.D Correlation Filter (/togglebtcfilter) ───────────────────────
#
# Filter ini menggabungkan arah BTC price dan BTC Dominance secara bersamaan
# untuk menentukan sinyal apa yang boleh lolos (altcoin vs BTCUSDT).
#
# Tabel kombinasi (altcoin):
# ┌─────────────┬──────────────┬──────────────────────────────────────────┐
# │  BTC price  │   BTC.D      │  Signal yang diizinkan (altcoin)         │
# ├─────────────┼──────────────┼──────────────────────────────────────────┤
# │  BULLISH    │  BEARISH     │  LONG altcoin  ✅  (altcoin season)      │
# │  BULLISH    │  BULLISH     │  Ambiguous — SKIP (BTC rally, dom naik)  │
# │  BULLISH    │  RANGING     │  LONG altcoin  ✅  (alt ikut BTC naik)   │
# │  BEARISH    │  BULLISH     │  SHORT altcoin ✅  (BTC turun dom naik)  │
# │  BEARISH    │  BEARISH     │  Ambiguous — SKIP (BTC turun dom turun)  │
# │  BEARISH    │  RANGING     │  SHORT altcoin ✅  (alt ikut BTC turun)  │
# │  RANGING    │  BEARISH     │  LONG altcoin  ✅  (BTC sideways, dom ↓) │
# │  RANGING    │  BULLISH     │  SHORT altcoin ✅  (BTC sideways, dom ↑) │
# │  RANGING    │  RANGING     │  SKIP (tidak ada arah jelas)             │
# └─────────────┴──────────────┴──────────────────────────────────────────┘
#
# Untuk BTCUSDT sendiri: hanya pakai arah BTC price (dom tidak relevan).
#
# Default: OFF agar tidak mengubah perilaku bot yang sudah ada.
BTC_CORR_FILTER_ON: bool = True    # toggle via /togglebtcfilter

# BTC.D timeframe untuk deteksi trend dominance (BTCDOMUSDT, pakai LTF)
BTCD_TF: str = "15m"

# ── Cooldown ────────────────────────────────────────────────────────────────
COOLDOWN_MINUTES = 10
COOLDOWN_AFTER_CLOSE = 0    # 0 = tidak ada cooldown setelah posisi close (TP)
COOLDOWN_AFTER_SL_HOURS = 3  # FIX: dikurangi dari 8 → 3 jam, hindari miss reversal setelah SL
_cooldown_map: dict = {}


# ── Cooldown Functions ───────────────────────────────────────────────────────

def is_on_cooldown(pair: str, direction: str) -> tuple:
    """
    Cek apakah pair+direction sedang dalam cooldown sinyal.
    Return: (on_cooldown: bool, remaining_minutes: float, reason: str)
    
    Cooldown per arah (BULLISH/BEARISH) — arah berlawanan tetap boleh masuk.
    """
    from datetime import timedelta
    now = datetime.now(timezone.utc)

    # Cek cooldown spesifik arah (signal cooldown)
    key_dir = f"{pair}|{direction}"
    until_dir = _cooldown_map.get(key_dir)
    if until_dir and now < until_dir:
        remaining = (until_dir - now).total_seconds() / 60
        dir_str = "LONG" if direction == "BULLISH" else "SHORT"
        return True, remaining, f"Cooldown {dir_str} — sisa {remaining:.0f} menit"

    # Cek cooldown post-close (blok semua arah)
    key_close = f"{pair}|CLOSE"
    until_close = _cooldown_map.get(key_close)
    if until_close and now < until_close:
        remaining = (until_close - now).total_seconds() / 60
        return True, remaining, f"Cooldown post-close — sisa {remaining:.0f} menit"

    return False, 0.0, ""


def set_cooldown(pair: str, label: str, entry_tf: str = ""):
    """
    Set cooldown sinyal setelah trade dieksekusi.
    Menggunakan SIGNAL_COOLDOWN_HOURS untuk durasi.
    """
    from datetime import timedelta
    now   = datetime.now(timezone.utc)
    until = now + timedelta(hours=SIGNAL_COOLDOWN_HOURS)

    # Cooldown per pair + arah tidak diketahui di sini,
    # jadi set cooldown generic per pair|label sebagai fallback
    key = f"{pair}|{label}"
    _cooldown_map[key] = until
    print(f"  ⏳ Cooldown set: {pair} [{label}] sampai {until.strftime('%H:%M')} UTC")


def set_cooldown_post_close(pair: str, reason: str = "", is_stoploss: bool = False):
    """
    Set cooldown setelah posisi close.
    - is_stoploss=True  → cooldown 8 jam (COOLDOWN_AFTER_SL_HOURS) — blok semua arah
    - is_stoploss=False → cooldown COOLDOWN_AFTER_CLOSE menit (default 0 = tidak ada)
    Cooldown hanya aktif untuk STOPLOSS, bukan Take Profit.
    """
    from datetime import timedelta
    now = datetime.now(timezone.utc)

    if is_stoploss:
        # Cooldown 4 jam setelah kena SL — blok semua sinyal pair ini
        until = now + timedelta(hours=COOLDOWN_AFTER_SL_HOURS)
        key   = f"{pair}|CLOSE"
        _cooldown_map[key] = until
        reason_str = f" ({reason})" if reason else ""
        print(f"  ⏳ SL Cooldown: {pair}{reason_str} — sampai {until.strftime('%H:%M')} UTC ({COOLDOWN_AFTER_SL_HOURS} jam)")
    else:
        # TP atau close lainnya: cooldown COOLDOWN_AFTER_CLOSE menit (default 0)
        if COOLDOWN_AFTER_CLOSE <= 0:
            # Tidak ada cooldown untuk TP — langsung hapus key jika ada
            key = f"{pair}|CLOSE"
            _cooldown_map.pop(key, None)
            reason_str = f" ({reason})" if reason else ""
            print(f"  ✅ No cooldown after TP: {pair}{reason_str} — langsung bisa re-entry")
            return
        until = now + timedelta(minutes=COOLDOWN_AFTER_CLOSE)
        key   = f"{pair}|CLOSE"
        _cooldown_map[key] = until
        reason_str = f" ({reason})" if reason else ""
        print(f"  ⏳ Post-close cooldown: {pair}{reason_str} — sampai {until.strftime('%H:%M')} UTC ({COOLDOWN_AFTER_CLOSE} menit)")


# ── fetch_ohlcv: alias ke fetch_ohlcv_realdata (untuk kompatibilitas kode lama) ──
# Beberapa bagian kode masih memanggil fetch_ohlcv(pair, tf, limit=N)
# dengan format pair "BTC/USDT" (ccxt style) — perlu dikonversi ke "BTCUSDT"
def fetch_ohlcv(pair: str, tf: str, limit: int = 200) -> pd.DataFrame | None:
    """
    Wrapper kompatibilitas — konversi format pair ccxt ('BTC/USDT') 
    ke format Binance ('BTCUSDT') lalu panggil fetch_ohlcv_realdata.
    """
    # Konversi ccxt style → Binance style jika ada slash
    symbol = pair.replace("/", "") if "/" in pair else pair
    return fetch_ohlcv_realdata(symbol, tf, limit=limit)



# ── SMC Ranging Adaptive Config (dari main(11) Section 20) ──────────────────
LTF_RANGING_SCORE_GATE      = 40     # Minimum composite LTF score saat HTF ranging
LTF_CONSECUTIVE_MIN         = 3      # Jumlah candle berturut-turut arah sama
DISPLACEMENT_BODY_RATIO_MIN = 0.60   # Body / range candle displacement
VOLUME_SPIKE_MULTIPLIER     = 1.5    # Volume spike = N× avg 20 bar
RANGE_ATR_FACTOR            = 3.0    # Range HTF minimal N× ATR agar layak trade
RANGE_EDGE_BUFFER_PCT       = 0.15   # Buffer 15% dari tepi range
STRONG_BOS_BODY_PCT         = 0.40   # Body candle BOS >= 40% = strong
SESSION_BONUS_MAP           = {"London": 15, "New York": 15, "Asia": 5, "Off-Hours": -10}

# ── SMC Macro Config ────────────────────────────────────────────────────────
ALTCOIN_EXEMPTIONS    = {"BTC/USDT", "ETH/USDT"}
BTCD_SMA_PERIOD       = 20
ATR_MIN_PCT           = 0.15
CONFIRM_BODY_RATIO    = 0.55
MAX_POSITION_HOURS    = 72
SIGNAL_HASH_TTL_HOURS = 24
SIGNAL_COOLDOWN_HOURS = 4   # FIX: dikurangi dari 12 → 4 jam agar tidak miss re-entry valid

# ── SMC Correlation Groups ───────────────────────────────────────────────────
CORRELATION_GROUPS = [
    {"DOGE/USDT", "1000SHIB/USDT", "1000PEPE/USDT", "GALA/USDT", "SAND/USDT", "MANA/USDT", "AXS/USDT", "CHZ/USDT"},
    {"ETH/USDT",  "ARB/USDT",  "OP/USDT",   "IMX/USDT"},
    {"SOL/USDT",  "APT/USDT",  "SUI/USDT",  "NEAR/USDT"},
    {"AVAX/USDT", "ATOM/USDT", "DOT/USDT",  "EGLD/USDT"},
    {"LINK/USDT", "GRT/USDT",  "RENDER/USDT"},
    {"BNB/USDT",  "INJ/USDT"},
]

# ── SMC Strategy Detection Parameters (dari main(11)) ───────────────────────
SWEEP_LOOKBACK       = 30
OB_LOOKBACK          = 40
FVG_LOOKBACK         = 40
EQH_EQL_TOLERANCE    = 0.0015
REJECTION_TOLERANCE  = 0.003
OB_MITIGATION_LIMIT  = 2
MIN_DISPLACEMENT_PCT = 0.25
MIN_FVG_PCT          = 0.08
RSI_PERIOD           = 14
RSI_OVERBOUGHT       = 70   # LONG diblok jika RSI ≥ nilai ini (overbought)
RSI_OVERSOLD         = 30   # SHORT diblok jika RSI ≤ nilai ini (oversold)
EMA_PERIODS          = [20, 50, 200]

# ── IMB (Body Imbalance) & Gap Config ─────────────────────────────────────────
IMB_LOOKBACK         = 40    # Jumlah candle lookback untuk deteksi IMB
IMB_MIN_BODY_PCT     = 0.40  # Body candle impulsif minimal 40% dari range-nya
IMB_MIN_SIZE_PCT     = 0.06  # Ukuran IMB minimal 0.06% dari harga (cegah noise)
IMB_FILL_LIMIT       = 0.75  # IMB dianggap terisi jika >75% filled → skip
GAP_LOOKBACK         = 50    # Lookback untuk deteksi gap harga
GAP_MIN_SIZE_PCT     = 0.10  # Gap minimal 0.10% dari harga

# ── SMC Scoring Weights ──────────────────────────────────────────────────────
# Tier 2 — BOS / Market Structure
SCORE_BOS_STRONG     = 25
SCORE_BOS_WEAK       = 12
SCORE_BOS_PENALTY    = -10

# Tier 3 — Momentum
SCORE_MOMENTUM       = 15
SCORE_MOMENTUM_NONE  = 0
SCORE_MOMENTUM_ANTI  = -5

# Tier 4 — Liquidity sweep/reaksi
SCORE_LIQUIDITY      = 15
SCORE_LIQUIDITY_WEAK = 8
SCORE_LIQUIDITY_NONE = -5

# Tier 5 — Entry Zone OB/FVG
SCORE_OB_AND_FVG     = 15
SCORE_OB_OR_FVG      = 10
SCORE_ZONE_NONE      = 0

# ── Zone Confluence Scoring ───────────────────────────────────────────────────
# Zona dianggap "kuat" jika ada beberapa faktor tumpuk di titik yang sama.
# Semakin banyak confluence → semakin besar kemungkinan mantul.
# Skor ini DITAMBAHKAN ke score total di atas entry_zone score.
# Maks teoritis: ~+40 pts (semua faktor terpenuhi sekaligus)
#
#   OB fresh (taps=0)          → +10  order di zona masih utuh, belum terisi
#   OB tested 1x               → +5   sudah teruji, masih relevan
#   FVG overlap OB             → +8   double imbalance — dua alasan harga balik
#   S/R tumpuk di zona         → +7   level historis ikut jadi magnet
#   Impulse kuat (>1%)         → +5   semakin cepat pasar tinggalkan = lebih banyak order tertinggal
#   Zona di Discount/Premium   → +5   beli di bawah eq (LONG) atau jual di atas eq (SHORT)
#   Super-fresh bonus          → +5   OB fresh + impulse tinggi sekaligus
#   Zona lemah (taps>2)        → -8   kemungkinan order di zona sudah habis
SCORE_ZONE_CONFLUENCE_OB_FRESH    = 10
SCORE_ZONE_CONFLUENCE_OB_TESTED   = 5
SCORE_ZONE_CONFLUENCE_FVG_OVERLAP = 8
SCORE_ZONE_CONFLUENCE_SR_OVERLAP  = 7
SCORE_ZONE_CONFLUENCE_IMPULSE     = 5
SCORE_ZONE_CONFLUENCE_PD_ARRAY    = 5
SCORE_ZONE_CONFLUENCE_SUPER_FRESH = 5
SCORE_ZONE_WEAK_PENALTY           = -8

# Supporting bonuses
SCORE_DISPLACEMENT   = 10
SCORE_RSI_IDEAL      = 5
SCORE_RSI_PENALTY    = -5
SCORE_EMA_ALIGNED    = 8
SCORE_EMA_BONUS      = 4
SCORE_REF_TF         = 5

# ── SMC Scan Config ──────────────────────────────────────────────────────────
MAX_WORKERS          = 3   # thread workers untuk scan paralel — 6 terlalu banyak, bisa kena 418



# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 2 — DATA SOURCE: Binance Futures (direct)
# ═══════════════════════════════════════════════════════════════════════════

_active_data_source = "Binance"

# Konversi interval Binance → string yang diterima endpoint klines
_INTERVAL_MAP = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "8h": "8h", "12h": "12h",
    "1d": "1d", "3d": "3d", "1w": "1w", "1M": "1M",
}

# OPT: Global rate limiter — batasi max request Binance yang berjalan BERSAMAAN
# Tanpa ini, 3 thread × 5 TF bisa kirim 15 request hampir serentak → 418
# Semaphore(4): max 4 request berjalan paralel di seluruh thread
_BINANCE_SEMAPHORE = threading.Semaphore(4)


def fetch_ohlcv_realdata(symbol_binance: str, interval: str, limit: int = 200) -> pd.DataFrame | None:
    """Ambil OHLCV langsung dari Binance Futures REST API (tanpa ccxt).
    Auto-retry saat kena 418 (IP ban sementara) atau 429 (rate limit).
    """
    tf = _INTERVAL_MAP.get(interval, interval)
    url = f"{get_base_url()}/fapi/v1/klines"
    params = {"symbol": symbol_binance, "interval": tf, "limit": min(limit, 1500)}
    wait_times = [5, 15, 30, 60]
    retries = 3
    for attempt in range(retries + 1):
        try:
            with _BINANCE_SEMAPHORE:   # OPT: max 4 request paralel ke Binance
                r = requests.get(url, params=params, headers=get_headers(), timeout=15)

            if r.status_code == 418:
                wait = wait_times[min(attempt, len(wait_times) - 1)]
                print(f"  ⚠️  Binance 418 IP ban sementara {symbol_binance} @ {interval} — tunggu {wait}s (attempt {attempt+1}/{retries+1})")
                if attempt < retries:
                    time.sleep(wait)
                    continue
                return None   # habis retry → skip pair ini, jangan crash

            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", wait_times[min(attempt, len(wait_times) - 1)]))
                print(f"  ⚠️  Binance 429 rate limit {symbol_binance} @ {interval} — tunggu {retry_after}s (attempt {attempt+1}/{retries+1})")
                if attempt < retries:
                    time.sleep(retry_after)
                    continue
                return None

            r.raise_for_status()
            candles = r.json()
            if not candles or len(candles) < 50:
                print(f"  ⚠️  Binance hanya {len(candles) if candles else 0} candles untuk {symbol_binance} @ {interval}")
                return None
            df = pd.DataFrame(candles, columns=[
                "timestamp", "open", "high", "low", "close", "volume",
                "close_time", "qav", "num_trades", "taker_buy_base", "taker_buy_quote", "ignore"
            ])
            df = df[["timestamp", "open", "high", "low", "close", "volume"]].astype(float)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            return df
        except requests.HTTPError as e:
            print(f"  ⚠️  Binance HTTP error {symbol_binance} @ {interval}: {e}")
            return None
        except requests.exceptions.Timeout:
            if attempt < retries:
                print(f"  ⚠️  Timeout {symbol_binance} @ {interval} — retry {attempt+1}/{retries}")
                time.sleep(wait_times[min(attempt, len(wait_times) - 1)])
                continue
            print(f"  ❌ Timeout habis {symbol_binance} @ {interval}")
            return None
        except Exception as e:
            print(f"  ❌ Binance fetch gagal {symbol_binance} @ {interval}: {e}")
            return None
    return None


bot_state = {
    "balance_start": 0,
    "wins": 0,
    "losses": 0,
    "win_streak": 0,
    "lose_streak": 0,
    "signals_today": 0,
    "last_summary_date": None,
    "start_time": None,
    # ── PNL tracking harian & bulanan ──────────────────────────────────────
    "balance_day_start": 0,
    "balance_day_date": None,
    "balance_month_start": 0,
    "balance_month_key": None,
    # ── Win/Loss harian & bulanan ──────────────────────────────────────────
    "day_wins": 0,
    "day_losses": 0,
    "month_wins": 0,
    "month_losses": 0,
    # ── PNL kumulatif terpisah per mode (LIVE / DEMO) ──────────────────────
    "mode_pnl": {
        "LIVE": {"realized": 0.0, "start_bal": 0.0, "wins": 0, "losses": 0},
        "DEMO": {"realized": 0.0, "start_bal": 0.0, "wins": 0, "losses": 0},
    },
}

active_positions = {}

# ── Pending Limit Orders ─────────────────────────────────────────────────────
# Menyimpan limit order yang sudah dikirim tapi belum terisi (belum jadi posisi).
# Format: {symbol: {"order_id": int, "direction": str, "entry": float, ...}}
pending_limit_orders: dict = {}

# Timeout limit order: batalkan jika tidak terisi dalam N menit
LIMIT_ORDER_TIMEOUT_MINUTES: int = 90

DAILY_SUMMARY_HOUR_UTC = 0


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 3 — TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

# Offset polling Telegram (untuk command listener)
_tg_offset = 0


def send_telegram_raw(msg):
    """
    Kirim pesan ke Telegram dengan error handling lengkap:
    - Cek HTTP response (tidak cuma network exception)
    - Retry otomatis jika rate-limited (429)
    - Fallback ke plain text jika HTML parse_mode gagal (misal karakter < > & di pesan)
    - Log detail error ke console agar tidak hilang diam-diam
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    def _send(text, parse_mode="HTML"):
        try:
            r = requests.post(url, data={
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       text,
                "parse_mode": parse_mode,
            }, timeout=10)
            return r
        except Exception as e:
            print(f"⚠️ Telegram network error: {e}")
            return None

    # ── Attempt 1: kirim dengan HTML parse_mode ──────────────────────────────
    r = _send(msg, parse_mode="HTML")

    if r is None:
        return   # network error, sudah di-log

    # ── Sukses → selesai, tidak perlu lanjut ─────────────────────────────────
    if r.ok:
        return

    # ── Rate limit: tunggu dan retry sekali ──────────────────────────────────
    if r.status_code == 429:
        try:
            retry_after = r.json().get("parameters", {}).get("retry_after", 5)
        except Exception:
            retry_after = 5
        print(f"⚠️ Telegram rate limit — tunggu {retry_after}s lalu retry")
        time.sleep(retry_after)
        r = _send(msg, parse_mode="HTML")
        if r is None or r.ok:
            return   # berhasil atau network error — selesai

    # ── HTML parse error: fallback ke plain text ─────────────────────────────
    if not r.ok:
        try:
            err_body = r.json()
            err_desc = err_body.get("description", "")
        except Exception:
            err_desc = r.text[:200]

        if "can't parse entities" in err_desc.lower() or r.status_code == 400:
            # Coba kirim ulang tanpa parse_mode (plain text)
            print(f"⚠️ Telegram HTML parse error — fallback ke plain text. Desc: {err_desc}")
            import re
            plain_msg = re.sub(r"<[^>]+>", "", msg)   # strip semua HTML tag
            r2 = _send(plain_msg, parse_mode="")
            if r2 is None or not r2.ok:
                print(f"⚠️ Telegram plain text fallback juga gagal: {r2.status_code if r2 else 'N/A'}")
        else:
            print(f"⚠️ Telegram error HTTP {r.status_code}: {err_desc}")


def fmt_price(price: float) -> str:
    """Format harga dengan presisi otomatis sesuai magnitude."""
    if price == 0:
        return "0"
    if price < 0.0001:  return f"{price:.8f}"
    if price < 0.001:   return f"{price:.7f}"
    if price < 0.01:    return f"{price:.6f}"
    if price < 0.1:     return f"{price:.5f}"
    if price < 1:       return f"{price:.4f}"
    if price < 10:      return f"{price:.4f}"
    if price < 1000:    return f"{price:.3f}"
    return f"{price:.2f}"


def send_telegram_signal(signal: dict, mode: dict, score_bd: dict, session: str,
                         pa_name: str, btc_bias: str, btcd_trend: str, macro_reason: str):
    pair     = signal["pair"]
    dir_str  = signal["direction"]
    dir_em   = "🟢" if dir_str == "LONG" else "🔴"
    grade    = signal.get("grade", "B")
    grade_em = "🏆" if grade == "A" else "🥈"
    tps      = signal["take_profit"]

    bar_filled = min(10, int(signal["score"] / 10))
    bar = "█" * bar_filled + "░" * (10 - bar_filled)

    try:
        breakdown_lines = "\n".join([
            f"  HTF Aligned    : {'+' if score_bd.get('htf_aligned',0)>=0 else ''}{score_bd.get('htf_aligned', 0)} pts",
            f"  Price Action   : +{score_bd.get('price_action', 0)} pts  ({pa_name})",
            f"  Supply/Demand  : +{score_bd.get('supply_demand', 0)} pts",
            f"  Support/Resist : +{score_bd.get('support_resistance', 0)} pts",
            f"  Volume         : +{score_bd.get('volume', 0)} pts",
            f"  Session        : +{score_bd.get('session', 0)} pts",
            f"  Macro          : {'+' if score_bd.get('macro', 0) >= 0 else ''}{score_bd.get('macro', 0)} pts",
        ])
    except Exception as bd_err:
        print(f"⚠️ send_telegram_signal: gagal build breakdown: {bd_err}")
        breakdown_lines = "  (detail tidak tersedia)"

    try:
        reasons_str = "\n".join([f"  • {r}" for r in signal.get("reason", [])])
    except Exception:
        reasons_str = "  (reasons tidak tersedia)"

    try:
        _entry_val = signal['entry']
        _sl_val    = signal['stop_loss']
        _tp1_val   = tps[0]
        _tp2_val   = tps[1]
        _sl_pct    = abs(_entry_val - _sl_val)  / max(_entry_val, 1e-9) * 100
        _tp1_pct   = abs(_tp1_val  - _entry_val) / max(_entry_val, 1e-9) * 100
        _tp2_pct   = abs(_tp2_val  - _entry_val) / max(_entry_val, 1e-9) * 100

        msg = (
            f"{dir_em} <b>{pair} — {dir_str}</b>  {grade_em} Grade {grade}\n"
            f"{'─'*38}\n"
            f"📊 Mode     : {mode['label']} ({mode['htf_tf']} → {mode['entry_tf']})\n"
            f"🕐 Session  : {session}\n"
            f"{'─'*38}\n"
            f"💰 Entry    : <b>${fmt_price(_entry_val)}</b>\n"
            f"🛑 Stop Loss: ${fmt_price(_sl_val)}  <b>(-{_sl_pct:.2f}% dari entry)</b>\n"
            f"🎯 TP1      : ${fmt_price(_tp1_val)}  (+{_tp1_pct:.2f}%)  (1:{RR_GRADE_B})\n"
            f"🎯 TP2      : ${fmt_price(_tp2_val)}  (+{_tp2_pct:.2f}%)  (1:{RR_GRADE_A+0.5:.1f})\n"
            f"📐 RR       : 1:{signal['RR']}\n"
            f"{'─'*38}\n"
            f"🧮 Score    : <b>{signal['score']} pts</b>\n"
            f"  [{bar}]\n"
            f"{breakdown_lines}\n"
            + (
                f"{'─'*38}\n"
                f"🧲 Zone Confluence ({score_bd.get('zone_confluence', 0):+d} pts):\n"
                + "\n".join([f"  • {r}" for r in signal.get('zone_confluence_reasons', [])]) + "\n"
                if signal.get('zone_confluence_reasons') else ""
            ) +
            f"{'─'*38}\n"
            f"📋 Reasons:\n{reasons_str}\n"
            f"{'─'*38}\n"
            f"🪙 BTC ({BTC_BIAS_TF}): {btc_bias} | BTC.D: {btcd_trend}\n"
            f"  {macro_reason}\n"
            f"{'─'*38}\n"
            f"⚡ Leverage: <b>{signal.get('_leverage', '?')}x</b> | Mode: <b>{'🔴 LIVE' if BOT_MODE == 'LIVE' else '🟢 DEMO'}</b>"
        )
        send_telegram_raw(msg)
    except Exception as e:
        print(f"⚠️ send_telegram_signal gagal build pesan penuh: {e} — kirim notif minimal")
        try:
            send_telegram_raw(
                f"{dir_em} <b>ENTRY {pair} — {dir_str}</b>\n"
                f"Entry: {signal.get('entry','?')} | SL: {signal.get('stop_loss','?')}\n"
                f"TP1: {tps[0] if tps else '?'} | TP2: {tps[1] if len(tps)>1 else '?'}\n"
                f"Score: {signal.get('score','?')} pts | Grade {grade}\n"
                f"⚠️ Detail sinyal tidak bisa ditampilkan (error: {e})"
            )
        except Exception as fallback_err:
            print(f"❌ Notif minimal juga gagal: {fallback_err}")


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 4 — BINANCE API HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def sign_request(params):
    params = dict(params)
    params["timestamp"]  = int(time.time() * 1000)
    params["recvWindow"] = 10000
    query = urlencode(params)
    _, api_secret = get_api_credentials()
    sig   = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    params["signature"] = sig
    return params

def get_headers():
    api_key, _ = get_api_credentials()
    return {"X-MBX-APIKEY": api_key}

def _api_request(method: str, path: str, params: dict, retries: int = 4) -> dict:
    """
    Wrapper request dengan auto-retry untuk rate limit Binance:
    - 418 = IP banned sementara (terlalu banyak request) → tunggu lama
    - 429 = rate limit → tunggu sesuai Retry-After header
    Exponential backoff: 5s, 15s, 30s, 60s
    """
    url     = get_base_url() + path
    headers = get_headers()
    wait_times = [5, 15, 30, 60]

    for attempt in range(retries + 1):
        try:
            if method == "GET":
                r = requests.get(url, params=params, headers=headers, timeout=15)
            elif method == "POST":
                r = requests.post(url, params=params, headers=headers, timeout=15)
            elif method == "DELETE":
                r = requests.delete(url, params=params, headers=headers, timeout=15)
            else:
                raise ValueError(f"Unknown method: {method}")

            if r.status_code == 418:
                wait = wait_times[min(attempt, len(wait_times) - 1)]
                print(f"  ⚠️  Binance 418 IP ban sementara — tunggu {wait}s (attempt {attempt+1}/{retries+1})")
                if attempt < retries:
                    time.sleep(wait)
                    continue
                r.raise_for_status()

            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", wait_times[min(attempt, len(wait_times) - 1)]))
                print(f"  ⚠️  Binance 429 rate limit — tunggu {retry_after}s (attempt {attempt+1}/{retries+1})")
                if attempt < retries:
                    time.sleep(retry_after)
                    continue
                r.raise_for_status()

            if not r.ok and method == "POST":
                try:
                    err = r.json()
                    print(f"  ❌ api_{method.lower()} {path} HTTP {r.status_code} | code={err.get('code')} msg={err.get('msg')}")
                except Exception:
                    print(f"  ❌ api_{method.lower()} {path} HTTP {r.status_code} | body={r.text[:300]}")

            r.raise_for_status()
            return r.json()

        except requests.exceptions.HTTPError:
            raise
        except requests.exceptions.Timeout:
            if attempt < retries:
                print(f"  ⚠️  Timeout {path} — retry {attempt+1}/{retries}")
                time.sleep(wait_times[min(attempt, len(wait_times) - 1)])
                continue
            raise
        except Exception:
            raise

    raise RuntimeError(f"API {method} {path} gagal setelah {retries+1} attempt")


def api_get(path, params=None, signed=False):
    if params is None:
        params = {}
    if signed:
        params = sign_request(params)
    return _api_request("GET", path, params)

def api_post(path, params=None):
    if params is None:
        params = {}
    params = sign_request(params)
    return _api_request("POST", path, params)

def api_delete(path, params=None):
    if params is None:
        params = {}
    params = sign_request(params)
    return _api_request("DELETE", path, params)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 5 — BALANCE & POSITION
# ═══════════════════════════════════════════════════════════════════════════

def init_balance():
    data = api_get("/fapi/v2/balance", signed=True)
    for asset in data:
        if asset["asset"] == "USDT":
            bal = float(asset["balance"])
            # Hanya set balance_start jika belum ada (jangan timpa nilai dari load_state)
            # Ini menjaga PnL kumulatif tetap valid setelah restart
            if bot_state["balance_start"] == 0:
                bot_state["balance_start"] = bal
            bot_state["start_time"] = datetime.now(timezone.utc)
            # Set snapshot harian & bulanan saat pertama start
            refresh_pnl_snapshots(bal)
            # Set start_bal per mode jika belum pernah diset
            if bot_state["mode_pnl"][BOT_MODE]["start_bal"] == 0:
                bot_state["mode_pnl"][BOT_MODE]["start_bal"] = bal
            # ── Init daily limit snapshot ─────────────────────────────────────
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if _daily_limit_state["date"] != today:
                _daily_limit_state["date"]         = today
                _daily_limit_state["balance_open"] = bal
                _daily_limit_state["paused_by"]    = None
                _daily_limit_state["auto_started"] = False
                print(f"📅 Daily limit init — {today} | Balance open: {bal:.2f} USDT")
            print(f"💰 Balance BOT: {bal} USDT")
            send_telegram_raw(
                f"🚀 <b>Bot SMC Auto Trade Started</b>\n"
                f"💰 Balance : <b>{bal} USDT</b>\n"
                f"🌐 Mode    : <b>{'🔴 LIVE (mainnet)' if BOT_MODE == 'LIVE' else '🟢 DEMO (testnet)'}</b>\n"
                f"ℹ️ Gunakan /changeliveordemo untuk ganti mode."
            )
            return bal
    return 0

def get_balance():
    """Ambil available balance USDT. Retry 3x jika 0 (kadang timing issue setelah posisi open)."""
    for _attempt in range(3):
        try:
            data = api_get("/fapi/v2/balance", signed=True)
            for asset in data:
                if asset["asset"] == "USDT":
                    val = float(asset["availableBalance"])
                    if val > 0 or _attempt == 2:
                        return val
            if _attempt < 2:
                time.sleep(0.3)
        except Exception as e:
            if _attempt == 2:
                print(f"  ⚠️  get_balance error: {e}")
            time.sleep(0.3)
    return 0

def get_total_balance():
    """Ambil total balance USDT (termasuk margin terpakai). Retry 3x jika 0."""
    for _attempt in range(3):
        try:
            data = api_get("/fapi/v2/balance", signed=True)
            for asset in data:
                if asset["asset"] == "USDT":
                    val = float(asset["balance"])
                    if val > 0 or _attempt == 2:
                        return val
            if _attempt < 2:
                time.sleep(0.3)
        except Exception as e:
            if _attempt == 2:
                print(f"  ⚠️  get_total_balance error: {e}")
            time.sleep(0.3)
    return 0

def has_position(symbol: str) -> bool:
    """
    Cek apakah posisi BENAR-BENAR masih open di Binance (query langsung).
    Tidak mengandalkan active_positions memory saja — bisa stale kalau
    posisi close dari luar (TP/SL kena, atau manual close di app Binance).
    Juga cek pending_limit_orders agar tidak double-entry.
    """
    # ── Cek pending limit order dulu (cepat, tanpa API call) ─────────────────
    if symbol in pending_limit_orders:
        return True
    try:
        data = api_get("/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
        for p in data:
            if float(p["positionAmt"]) != 0:
                return True
        return False
    except Exception as e:
        # Fallback ke memory jika API error — lebih aman daripada double entry
        print(f"  ⚠️  has_position API error {symbol}: {e} — fallback ke memory")
        return symbol in active_positions


def has_active_position(pair: str, trade_direction: str) -> bool:
    """
    Cek apakah sudah ada posisi aktif untuk pair + arah tertentu.
    trade_direction: 'BULLISH' atau 'BEARISH'

    Cek dari active_positions memory (cepat) + pending_limit_orders + validasi ke Binance.
    Mencegah double-entry di arah yang sama.
    """
    dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"

    # ── Cek pending limit order (belum terisi tapi slot sudah dipakai) ────────
    if pair in pending_limit_orders:
        pend_dir = pending_limit_orders[pair].get("direction", "")
        if pend_dir == dir_str:
            return True

    # ── Cek dari memory dulu (cepat) ─────────────────────────────────────────
    for sym, pos in active_positions.items():
        if sym == pair and pos.get("direction") == dir_str:
            return True

    # ── Cek langsung ke Binance untuk arah spesifik (hedge mode aware) ───────
    try:
        data = api_get("/fapi/v2/positionRisk", {"symbol": pair}, signed=True)
        for p in data:
            amt = float(p.get("positionAmt", 0))
            if amt == 0:
                continue
            # ONE-WAY mode: amt > 0 = LONG, amt < 0 = SHORT
            pos_side = p.get("positionSide", "BOTH")
            if pos_side == "BOTH":
                if dir_str == "LONG" and amt > 0:
                    return True
                if dir_str == "SHORT" and amt < 0:
                    return True
            else:
                # HEDGE mode: positionSide eksplisit
                if pos_side == dir_str:
                    return True
    except Exception as e:
        print(f"  ⚠️  has_active_position API error {pair}: {e} — fallback ke memory")
        # Fallback: kalau API error, cek memory saja
        return any(sym == pair and pos.get("direction") == dir_str
                   for sym, pos in active_positions.items())

    return False


def sync_closed_positions():
    """
    Scan semua active_positions dan bandingkan dengan posisi real di Binance.
    Jika posisi sudah tidak ada di Binance (close dari luar / TP-SL kena),
    hapus dari active_positions dan kirim notif ke Telegram.

    Dipanggil setiap scan loop — ringan karena hanya query 1x untuk semua posisi.
    """
    if not active_positions:
        return

    try:
        # Ambil semua posisi Binance sekali — lebih efisien dari query per symbol
        all_pos = api_get("/fapi/v2/positionRisk", signed=True)
        # ── Hedge mode fix: satu symbol bisa punya 2 row (LONG + SHORT) ──────
        # Simpan sebagai dict: symbol → total |positionAmt| dari semua sisi.
        # Selama SALAH SATU sisi masih open (amt != 0), posisi dianggap masih ada.
        live_positions: dict = {}
        for p in all_pos:
            sym = p["symbol"]
            amt = abs(float(p.get("positionAmt", 0)))
            live_positions[sym] = live_positions.get(sym, 0.0) + amt
    except Exception as e:
        print(f"  ⚠️  sync_closed_positions: gagal fetch positionRisk: {e}")
        return

    to_remove = []
    for symbol, pos in list(active_positions.items()):
        live_total = live_positions.get(symbol, 0.0)
        if live_total == 0.0:
            # Posisi sudah tidak ada di Binance — close dari luar
            to_remove.append(symbol)

    for symbol in to_remove:
        pos       = active_positions[symbol]
        direction = pos["direction"]
        entry     = pos["entry"]
        sl_price  = pos["sl"]
        tp1_price = pos["tp1"]
        tp2_price = pos["tp2"]
        price     = get_current_price(symbol)

        # ── Bersihkan sisa open orders (SL/TP yang belum kena) ───────────────
        # Setelah posisi close (TP1 kena, SL kena, atau manual), order SL/TP
        # yang tersisa di Binance dibersihkan otomatis agar tidak nyampah.
        try:
            cancel_open_orders(symbol)
            print(f"  🧹 [{symbol}] Sisa open orders dibersihkan setelah posisi close.")
        except Exception as _cancel_err:
            print(f"  ⚠️  [{symbol}] Gagal cancel sisa open orders: {_cancel_err}")

        # ── Tentukan apakah close karena SL atau TP ──────────────────────────
        # Prioritas deteksi:
        # 1. Trailing BE aktif (tp1_hit=True, trailing_active=True) →
        #    sl sudah di entry, jadi price ≈ sl_price BUKAN SL murni, ini trailing win
        # 2. Price dekat TP → TP hit
        # 3. Price dekat SL (original) → SL hit
        # 4. Ambiguous → cek jarak
        is_sl_hit     = False
        is_trailing_be = pos.get("tp1_hit", False) and pos.get("trailing_active", False)

        if price is not None:
            sl_dist  = abs(price - sl_price)  / max(sl_price,  1e-9)
            tp1_dist = abs(price - tp1_price) / max(tp1_price, 1e-9)

            if direction == "LONG":
                if is_trailing_be and price >= entry * 0.998:
                    # Trailing BE close: harga di atas atau sangat dekat entry → bukan SL
                    is_sl_hit = False
                elif price <= sl_price * 1.005:
                    is_sl_hit = True
                elif price >= tp1_price * 0.995:
                    is_sl_hit = False
                else:
                    is_sl_hit = sl_dist < tp1_dist
            else:  # SHORT
                if is_trailing_be and price <= entry * 1.002:
                    # Trailing BE close: harga di bawah atau sangat dekat entry → bukan SL
                    is_sl_hit = False
                elif price >= sl_price * 0.995:
                    is_sl_hit = True
                elif price <= tp1_price * 1.005:
                    is_sl_hit = False
                else:
                    is_sl_hit = sl_dist < tp1_dist

        # Tentukan hasil trade
        if price is not None:
            # Kalau TP1 sudah kena sebelumnya, lot yang tersisa hanya tp2_size.
            # Pakai full lot hanya kalau TP1 belum pernah kena.
            _close_lot = pos.get("tp2_size") if pos.get("tp1_hit") else pos["lot"]
            if not _close_lot:
                _close_lot = pos["lot"]
            # BE = TP1 sudah kena + bukan SL murni → dihitung WIN meski sisa lot close di entry
            _is_be = pos.get("tp1_hit", False) and not is_sl_hit
            if direction == "LONG":
                pnl    = (price - entry) * _close_lot
                is_win = _is_be or price > entry
            else:
                pnl    = (entry - price) * _close_lot
                is_win = _is_be or price < entry
            # BE close: pastikan PnL sedikit positif agar update_performance hitung WIN
            if _is_be and pnl <= 0:
                pnl = 0.001
            pnl_em  = "🟢" if pnl >= 0 else "🔴"
            pnl_str = f"{pnl_em} {'+'if pnl>=0 else ''}{pnl:.2f} USDT"
        else:
            pnl_str = "N/A"
            is_win  = False
            is_sl_hit = False  # tidak bisa tentukan, asumsikan bukan SL

        _perf_pnl = pnl if price is not None else (1 if is_win else -1)
        update_performance(_perf_pnl)
        del active_positions[symbol]

        # ── Set cooldown: 4 jam jika SL, tidak ada cooldown jika TP/trailing ───
        if is_sl_hit:
            set_cooldown_post_close(symbol, reason="SL hit terdeteksi", is_stoploss=True)
            record_pair_sl(symbol)
            close_reason  = "🛑 Stoploss kena"
            cooldown_note = f"⏳ <b>Cooldown {COOLDOWN_AFTER_SL_HOURS} jam aktif</b> — bot tidak akan re-entry {symbol} setelah SL."
        elif is_trailing_be:
            set_cooldown_post_close(symbol, reason="Trailing BE close", is_stoploss=False)
            close_reason  = "🔄 Trailing Stop (Break Even) — posisi ditutup di area profit"
            cooldown_note = f"✅ Trailing BE — tidak ada cooldown, bot bisa re-entry {symbol} segera."
        else:
            set_cooldown_post_close(symbol, reason="TP/manual close", is_stoploss=False)
            close_reason  = "🎯 Take Profit / Manual Close"
            cooldown_note = "✅ Tidak ada cooldown — bot bisa re-entry {symbol} segera.".format(symbol=symbol)

        wins    = bot_state["wins"]
        losses  = bot_state["losses"]
        total   = wins + losses
        wr      = (wins / total * 100) if total > 0 else 0
        dir_em  = "🟢" if direction == "LONG" else "🔴"

        print(f"  🔄 [{symbol}] Posisi close terdeteksi dari luar | PnL: {pnl_str} | {'SL HIT' if is_sl_hit else 'TP/Manual'}")
        send_telegram_raw(
            f"🔄 <b>Posisi Ditutup Dari Luar — {symbol}</b>\n"
            f"{'─'*34}\n"
            f"{dir_em} Arah     : <b>{direction}</b>\n"
            f"💰 Entry   : <b>{entry}</b>\n"
            f"📊 PnL Est : <b>{pnl_str}</b>\n"
            f"{'─'*34}\n"
            f"{close_reason}\n"
            f"ℹ️ Posisi ini ditutup di luar bot\n"
            f"(TP/SL kena, atau manual close di Binance)\n"
            f"🧹 Sisa open orders (SL/TP) sudah dibersihkan otomatis.\n"
            f"{'─'*34}\n"
            f"📊 {'✅ WIN' if is_win else '❌ LOSS'} | "
            f"Win: <b>{wins}</b> | Loss: <b>{losses}</b> | WR: <b>{wr:.1f}%</b>\n"
            f"{'─'*34}\n"
            f"{cooldown_note}"
        )

    if to_remove:
        print(f"  🔄 sync_closed_positions: {len(to_remove)} posisi dihapus dari memory → {to_remove}")

def count_open_positions():
    """
    Hitung posisi aktif dari Binance + active_positions memory.
    Pakai max keduanya untuk handle race condition:
    posisi yang baru dibuka mungkin belum muncul di positionRisk Binance.
    """
    try:
        data = api_get("/fapi/v2/positionRisk", signed=True)
        binance_count = sum(1 for p in data if float(p["positionAmt"]) != 0)
    except Exception:
        binance_count = 0
    memory_count = len(active_positions)
    # Pakai nilai terbesar — lebih aman daripada under-count
    return max(binance_count, memory_count)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 5b — SINKRONISASI POSISI PRE-EXISTING
# ═══════════════════════════════════════════════════════════════════════════
#
#  Posisi yang sudah buka SEBELUM bot di-start akan di-import ke
#  active_positions sehingga:
#  - Ikut dihitung di hourly report & /pnl
#  - Trailing stop & TP monitor tetap berjalan
#  - /closeallposition bisa menutupnya
#
#  Data yang tidak diketahui (entry asli, SL/TP) diisi dari Binance
#  positionRisk + openOrders supaya semaksimal mungkin akurat.
# ═══════════════════════════════════════════════════════════════════════════

def sync_existing_positions():
    """
    Import semua posisi yang sudah ada di Binance ke active_positions.
    Dipanggil sekali saat bot start, setelah init_balance().
    """
    try:
        all_pos = api_get("/fapi/v2/positionRisk", signed=True)
    except Exception as e:
        print(f"⚠️ Gagal sync posisi existing: {e}")
        return

    imported = []
    for p in all_pos:
        symbol = p["symbol"]
        amt    = float(p["positionAmt"])
        if amt == 0:
            continue
        if symbol in active_positions:
            continue  # sudah ada, skip

        direction  = "LONG" if amt > 0 else "SHORT"
        side       = "BUY" if direction == "LONG" else "SELL"
        sl_side    = "SELL" if side == "BUY" else "BUY"
        entry      = float(p.get("entryPrice", 0))
        mark_price = float(p.get("markPrice",  entry))
        liq_price  = float(p.get("liquidationPrice", 0))
        lot        = abs(amt)

        # Coba ambil SL & TP dari open orders — cek BOTH regular + algo endpoint
        sl_price  = 0.0
        tp1_price = 0.0
        tp2_price = 0.0
        try:
            orders = api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
            stop_orders = [o for o in orders if o["type"] in ("STOP_MARKET", "STOP")]
            tp_orders   = [o for o in orders if o["type"] in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT")]
            if stop_orders:
                sl_price = float(stop_orders[0].get("stopPrice", 0))
            if len(tp_orders) >= 1:
                tp1_price = float(tp_orders[0].get("stopPrice", 0))
            if len(tp_orders) >= 2:
                tp2_price = float(tp_orders[1].get("stopPrice", 0))
        except Exception:
            pass

        # ── Juga cek algoOrder (SL/TP yang dikirim via /fapi/v1/algoOrder) ──
        # Mainnet saja — testnet tidak support endpoint ini (404)
        if sl_price == 0.0 or tp1_price == 0.0:
            try:
                algo_resp = api_get("/fapi/v1/algoOrder/openOrders", {"symbol": symbol}, signed=True)
                algo_list = algo_resp if isinstance(algo_resp, list) else algo_resp.get("orders", [])
                algo_stops = [o for o in algo_list if o.get("type") in ("STOP_MARKET", "STOP")]
                algo_tps   = [o for o in algo_list if o.get("type") in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT")]
                if sl_price == 0.0 and algo_stops:
                    sl_price = float(algo_stops[0].get("triggerPrice", 0) or algo_stops[0].get("stopPrice", 0))
                if tp1_price == 0.0 and len(algo_tps) >= 1:
                    tp1_price = float(algo_tps[0].get("triggerPrice", 0) or algo_tps[0].get("stopPrice", 0))
                if tp2_price == 0.0 and len(algo_tps) >= 2:
                    tp2_price = float(algo_tps[1].get("triggerPrice", 0) or algo_tps[1].get("stopPrice", 0))
            except Exception:
                pass  # testnet → 404, skip saja

        # Fallback SL & TP kalau open orders kosong
        if sl_price == 0:
            # Estimasi SL dari liquidation price (lebih aman)
            if liq_price > 0:
                sl_price = liq_price
            elif direction == "LONG":
                sl_price = entry * 0.97
            else:
                sl_price = entry * 1.03

        if tp1_price == 0:
            tp1_price = (entry * 1.015) if direction == "LONG" else (entry * 0.985)
        if tp2_price == 0:
            tp2_price = (entry * 1.025) if direction == "LONG" else (entry * 0.975)

        # Ambil tick & step size
        filters      = get_lot_filters(symbol)
        tick_size    = filters.get("tickSize",     0.01)
        step_size    = filters.get("stepSize",     0.001)
        mkt_step     = filters.get("mkt_stepSize", step_size)
        min_qty      = filters.get("minQty",       0.001)
        qty_prec     = filters.get("quantityPrecision", None)

        tp1_size = round_lot_to_step(lot * TP1_PARTIAL, step_size)
        tp2_size = round_lot_to_step(lot - tp1_size, step_size)

        # ── Flag: apakah perlu pasang SL/TP baru? ────────────────────────────
        needs_sl = (sl_price == 0.0)
        needs_tp = (tp1_price == 0.0)

        active_positions[symbol] = {
            "entry":          entry,
            "sl":             sl_price,
            "tp1":            tp1_price,
            "tp2":            tp2_price,
            "lot":            lot,
            "tp1_size":       tp1_size,
            "tp2_size":       tp2_size,
            "side":           side,
            "sl_side":        sl_side,
            "direction":      direction,
            "position_side":  "BOTH",   # default; hedge mode akan override saat monitor
            "tick_size":      tick_size,
            "step_size":      step_size,
            "mkt_step_size":  mkt_step,
            "min_qty":        min_qty,
            "eff_prec":       max(0, round(-math.log10(tick_size))) if tick_size > 0 else 4,
            "tp1_hit":        False,
            "trailing_active": False,
            "open_time":      None,   # waktu asli tidak diketahui
            "pre_existing":   True,   # flag — masuk sebelum bot start
        }
        imported.append(symbol)
        print(f"  📥 Import posisi existing: {symbol} {direction} | Entry:{entry} | Lot:{lot} | sl={'ada' if not needs_sl else '❌ KOSONG'}")

        # ── Auto pasang SL (dan TP) jika tidak ada — posisi naked bahaya! ────
        if needs_sl:
            print(f"  🚨 [{symbol}] Tidak ada SL — akan dipasang otomatis...")
            try:
                mark_now  = get_current_price(symbol) or entry
                # Hitung SL dari mark price sekarang (bukan entry lama) agar tidak
                # langsung kena kalau harga sudah bergerak jauh
                if direction == "LONG":
                    auto_sl = round_price_to_tick(min(sl_price if sl_price > 0 else entry * 0.97,
                                                      mark_now * 0.985), tick_size)
                else:
                    auto_sl = round_price_to_tick(max(sl_price if sl_price > 0 else entry * 1.03,
                                                      mark_now * 1.015), tick_size)

                if qty_prec is not None:
                    _fix_lot = round(math.floor(lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
                else:
                    _fix_lot = lot

                algo_post_sl(symbol, sl_side, auto_sl, quantity=_fix_lot, position_side="BOTH")
                active_positions[symbol]["sl"] = auto_sl
                print(f"  ✅ [{symbol}] Auto-SL dipasang di {auto_sl}")
                send_telegram_raw(
                    f"🛡️ <b>AUTO-SL DIPASANG — {symbol}</b>\n"
                    f"{'─'*34}\n"
                    f"Posisi ditemukan tanpa SL saat bot restart.\n"
                    f"{'🟢' if direction == 'LONG' else '🔴'} {direction} | Entry: {entry}\n"
                    f"🛑 SL otomatis dipasang di: <b>{auto_sl}</b>\n"
                    f"Mark price saat ini: {mark_now}"
                )
            except Exception as _sl_err:
                print(f"  ❌ [{symbol}] Gagal auto-pasang SL: {_sl_err}")
                send_telegram_raw(
                    f"🆘 <b>POSISI TANPA SL — {symbol}</b>\n"
                    f"{'─'*34}\n"
                    f"Bot gagal pasang SL otomatis!\n"
                    f"{'🟢' if direction == 'LONG' else '🔴'} {direction} | Entry: {entry} | Lot: {lot}\n"
                    f"⚠️ Cek dan pasang SL manual di Binance!\n"
                    f"Error: <code>{_sl_err}</code>"
                )

        if needs_tp:
            try:
                mark_now = get_current_price(symbol) or entry
                sl_dist  = abs((active_positions[symbol]["sl"] or entry) - entry)
                if sl_dist == 0:
                    sl_dist = entry * 0.02  # fallback 2%
                if direction == "LONG":
                    auto_tp1 = round_price_to_tick(entry + sl_dist * 1.5, tick_size)
                    auto_tp2 = round_price_to_tick(entry + sl_dist * 2.5, tick_size)
                else:
                    auto_tp1 = round_price_to_tick(entry - sl_dist * 1.5, tick_size)
                    auto_tp2 = round_price_to_tick(entry - sl_dist * 2.5, tick_size)

                if qty_prec is not None:
                    _fix_lot = round(math.floor(lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
                else:
                    _fix_lot = lot
                _tp1_sz = round_lot_to_step(_fix_lot * TP1_PARTIAL, mkt_step)
                _tp2_sz = round_lot_to_step(_fix_lot - _tp1_sz, mkt_step)
                if _tp1_sz < min_qty: _tp1_sz = round_lot_to_step(min_qty, mkt_step)
                if _tp2_sz < min_qty: _tp2_sz = round_lot_to_step(min_qty, mkt_step)

                algo_post_tp(symbol, sl_side, auto_tp1, _tp1_sz, position_side="BOTH")
                algo_post_tp(symbol, sl_side, auto_tp2, _tp2_sz, position_side="BOTH")
                active_positions[symbol]["tp1"] = auto_tp1
                active_positions[symbol]["tp2"] = auto_tp2
                active_positions[symbol]["tp1_size"] = _tp1_sz
                active_positions[symbol]["tp2_size"] = _tp2_sz
                print(f"  ✅ [{symbol}] Auto-TP dipasang: tp1={auto_tp1} tp2={auto_tp2}")
            except Exception as _tp_err:
                print(f"  ⚠️  [{symbol}] Gagal auto-pasang TP: {_tp_err}")

    if imported:
        msg_lines = [f"  • {s} {active_positions[s]['direction']} | Entry: {active_positions[s]['entry']}" for s in imported]
        msg = (
            f"📥 <b>Posisi Pre-Existing Terdeteksi</b>\n"
            f"{'─'*38}\n"
            f"Bot menemukan <b>{len(imported)}</b> posisi yang sudah terbuka:\n"
            + "\n".join(msg_lines) + "\n"
            f"{'─'*38}\n"
            f"ℹ️ Posisi ini ikut dihitung di /pnl dan hourly report."
        )
        send_telegram_raw(msg)
        print(f"✅ {len(imported)} posisi pre-existing di-import.")
    else:
        print("✅ Tidak ada posisi pre-existing.")

# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 4 — EMA TREND FILTER
# ═══════════════════════════════════════════════════════════════════════════

def calculate_ema(series: pd.Series, period: int) -> float:
    if len(series) < period:
        return float(series.iloc[-1])
    ema = series.ewm(span=period, adjust=False).mean()
    return float(ema.iloc[-1])


def get_ema_score(df: pd.DataFrame, direction: str) -> tuple:
    closes = df["close"]
    ema20  = calculate_ema(closes, 20)
    ema50  = calculate_ema(closes, 50)
    ema200 = calculate_ema(closes, 200)
    price  = float(closes.iloc[-1])

    if direction == "BULLISH":
        short_aligned = ema20 > ema50
        long_aligned  = ema50 > ema200
    else:
        short_aligned = ema20 < ema50
        long_aligned  = ema50 < ema200

    score = 0
    parts = []
    if short_aligned:
        score += SCORE_EMA_ALIGNED
        parts.append(f"EMA20{'>' if direction == 'BULLISH' else '<'}EMA50 ✅")
    if long_aligned:
        score += SCORE_EMA_BONUS
        parts.append(f"EMA50{'>' if direction == 'BULLISH' else '<'}EMA200 ✅")
    if not short_aligned and not long_aligned:
        parts.append("EMA tidak align ⚠️")

    reason = " | ".join(parts) if parts else "EMA neutral"
    return score, reason, ema20, ema50, ema200


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 5 — MARKET STRUCTURE (BOS / CHoCH)
# ═══════════════════════════════════════════════════════════════════════════

def find_swings(df: pd.DataFrame, window: int = SWING_WINDOW) -> tuple:
    highs, lows = [], []
    for i in range(window, len(df) - window):
        hi_window = df["high"].iloc[i - window: i + window + 1]
        lo_window = df["low"].iloc[i - window: i + window + 1]
        if df["high"].iloc[i] == hi_window.max():
            highs.append((i, float(df["high"].iloc[i])))
        if df["low"].iloc[i] == lo_window.min():
            lows.append((i, float(df["low"].iloc[i])))
    return highs, lows


def detect_structure(df: pd.DataFrame) -> tuple:
    """Returns: (trend, event, last_swing_high, last_swing_low)"""
    highs, lows = find_swings(df)
    if len(highs) < 2 or len(lows) < 2:
        return "RANGING", None, None, None

    last_sh, prev_sh = highs[-1][1], highs[-2][1]
    last_sl, prev_sl = lows[-1][1],  lows[-2][1]

    hh = last_sh > prev_sh
    hl = last_sl > prev_sl
    lh = last_sh < prev_sh
    ll = last_sl < prev_sl

    if hh and hl:   return "BULLISH", "BOS",   last_sh, last_sl
    if lh and ll:   return "BEARISH", "BOS",   last_sh, last_sl
    if hh and ll:   return "RANGING", "CHoCH", last_sh, last_sl
    if lh and hl:   return "RANGING", "CHoCH", last_sh, last_sl
    return "RANGING", None, None, None


def get_ref_bias(df_ref: Optional[pd.DataFrame]) -> str:
    if df_ref is None:
        return "N/A"
    trend, _, _, _ = detect_structure(df_ref)
    return trend


# ═══════════════════════════════════════════════════════════════════════════
# ██  MISSING FUNCTIONS — PA / S&D / S/R / SIGNAL HASH / HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def detect_htf_trend(df: pd.DataFrame) -> str:
    """
    Wrapper sederhana untuk detect_structure — hanya return trend string.
    Dipakai di backtest dan analyze_pair (versi PA lama).
    Return: 'BULLISH' | 'BEARISH' | 'RANGING'
    """
    trend, _, _, _ = detect_structure(df)
    return trend


def detect_price_action_pattern(df: pd.DataFrame, direction: str) -> tuple:
    """
    Deteksi pola Price Action pada candle terakhir.
    Return: (pattern_name: str, score_pts: int)

    Patterns:
    - Engulfing  → SCORE_PA_STRONG  (25 pts)
    - Pin Bar    → SCORE_PA_MEDIUM  (15 pts)
    - Inside Bar → SCORE_PA_WEAK    (8 pts)
    - None       → SCORE_PA_NONE    (-5 pts)
    """
    if df is None or len(df) < 3:
        return "None", SCORE_PA_NONE

    c  = df.iloc[-1]   # candle terakhir
    p  = df.iloc[-2]   # candle sebelumnya

    o, h, l, close_p = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
    po, ph, pl, pc   = float(p["open"]), float(p["high"]), float(p["low"]), float(p["close"])

    body       = abs(close_p - o)
    full_range = h - l if h != l else 1e-9
    upper_wick = h - max(o, close_p)
    lower_wick = min(o, close_p) - l

    # ── Engulfing ─────────────────────────────────────────────────────────────
    bullish_engulf = (close_p > o and close_p > pc and o < po and
                      body > abs(pc - po) * 0.8)
    bearish_engulf = (close_p < o and close_p < pc and o > po and
                      body > abs(pc - po) * 0.8)
    if direction == "BULLISH" and bullish_engulf:
        return "Bullish Engulfing", SCORE_PA_STRONG
    if direction == "BEARISH" and bearish_engulf:
        return "Bearish Engulfing", SCORE_PA_STRONG

    # ── Pin Bar ───────────────────────────────────────────────────────────────
    if body > 0:
        if direction == "BULLISH" and lower_wick >= body * WICK_BODY_RATIO_MIN:
            return "Bullish Pin Bar", SCORE_PA_MEDIUM
        if direction == "BEARISH" and upper_wick >= body * WICK_BODY_RATIO_MIN:
            return "Bearish Pin Bar", SCORE_PA_MEDIUM

    # ── Inside Bar breakout ───────────────────────────────────────────────────
    inside = (h < ph and l > pl)
    if inside:
        if direction == "BULLISH" and close_p > pc:
            return "Inside Bar Bull", SCORE_PA_WEAK
        if direction == "BEARISH" and close_p < pc:
            return "Inside Bar Bear", SCORE_PA_WEAK

    return "None", SCORE_PA_NONE


def find_supply_demand_zones(df: pd.DataFrame, direction: str) -> list:
    """
    Deteksi zona Supply & Demand dari data OHLCV.
    Return: list of dict {type, top, bottom, touches, strength_pct}

    - direction BULLISH → cari Demand zone (low area sebelum impulse naik)
    - direction BEARISH → cari Supply zone (high area sebelum impulse turun)
    """
    if df is None or len(df) < ZONE_LOOKBACK:
        return []

    recent = df.iloc[-ZONE_LOOKBACK:].reset_index(drop=True)
    zones  = []

    for i in range(2, len(recent) - 1):
        c     = recent.iloc[i]
        c_nxt = recent.iloc[i + 1]
        o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
        n_cl  = float(c_nxt["close"])
        body  = abs(cl - o)
        rng   = h - l if h != l else 1e-9

        # Minimum impulse dari zona
        strength_pct = body / (o + 1e-9) * 100
        if strength_pct < MIN_ZONE_STRENGTH_PCT:
            continue

        # Demand zone: candle bearish diikuti impulse bullish kuat
        if direction == "BULLISH" and cl < o and n_cl > cl:
            zone = {
                "type":         "demand",
                "top":          max(o, cl),
                "bottom":       l,
                "touches":      0,
                "strength_pct": strength_pct,
            }
            # Hitung berapa kali harga menyentuh zona setelahnya
            for j in range(i + 1, len(recent)):
                price_j = float(recent.iloc[j]["low"])
                if zone["bottom"] <= price_j <= zone["top"]:
                    zone["touches"] += 1
            if zone["touches"] <= ZONE_MITIGATED_LIMIT:
                zones.append(zone)

        # Supply zone: candle bullish diikuti impulse bearish kuat
        elif direction == "BEARISH" and cl > o and n_cl < cl:
            zone = {
                "type":         "supply",
                "top":          h,
                "bottom":       min(o, cl),
                "touches":      0,
                "strength_pct": strength_pct,
            }
            for j in range(i + 1, len(recent)):
                price_j = float(recent.iloc[j]["high"])
                if zone["bottom"] <= price_j <= zone["top"]:
                    zone["touches"] += 1
            if zone["touches"] <= ZONE_MITIGATED_LIMIT:
                zones.append(zone)

    return zones


def price_in_sd_zone(price: float, zones: list) -> tuple:
    """
    Cek apakah harga saat ini berada di dalam salah satu zona S&D.
    Return: (in_zone: bool, best_zone: dict | None)
    best_zone = zona dengan touches paling sedikit (paling fresh)
    """
    if not zones:
        return False, None

    candidates = []
    for z in zones:
        lo  = z["bottom"] * (1 - ZONE_PROXIMITY_PCT)
        hi  = z["top"]    * (1 + ZONE_PROXIMITY_PCT)
        if lo <= price <= hi:
            candidates.append(z)

    if not candidates:
        return False, None

    # Pilih zona paling fresh (touches paling sedikit)
    best = min(candidates, key=lambda z: z["touches"])
    return True, best


def find_sr_levels(df: pd.DataFrame) -> list:
    """
    Deteksi level Support & Resistance dari swing high/low.
    Return: list of dict {price, touches, type}
    """
    if df is None or len(df) < SR_LOOKBACK:
        return []

    recent  = df.iloc[-SR_LOOKBACK:].reset_index(drop=True)
    highs, lows = find_swings(recent, window=3)
    levels  = []

    # Kumpulkan semua swing high & low sebagai kandidat level
    candidates = [(h[1], "resistance") for h in highs] + [(l[1], "support") for l in lows]

    # Cluster level yang berdekatan (dalam SR_PROXIMITY_PCT) jadi satu
    merged = []
    used   = set()
    for i, (p1, t1) in enumerate(candidates):
        if i in used:
            continue
        cluster = [p1]
        for j, (p2, _) in enumerate(candidates):
            if j != i and j not in used:
                if abs(p1 - p2) / (p1 + 1e-9) <= SR_PROXIMITY_PCT:
                    cluster.append(p2)
                    used.add(j)
        used.add(i)
        avg_price = sum(cluster) / len(cluster)
        merged.append({"price": avg_price, "touches": len(cluster), "type": t1})

    # Filter: minimum SR_TOUCH_MIN touches
    return [lv for lv in merged if lv["touches"] >= SR_TOUCH_MIN]


def price_near_sr(price: float, sr_levels: list, direction: str) -> tuple:
    """
    Cek apakah harga dekat level S/R yang relevan.
    Return: (near: bool, touches: int, level_price: float | None)
    """
    if not sr_levels:
        return False, 0, None

    for lv in sorted(sr_levels, key=lambda x: abs(x["price"] - price)):
        dist_pct = abs(price - lv["price"]) / (lv["price"] + 1e-9)
        if dist_pct <= SR_PROXIMITY_PCT:
            # Validasi: support untuk BULLISH, resistance untuk BEARISH
            if direction == "BULLISH" and lv["type"] == "support":
                return True, lv["touches"], lv["price"]
            if direction == "BEARISH" and lv["type"] == "resistance":
                return True, lv["touches"], lv["price"]
            # Jika tipe tidak cocok tapi sangat dekat, tetap hitung
            if dist_pct <= SR_PROXIMITY_PCT * 0.5:
                return True, lv["touches"], lv["price"]

    return False, 0, None


def compute_zone_confluence_score(
    price: float,
    direction: str,
    ob: Optional[dict],
    fvg: Optional[dict],
    sr_levels: list,
    df: pd.DataFrame,
) -> tuple:
    """
    Hitung seberapa kuat zona entry berdasarkan confluence faktor.

    Faktor yang dinilai:
      1. Freshness OB (taps=0 → kuat, taps=1 → cukup, taps>2 → lemah)
      2. FVG overlap dengan OB (dua imbalance sekaligus di zona yang sama)
      3. S/R level tumpuk di zona entry (level historis ikut jadi magnet)
      4. Kekuatan impulse saat OB terbentuk (>1% body candle impulse)
      5. Posisi zona relatif ke equilibrium (Discount untuk LONG, Premium untuk SHORT)
      6. Super-fresh bonus jika OB fresh + impulse tinggi sekaligus

    Return: (confluence_score: int, confluence_reasons: list[str])
    """
    score   = 0
    reasons = []

    if ob is None and fvg is None:
        return 0, []

    # ── 1. OB Freshness ──────────────────────────────────────────────────────
    if ob is not None:
        taps      = ob.get("taps", 0)
        impulse   = ob.get("impulse", 0.0)   # % body candle impulse

        if taps == 0:
            score += SCORE_ZONE_CONFLUENCE_OB_FRESH
            reasons.append("OB fresh (belum disentuh)")
        elif taps == 1:
            score += SCORE_ZONE_CONFLUENCE_OB_TESTED
            reasons.append("OB tested 1x")
        elif taps > 2:
            score += SCORE_ZONE_WEAK_PENALTY
            reasons.append(f"OB lemah ({taps}x disentuh)")

        # ── 4. Impulse kuat saat OB terbentuk ────────────────────────────────
        if impulse >= 1.0:
            score += SCORE_ZONE_CONFLUENCE_IMPULSE
            reasons.append(f"Impulse kuat ({impulse:.1f}%)")

        # ── 7. Super-fresh bonus (OB fresh + impulse tinggi) ─────────────────
        if taps == 0 and impulse >= 1.5:
            score += SCORE_ZONE_CONFLUENCE_SUPER_FRESH
            reasons.append("Super-fresh OB (taps=0 + impulse≥1.5%)")

    # ── 2. FVG overlap dengan OB ──────────────────────────────────────────────
    if ob is not None and fvg is not None:
        # Cek apakah area FVG bertumpang tindih dengan area OB
        ob_lo  = ob.get("low",    price * 0.99)
        ob_hi  = ob.get("high",   price * 1.01)
        fvg_lo = fvg.get("bottom", price * 0.99)
        fvg_hi = fvg.get("top",    price * 1.01)
        overlap = min(ob_hi, fvg_hi) - max(ob_lo, fvg_lo)
        if overlap > 0:
            score += SCORE_ZONE_CONFLUENCE_FVG_OVERLAP
            reasons.append("FVG overlap OB (double imbalance)")
    elif fvg is not None and ob is None:
        # FVG saja juga tetap kasih partial bonus (imbalance tanpa OB)
        score += 3
        reasons.append("FVG imbalance (no OB)")

    # ── 3. S/R level tumpuk di zona ──────────────────────────────────────────
    if sr_levels and ob is not None:
        ob_lo = ob.get("low",  price * 0.99)
        ob_hi = ob.get("high", price * 1.01)
        for lv in sr_levels:
            lv_price = lv.get("price", 0)
            # S/R dianggap tumpuk jika levelnya berada di dalam range OB ± 0.5%
            if ob_lo * 0.995 <= lv_price <= ob_hi * 1.005:
                touches = lv.get("touches", 0)
                score  += SCORE_ZONE_CONFLUENCE_SR_OVERLAP
                reasons.append(f"S/R tumpuk di OB ({touches} touches @ {lv_price:.4f})")
                break   # cukup 1 S/R yang overlap, tidak perlu double-count

    # ── 5. Posisi zona relatif ke equilibrium (Premium / Discount) ───────────
    # Equilibrium = midpoint dari range 50 candle terakhir
    try:
        recent_high = float(df["high"].iloc[-50:].max())
        recent_low  = float(df["low"].iloc[-50:].min())
        equilibrium = (recent_high + recent_low) / 2.0

        if direction == "BULLISH" and price < equilibrium:
            # Beli di Discount (bawah EQ) → lebih aman
            score += SCORE_ZONE_CONFLUENCE_PD_ARRAY
            reasons.append(f"Zona di Discount (harga {price:.4f} < EQ {equilibrium:.4f})")
        elif direction == "BEARISH" and price > equilibrium:
            # Jual di Premium (atas EQ) → lebih aman
            score += SCORE_ZONE_CONFLUENCE_PD_ARRAY
            reasons.append(f"Zona di Premium (harga {price:.4f} > EQ {equilibrium:.4f})")
    except Exception:
        pass

    return score, reasons


def get_btcd_bias() -> str:
    """
    Ambil BTC Dominance trend untuk filter korelasi.
    Return: 'BULLISH' | 'BEARISH' | 'FLAT'
    """
    try:
        df_btcd = fetch_btcd_ohlcv(tf=BTCD_TF, limit=80)
        if df_btcd is None or len(df_btcd) < 10:
            return "FLAT"
        trend = get_btcd_trend(df_btcd)
        if trend == "RISING":   return "BULLISH"
        if trend == "FALLING":  return "BEARISH"
        return "FLAT"
    except Exception as e:
        print(f"  ⚠️  get_btcd_bias error: {e}")
        return "FLAT"


# ═══════════════════════════════════════════════════════════════════════════
# ██  MARKET REGIME DETECTOR — BTC + BTCD EMA Real-Time
# ═══════════════════════════════════════════════════════════════════════════
#
# Logika sederhana & tegas:
#   BULL_REGIME  : BTC EMA bullish + BTCD turun  → LONG ok, SHORT DIBLOK
#   BEAR_REGIME  : BTC EMA bearish + BTCD naik   → SHORT ok, LONG DIBLOK
#   NEUTRAL      : Kondisi lain                   → kedua arah boleh (ikut sinyal biasa)
#
# Deteksi pakai EMA (bukan swing) agar responsif terhadap crash/pump mendadak.
# Cache 3 menit agar tidak overload API di setiap scan pair.
# ═══════════════════════════════════════════════════════════════════════════

_regime_cache: dict = {"regime": "NEUTRAL", "reason": "", "ts": 0.0}
_REGIME_CACHE_TTL = 180   # detik — refresh setiap 3 menit

def get_market_regime() -> tuple:
    """
    Deteksi market regime dari BTC EMA + BTCD EMA secara real-time.

    Return: (regime: str, reason: str, block_long: bool, block_short: bool)
      regime  : 'BULL_REGIME' | 'BEAR_REGIME' | 'NEUTRAL'
      reason  : penjelasan singkat untuk log
      block_long  : True jika semua sinyal LONG harus diblok
      block_short : True jika semua sinyal SHORT harus diblok
    """
    global _regime_cache
    now = time.time()

    # Return cache jika masih valid
    if (now - _regime_cache["ts"]) < _REGIME_CACHE_TTL:
        r = _regime_cache
        return r["regime"], r["reason"], r["block_long"], r["block_short"]

    regime     = "NEUTRAL"
    reason     = "BTC/BTCD kondisi campuran — kedua arah diizinkan"
    block_long  = False
    block_short = False

    try:
        # ── Fetch BTC LTF (1h, 60 candle) untuk EMA direction ────────────────
        df_btc = fetch_ohlcv("BTC/USDT", "1h", limit=60)
        if df_btc is None or len(df_btc) < 30:
            raise ValueError("BTC data tidak cukup")

        closes_btc = df_btc["close"].astype(float)
        btc_ema9   = float(closes_btc.ewm(span=9,  adjust=False).mean().iloc[-1])
        btc_ema21  = float(closes_btc.ewm(span=21, adjust=False).mean().iloc[-1])
        btc_price  = float(closes_btc.iloc[-1])

        # BTC bullish: harga di atas EMA9 dan EMA9 di atas EMA21
        btc_bullish = (btc_price > btc_ema9) and (btc_ema9 > btc_ema21)
        # BTC bearish: harga di bawah EMA9 dan EMA9 di bawah EMA21
        btc_bearish = (btc_price < btc_ema9) and (btc_ema9 < btc_ema21)

        # ── Fetch BTCD LTF untuk EMA direction ───────────────────────────────
        df_btcd = fetch_btcd_ohlcv(tf=BTCDOM_LTF_TF, limit=60)
        btcd_rising  = False
        btcd_falling = False

        if df_btcd is not None and len(df_btcd) >= 20:
            closes_btcd  = df_btcd["close"].astype(float)
            btcd_ema9    = float(closes_btcd.ewm(span=9,  adjust=False).mean().iloc[-1])
            btcd_ema21   = float(closes_btcd.ewm(span=21, adjust=False).mean().iloc[-1])
            btcd_price   = float(closes_btcd.iloc[-1])
            btcd_rising  = (btcd_price > btcd_ema9)  and (btcd_ema9  > btcd_ema21)
            btcd_falling = (btcd_price < btcd_ema9)  and (btcd_ema9  < btcd_ema21)

        # ── Tentukan regime ───────────────────────────────────────────────────
        if btc_bullish and btcd_falling:
            # BTC naik + Dominance turun = Alt season → LONG optimal, SHORT diblok
            regime      = "BULL_REGIME"
            block_short = True
            reason      = (
                f"🟢 BULL REGIME: BTC EMA bullish (price={btc_price:.0f} > EMA9={btc_ema9:.0f} > EMA21={btc_ema21:.0f}) "
                f"+ BTCD EMA turun → Alt season → SHORT alt DIBLOK"
            )

        elif btc_bearish and btcd_rising:
            # BTC turun + Dominance naik = Alt bleeding → SHORT optimal, LONG diblok
            regime     = "BEAR_REGIME"
            block_long = True
            reason     = (
                f"🔴 BEAR REGIME: BTC EMA bearish (price={btc_price:.0f} < EMA9={btc_ema9:.0f} < EMA21={btc_ema21:.0f}) "
                f"+ BTCD EMA naik → Alt bleeding → LONG alt DIBLOK"
            )

        else:
            # Kondisi campuran atau transisi → neutral, biarkan sinyal PA/SMC yang bicara
            btc_state  = "BULLISH" if btc_bullish else ("BEARISH" if btc_bearish else "MIXED")
            btcd_state = "RISING"  if btcd_rising  else ("FALLING" if btcd_falling  else "MIXED")
            reason     = (
                f"⚪ NEUTRAL: BTC EMA={btc_state}, BTCD EMA={btcd_state} "
                f"— tidak ada kondisi dominan, semua arah diizinkan"
            )

    except Exception as e:
        reason = f"⚠️ Regime error: {e} — fallback NEUTRAL"
        print(f"  ⚠️  get_market_regime: {e}")

    # Simpan ke cache
    _regime_cache = {
        "regime":      regime,
        "reason":      reason,
        "block_long":  block_long,
        "block_short": block_short,
        "ts":          now,
    }
    return regime, reason, block_long, block_short


# ── Signal Hash — deduplikasi sinyal yang sama ───────────────────────────────
_signal_hashes: dict = {}   # key: hash_str → expired_at (datetime)

def _make_signal_hash(pair: str, direction: str, entry: float, sl: float) -> str:
    """Buat hash unik dari kombinasi pair + direction + entry (rounded) + sl (rounded)."""
    e_r = round(entry, 4)
    s_r = round(sl,    4)
    return f"{pair}|{direction}|{e_r}|{s_r}"

def _is_duplicate_signal(pair: str, direction: str, entry: float, sl: float) -> bool:
    """
    Return True jika sinyal dengan kombinasi pair+direction+entry+sl yang sama
    sudah pernah dikirim dalam SIGNAL_HASH_TTL_HOURS jam terakhir.
    """
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    key = _make_signal_hash(pair, direction, entry, sl)

    # Bersihkan hash yang sudah expired
    expired_keys = [k for k, exp in _signal_hashes.items() if now >= exp]
    for k in expired_keys:
        _signal_hashes.pop(k, None)

    return key in _signal_hashes

def _register_signal_hash(pair: str, direction: str, entry: float, sl: float):
    """Daftarkan sinyal ke hash registry — akan expired setelah SIGNAL_HASH_TTL_HOURS."""
    from datetime import timedelta
    key = _make_signal_hash(pair, direction, entry, sl)
    _signal_hashes[key] = datetime.now(timezone.utc) + timedelta(hours=SIGNAL_HASH_TTL_HOURS)


# ── _tg_send: alias untuk send_telegram_raw dengan optional chat_id ──────────
def _tg_send(msg: str, chat_id: str = None):
    """
    Kirim pesan Telegram. Alias dari send_telegram_raw.
    chat_id opsional — jika None, pakai TELEGRAM_CHAT_ID default.
    """
    if chat_id and chat_id != TELEGRAM_CHAT_ID:
        # Kirim ke chat_id spesifik
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try:
            requests.post(url, data={
                "chat_id": chat_id, "text": msg, "parse_mode": "HTML"
            }, timeout=10)
        except Exception as e:
            print(f"⚠️ _tg_send error: {e}")
    else:
        send_telegram_raw(msg)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 6 — LIQUIDITY SWEEP
# ═══════════════════════════════════════════════════════════════════════════

def detect_eqh_eql(df: pd.DataFrame) -> dict:
    recent   = df.iloc[-SWEEP_LOOKBACK:]
    h_vals   = recent["high"].values
    l_vals   = recent["low"].values
    eqh_pool, eql_pool = [], []

    for i in range(len(h_vals)):
        for j in range(i + 1, len(h_vals)):
            if abs(h_vals[i] - h_vals[j]) / (h_vals[i] + 1e-9) <= EQH_EQL_TOLERANCE:
                eqh_pool.append(max(h_vals[i], h_vals[j]))
    for i in range(len(l_vals)):
        for j in range(i + 1, len(l_vals)):
            if abs(l_vals[i] - l_vals[j]) / (l_vals[i] + 1e-9) <= EQH_EQL_TOLERANCE:
                eql_pool.append(min(l_vals[i], l_vals[j]))

    return {
        "eqh": float(max(eqh_pool)) if eqh_pool else None,
        "eql": float(min(eql_pool)) if eql_pool else None,
    }


def detect_liquidity_sweep(df: pd.DataFrame, trend: str, strict: bool = True) -> tuple:
    recent     = df.iloc[-SWEEP_LOOKBACK:]
    c          = df.iloc[-1]
    c_prev     = df.iloc[-2]
    eq         = detect_eqh_eql(df)
    swing_high = float(recent["high"].max())
    swing_low  = float(recent["low"].min())

    def wick_body_ratio(candle, direction: str) -> float:
        o, h, l, cl = (float(candle[x]) for x in ["open", "high", "low", "close"])
        body = abs(cl - o)
        if body < 1e-9:
            return 0.0
        return (min(o, cl) - l) / body if direction == "BULLISH" else (h - max(o, cl)) / body

    def close_rejected(candle, level: float, direction: str) -> bool:
        cl = float(candle["close"])
        return cl > level * (1 - REJECTION_TOLERANCE) if direction == "BULLISH" \
            else cl < level * (1 + REJECTION_TOLERANCE)

    if trend == "BULLISH":
        eql = eq.get("eql")
        for candle in [c_prev, c]:
            swept_level = None
            if float(candle["low"]) < swing_low:
                swept_level = swing_low
            elif eql and float(candle["low"]) < eql:
                swept_level = eql
            if swept_level is not None:
                rejected = close_rejected(candle, swept_level, "BULLISH")
                ratio    = wick_body_ratio(candle, "BULLISH")
                if strict:
                    if rejected and ratio >= WICK_BODY_RATIO_MIN:
                        return True, "Bullish Sweep + Strong Rejection", swept_level, "STRONG"
                else:
                    if rejected:
                        strength = "STRONG" if ratio >= WICK_BODY_RATIO_MIN else "RELAXED"
                        return True, f"Bullish Sweep + Rejection ({strength})", swept_level, strength

    elif trend == "BEARISH":
        eqh = eq.get("eqh")
        for candle in [c_prev, c]:
            swept_level = None
            if float(candle["high"]) > swing_high:
                swept_level = swing_high
            elif eqh and float(candle["high"]) > eqh:
                swept_level = eqh
            if swept_level is not None:
                rejected = close_rejected(candle, swept_level, "BEARISH")
                ratio    = wick_body_ratio(candle, "BEARISH")
                if strict:
                    if rejected and ratio >= WICK_BODY_RATIO_MIN:
                        return True, "Bearish Sweep + Strong Rejection", swept_level, "STRONG"
                else:
                    if rejected:
                        strength = "STRONG" if ratio >= WICK_BODY_RATIO_MIN else "RELAXED"
                        return True, f"Bearish Sweep + Rejection ({strength})", swept_level, strength

    return False, None, None, None


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 7 — ORDER BLOCK
# ═══════════════════════════════════════════════════════════════════════════

def find_order_blocks(df: pd.DataFrame, trend: str) -> list:
    ob_list = []
    start   = max(1, len(df) - OB_LOOKBACK)
    price   = float(df["close"].iloc[-1])

    for i in range(start, len(df) - 2):
        c   = df.iloc[i]
        nxt = df.iloc[i + 1]
        impulse_pct = abs(float(nxt["close"]) - float(nxt["open"])) / (float(nxt["open"]) + 1e-9) * 100

        if impulse_pct < MIN_DISPLACEMENT_PCT:
            continue

        is_valid = False
        if trend == "BULLISH" and float(c["close"]) < float(c["open"]) and float(nxt["close"]) > float(nxt["open"]):
            is_valid = True
        elif trend == "BEARISH" and float(c["close"]) > float(c["open"]) and float(nxt["close"]) < float(nxt["open"]):
            is_valid = True

        if not is_valid:
            continue

        ob   = {"low": float(c["low"]), "high": float(c["high"]),
                "mid": (float(c["low"]) + float(c["high"])) / 2,
                "index": i, "impulse": impulse_pct}
        taps = sum(
            1 for j in range(i + 2, len(df))
            if float(df.iloc[j]["low"]) <= ob["high"] and float(df.iloc[j]["high"]) >= ob["low"]
        )
        if taps >= OB_MITIGATION_LIMIT:
            continue

        ob["taps"]     = taps
        dist_pct       = abs(price - ob["mid"]) / (price + 1e-9) * 100
        ob["strength"] = impulse_pct / (1.0 + dist_pct) / (1.0 + taps)
        ob_list.append(ob)

    ob_list.sort(key=lambda x: x["strength"], reverse=True)
    return ob_list[:3]


def price_in_ob(price: float, ob_list: list, tolerance: float = 0.004) -> tuple:
    if not ob_list:
        return False, None
    lo   = price * (1 - tolerance)
    hi   = price * (1 + tolerance)
    hits = [ob for ob in ob_list if ob["low"] <= hi and ob["high"] >= lo]
    return (True, max(hits, key=lambda x: x["strength"])) if hits else (False, None)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 8 — FAIR VALUE GAP
# ═══════════════════════════════════════════════════════════════════════════

def find_fvg(df: pd.DataFrame, trend: str) -> Optional[dict]:
    start    = max(1, len(df) - FVG_LOOKBACK)
    price    = float(df["close"].iloc[-1])
    fvg_list = []

    for i in range(start, len(df) - 1):
        p1 = df.iloc[i - 1]
        p3 = df.iloc[i + 1]

        if trend == "BULLISH":
            gap     = float(p3["low"]) - float(p1["high"])
            min_gap = float(p1["high"]) * MIN_FVG_PCT / 100
            if gap < min_gap:
                continue
            fvg_top, fvg_bot = float(p3["low"]), float(p1["high"])
        elif trend == "BEARISH":
            gap     = float(p1["low"]) - float(p3["high"])
            min_gap = float(p3["high"]) * MIN_FVG_PCT / 100
            if gap < min_gap:
                continue
            fvg_top, fvg_bot = float(p1["low"]), float(p3["high"])
        else:
            continue

        post     = df.iloc[i + 2:]
        fill_pct = 0.0
        if len(post) > 0:
            gap_size = fvg_top - fvg_bot
            if trend == "BULLISH":
                deepest  = float(post["low"].min())
                fill_pct = min(1.0, max(0.0, (fvg_top - deepest) / gap_size)) if gap_size > 0 else 0.0
            else:
                deepest  = float(post["high"].max())
                fill_pct = min(1.0, max(0.0, (deepest - fvg_bot) / gap_size)) if gap_size > 0 else 0.0

        if fill_pct >= 0.85:
            continue

        fvg_mid = (fvg_top + fvg_bot) / 2
        in_fvg  = fvg_bot * (1 - 0.005) <= price <= fvg_top * (1 + 0.005)
        fvg_list.append({"top": fvg_top, "bottom": fvg_bot, "mid": fvg_mid,
                         "fill_pct": fill_pct, "index": i, "in_zone": in_fvg})

    if not fvg_list:
        return None
    in_zone = [f for f in fvg_list if f["in_zone"]]
    return in_zone[-1] if in_zone else fvg_list[-1]


def price_in_fvg(price: float, fvg: Optional[dict]) -> bool:
    if not fvg:
        return False
    return fvg["bottom"] * 0.995 <= price <= fvg["top"] * 1.005


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 8b — BODY IMBALANCE (IMB) & PRICE GAP
# ═══════════════════════════════════════════════════════════════════════════

def find_imb(df: pd.DataFrame, trend: str) -> Optional[dict]:
    """
    Deteksi Body Imbalance (IMB) — zona antara body candle impulsif.

    IMB BULLISH: zona antara close candle sebelumnya dan open candle impulsif bullish
    IMB BEARISH: zona antara close candle sebelumnya dan open candle impulsif bearish

    Berbeda dari FVG (gap wick-to-wick), IMB fokus pada body candle sehingga
    lebih konservatif dan sering lebih akurat sebagai zona entry/support.
    """
    start    = max(2, len(df) - IMB_LOOKBACK)
    price    = float(df["close"].iloc[-1])
    imb_list = []

    for i in range(start, len(df) - 1):
        c    = df.iloc[i]
        prev = df.iloc[i - 1]
        o, h, l, cl  = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
        candle_range = h - l
        if candle_range < 1e-9:
            continue
        body_ratio = abs(cl - o) / candle_range

        # Harus candle impulsif (body besar)
        if body_ratio < IMB_MIN_BODY_PCT:
            continue

        prev_cl = float(prev["close"])

        if trend == "BULLISH" and cl > o:
            # IMB Bullish: zona antara prev_close dan open candle bullish ini
            imb_top = o
            imb_bot = prev_cl
            if imb_top <= imb_bot:
                continue
        elif trend == "BEARISH" and cl < o:
            # IMB Bearish: zona antara open candle bearish dan prev_close
            imb_top = prev_cl
            imb_bot = o
            if imb_top <= imb_bot:
                continue
        else:
            continue

        # Ukuran minimal
        size_pct = (imb_top - imb_bot) / (imb_bot + 1e-9) * 100
        if size_pct < IMB_MIN_SIZE_PCT:
            continue

        # Cek fill: seberapa dalam harga sudah masuk ke zona IMB setelah terbentuk
        post     = df.iloc[i + 1:]
        fill_pct = 0.0
        gap_size = imb_top - imb_bot
        if len(post) > 0 and gap_size > 0:
            if trend == "BULLISH":
                deepest  = float(post["low"].min())
                fill_pct = min(1.0, max(0.0, (imb_top - deepest) / gap_size))
            else:
                deepest  = float(post["high"].max())
                fill_pct = min(1.0, max(0.0, (deepest - imb_bot) / gap_size))

        if fill_pct >= IMB_FILL_LIMIT:
            continue  # IMB sudah terisi terlalu dalam → tidak valid lagi

        imb_mid = (imb_top + imb_bot) / 2
        in_zone = imb_bot * (1 - 0.005) <= price <= imb_top * (1 + 0.005)
        imb_list.append({
            "top":      imb_top, "bottom": imb_bot, "mid": imb_mid,
            "fill_pct": fill_pct, "index": i, "in_zone": in_zone,
            "size_pct": size_pct,
        })

    if not imb_list:
        return None
    in_zone = [z for z in imb_list if z["in_zone"]]
    return in_zone[-1] if in_zone else imb_list[-1]


def price_in_imb(price: float, imb: Optional[dict]) -> bool:
    if not imb:
        return False
    return imb["bottom"] * 0.995 <= price <= imb["top"] * 1.005


def find_gap(df: pd.DataFrame, trend: str) -> Optional[dict]:
    """
    Deteksi Price Gap — area kosong antara high/low dua candle yang tidak overlap.

    Gap BULLISH: low candle ke-3 > high candle ke-1 (harga loncat naik, ada void di bawah)
    Gap BEARISH: high candle ke-3 < low candle ke-1 (harga loncat turun, ada void di atas)
    """
    start    = max(1, len(df) - GAP_LOOKBACK)
    price    = float(df["close"].iloc[-1])
    gap_list = []

    for i in range(start, len(df) - 1):
        c1 = df.iloc[i - 1]
        c3 = df.iloc[i + 1]
        c1_high = float(c1["high"])
        c1_low  = float(c1["low"])
        c3_high = float(c3["high"])
        c3_low  = float(c3["low"])

        if trend == "BULLISH":
            # Gap bullish: c3_low > c1_high → ada ruang di bawah (magnet saat pullback)
            gap_size = c3_low - c1_high
            if gap_size <= 0:
                continue
            gap_top = c3_low
            gap_bot = c1_high
        elif trend == "BEARISH":
            # Gap bearish: c3_high < c1_low → ada ruang di atas (magnet saat pullback)
            gap_size = c1_low - c3_high
            if gap_size <= 0:
                continue
            gap_top = c1_low
            gap_bot = c3_high
        else:
            continue

        # Ukuran minimal (cegah noise micro-gap)
        ref_price = (gap_top + gap_bot) / 2
        size_pct  = gap_size / (ref_price + 1e-9) * 100
        if size_pct < GAP_MIN_SIZE_PCT:
            continue

        # Fill check: cek apakah harga sudah menutup gap
        post     = df.iloc[i + 2:]
        fill_pct = 0.0
        if len(post) > 0 and gap_size > 0:
            if trend == "BULLISH":
                deepest  = float(post["low"].min())
                fill_pct = min(1.0, max(0.0, (gap_top - deepest) / gap_size))
            else:
                deepest  = float(post["high"].max())
                fill_pct = min(1.0, max(0.0, (deepest - gap_bot) / gap_size))

        if fill_pct >= 0.90:
            continue  # Gap sudah 90% tertutup → tidak relevan

        gap_mid = (gap_top + gap_bot) / 2
        in_zone = gap_bot * (1 - 0.005) <= price <= gap_top * (1 + 0.005)
        gap_list.append({
            "top":      gap_top, "bottom": gap_bot, "mid": gap_mid,
            "fill_pct": fill_pct, "index": i, "in_zone": in_zone,
            "size_pct": size_pct,
        })

    if not gap_list:
        return None
    in_zone = [g for g in gap_list if g["in_zone"]]
    return in_zone[-1] if in_zone else gap_list[-1]


def price_in_gap(price: float, gap: Optional[dict]) -> bool:
    if not gap:
        return False
    return gap["bottom"] * 0.995 <= price <= gap["top"] * 1.005


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 9 — DISPLACEMENT & VOLUME
# ═══════════════════════════════════════════════════════════════════════════

def detect_displacement(df: pd.DataFrame, trend: str) -> bool:
    threshold = MIN_DISPLACEMENT_PCT * 2
    for candle in [df.iloc[-1], df.iloc[-2]]:
        o, cl    = float(candle["open"]), float(candle["close"])
        body_pct = abs(cl - o) / (o + 1e-9) * 100
        if body_pct >= threshold:
            if trend == "BULLISH" and cl > o:
                return True
            if trend == "BEARISH" and cl < o:
                return True
    return False


def volume_ratio(df: pd.DataFrame, lookback: int = 20) -> float:
    avg = df["volume"].iloc[-lookback:-1].mean()
    vol = float(df["volume"].iloc[-1])
    return round(vol / avg, 2) if avg > 0 else 0.0


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 10 — RSI
# ═══════════════════════════════════════════════════════════════════════════

def calculate_rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> float:
    closes = df["close"].values
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]
    avg_g  = sum(gains[:period]) / period
    avg_l  = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return round(100.0 - (100.0 / (1.0 + avg_g / avg_l)), 2)


def rsi_score(rsi: float, direction: str = "") -> int:
    # Penalti ganda jika RSI overbought untuk LONG atau oversold untuk SHORT
    if direction == "BULLISH" and rsi > 70:
        return SCORE_RSI_PENALTY * 2   # -10: bahaya entry LONG saat overbought
    if direction == "BEARISH" and rsi < 30:
        return SCORE_RSI_PENALTY * 2   # -10: bahaya entry SHORT saat oversold
    if 50 <= rsi <= 65:
        return SCORE_RSI_IDEAL
    if rsi > 75 or rsi < 25:
        return SCORE_RSI_PENALTY
    return 0


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 11 — MACRO
# ═══════════════════════════════════════════════════════════════════════════

_btcd_cache: dict = {"df": None, "ts": 0.0}

# v15: Ticker BTCD di Binance Futures
BTCDOM_TICKER    = "BTCDOMUSDT"
BTCDOM_HTF_TF    = "15m"   # LTF untuk trend BTCD (pair ini hanya tersedia di LTF)
BTCDOM_LTF_TF    = "15m"   # LTF untuk korelasi real-time
BTCD_CACHE_TTL   = 60      # cache 60 detik


def fetch_btcd_ohlcv(tf: str = BTCDOM_LTF_TF, limit: int = 80) -> Optional[pd.DataFrame]:
    """
    Fetch OHLCV BTCDOMUSDT dari Binance Futures via /fapi/v1/indexPriceKlines.
    BTCDOMUSDT adalah index pair — tidak bisa pakai /fapi/v1/klines biasa.
    Jika tidak tersedia atau error → return None → BTC.D dianggap RANGING (FLAT).
    """
    global _btcd_cache
    now = time.time()
    if (
        _btcd_cache["df"] is not None
        and (now - _btcd_cache["ts"]) < BTCD_CACHE_TTL
    ):
        return _btcd_cache["df"]

    try:
        tf_mapped = _INTERVAL_MAP.get(tf, tf)
        url    = f"{get_base_url()}/fapi/v1/indexPriceKlines"
        params = {"pair": BTCDOM_TICKER, "interval": tf_mapped, "limit": limit}
        r      = requests.get(url, params=params, headers=get_headers(), timeout=10)
        r.raise_for_status()
        candles = r.json()
        if not candles or len(candles) < 5:
            print(f"  ⚠️  {BTCDOM_TICKER} data tidak cukup ({len(candles) if candles else 0} bar) → BTC.D dianggap RANGING")
            _btcd_cache = {"df": None, "ts": now}
            return None
        df = pd.DataFrame(candles, columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "qav", "num_trades", "taker_buy_base", "taker_buy_quote", "ignore"
        ])
        df = df[["timestamp", "open", "high", "low", "close", "volume"]].astype(float)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        _btcd_cache = {"df": df, "ts": now}
        return df
    except Exception as e:
        print(f"  ⚠️  {BTCDOM_TICKER} fetch error ({tf}): {e} → BTC.D dianggap RANGING")
        _btcd_cache = {"df": None, "ts": now}
        return None


# Alias untuk kompatibilitas kode lama (dipanggil di beberapa tempat)
def fetch_btc_dominance_series() -> Optional[pd.DataFrame]:
    """Wrapper — return DataFrame BTCDOM (bukan list CoinGecko seperti versi lama)."""
    return fetch_btcd_ohlcv(tf=BTCDOM_LTF_TF, limit=80)


def get_btcd_trend(df_btcd: Optional[pd.DataFrame]) -> str:
    """
    Deteksi trend BTCD HTF dari DataFrame OHLCV BTCDOMUSDT.
    Gunakan EMA perbandingan: EMA20 vs EMA50 di close.
    Return: 'RISING', 'FALLING', atau 'FLAT'
    """
    if df_btcd is None or len(df_btcd) < BTCD_SMA_PERIOD * 2:
        return "FLAT"
    closes   = df_btcd["close"].values
    sma_now  = float(closes[-BTCD_SMA_PERIOD:].mean())
    sma_prev = float(closes[-BTCD_SMA_PERIOD * 2:-BTCD_SMA_PERIOD].mean())
    diff_pct = (sma_now - sma_prev) / (sma_prev + 1e-9) * 100
    if diff_pct > 0.3:  return "RISING"
    if diff_pct < -0.3: return "FALLING"
    return "FLAT"


# ═══════════════════════════════════════════════════════════════════════════
# ██  STOCHASTIC RSI ENGINE — Setting 5,3,3 (Digunakan untuk BTC Multi-TF)
# ═══════════════════════════════════════════════════════════════════════════

def calc_stoch_rsi(df: pd.DataFrame, rsi_len: int = 5, stoch_len: int = 5,
                   k_smooth: int = 3, d_smooth: int = 3) -> tuple:
    """
    Hitung Stochastic RSI dengan setting 5,3,3.
    Return: (k_line: float, d_line: float)
      k < 20   = Oversold zone
      k > 80   = Overbought zone
    """
    if df is None or len(df) < rsi_len + stoch_len + k_smooth + d_smooth + 5:
        return 50.0, 50.0

    closes = df["close"].astype(float)

    # ── Hitung RSI ─────────────────────────────────────────────────────────────
    delta  = closes.diff()
    gain   = delta.clip(lower=0)
    loss   = -delta.clip(upper=0)
    avg_g  = gain.ewm(com=rsi_len - 1, min_periods=rsi_len).mean()
    avg_l  = loss.ewm(com=rsi_len - 1, min_periods=rsi_len).mean()
    rs     = avg_g / avg_l.replace(0, 1e-9)
    rsi    = 100 - (100 / (1 + rs))

    # ── Hitung Stochastic dari RSI ─────────────────────────────────────────────
    rsi_low  = rsi.rolling(stoch_len).min()
    rsi_high = rsi.rolling(stoch_len).max()
    raw_k    = 100 * (rsi - rsi_low) / (rsi_high - rsi_low + 1e-9)

    # Smoothing K dan D
    k_line   = raw_k.rolling(k_smooth).mean()
    d_line   = k_line.rolling(d_smooth).mean()

    k_val = float(k_line.iloc[-1]) if not k_line.empty else 50.0
    d_val = float(d_line.iloc[-1]) if not d_line.empty else 50.0

    if pd.isna(k_val): k_val = 50.0
    if pd.isna(d_val): d_val = 50.0

    return round(k_val, 2), round(d_val, 2)


def get_stoch_rsi_state(k: float, d: float) -> str:
    """
    Terjemahkan nilai Stochastic RSI menjadi state kondisi pasar.
    Return: 'OVERBOUGHT' | 'OVERSOLD' | 'NEUTRAL' | 'CROSSING_UP' | 'CROSSING_DOWN'
    """
    if k > 80 and d > 80:
        return "OVERBOUGHT"
    if k < 20 and d < 20:
        return "OVERSOLD"
    if k > 80 and k > d and d <= 80:
        return "OVERBOUGHT"
    if k < 20 and k < d and d >= 20:
        return "OVERSOLD"
    # Crossing patterns
    if k > d and k < 50:
        return "CROSSING_UP"
    if k < d and k > 50:
        return "CROSSING_DOWN"
    return "NEUTRAL"


# ═══════════════════════════════════════════════════════════════════════════
# ██  DIVERGENCE DETECTOR — Bullish & Bearish untuk Konfirmasi Entry
# ═══════════════════════════════════════════════════════════════════════════

def detect_divergence(df: pd.DataFrame, rsi_len: int = 5, stoch_len: int = 5,
                       k_smooth: int = 3, d_smooth: int = 3,
                       lookback: int = 30) -> str:
    """
    Deteksi Bullish / Bearish Divergence menggunakan Stochastic RSI (5,3,3).

    Bullish Divergence: Harga buat Lower Low, Stoch RSI buat Higher Low
      → sinyal potensi reversal naik (bagus untuk konfirmasi LONG)

    Bearish Divergence: Harga buat Higher High, Stoch RSI buat Lower High
      → sinyal potensi reversal turun (bagus untuk konfirmasi SHORT)

    Return: 'BULLISH_DIV' | 'BEARISH_DIV' | 'NONE'
    """
    if df is None or len(df) < lookback + 10:
        return "NONE"

    closes = df["close"].astype(float)
    highs  = df["high"].astype(float)
    lows   = df["low"].astype(float)

    # Hitung Stoch RSI untuk seluruh window
    delta  = closes.diff()
    gain   = delta.clip(lower=0)
    loss   = -delta.clip(upper=0)
    avg_g  = gain.ewm(com=rsi_len - 1, min_periods=rsi_len).mean()
    avg_l  = loss.ewm(com=rsi_len - 1, min_periods=rsi_len).mean()
    rs     = avg_g / avg_l.replace(0, 1e-9)
    rsi    = 100 - (100 / (1 + rs))

    rsi_low  = rsi.rolling(stoch_len).min()
    rsi_high = rsi.rolling(stoch_len).max()
    raw_k    = 100 * (rsi - rsi_low) / (rsi_high - rsi_low + 1e-9)
    k_line   = raw_k.rolling(k_smooth).mean()

    recent_df = df.iloc[-lookback:]
    recent_k  = k_line.iloc[-lookback:]

    # Cari swing lows (untuk bullish div) dan swing highs (untuk bearish div)
    price_lows  = []
    price_highs = []
    k_lows      = []
    k_highs     = []

    window_sw = 3   # kecil untuk LTF, cukup untuk swing pivot
    prices_l  = recent_df["low"].values
    prices_h  = recent_df["high"].values
    k_vals    = recent_k.values

    for i in range(window_sw, len(prices_l) - window_sw):
        # Swing low harga
        if prices_l[i] == min(prices_l[i - window_sw: i + window_sw + 1]):
            price_lows.append((i, prices_l[i]))
        # Swing high harga
        if prices_h[i] == max(prices_h[i - window_sw: i + window_sw + 1]):
            price_highs.append((i, prices_h[i]))
        # Swing low stoch RSI
        if not pd.isna(k_vals[i]):
            if k_vals[i] == min([v for v in k_vals[i - window_sw: i + window_sw + 1] if not pd.isna(v)] or [k_vals[i]]):
                k_lows.append((i, k_vals[i]))
            if k_vals[i] == max([v for v in k_vals[i - window_sw: i + window_sw + 1] if not pd.isna(v)] or [k_vals[i]]):
                k_highs.append((i, k_vals[i]))

    # ── Cek Bullish Divergence: Harga LL, Stoch RSI HL ───────────────────────
    if len(price_lows) >= 2 and len(k_lows) >= 2:
        # Bandingkan dua swing low terakhir
        pl1_i, pl1_p = price_lows[-2]
        pl2_i, pl2_p = price_lows[-1]
        # Cari k_low terdekat dengan setiap swing low harga
        kl1 = min(k_lows, key=lambda x: abs(x[0] - pl1_i), default=None)
        kl2 = min(k_lows, key=lambda x: abs(x[0] - pl2_i), default=None)
        if kl1 and kl2:
            price_ll = pl2_p < pl1_p    # harga: Lower Low
            stoch_hl = kl2[1] > kl1[1]  # stoch: Higher Low
            if price_ll and stoch_hl and kl1[1] < 40:   # konfirmasi: stoch dari zona rendah
                return "BULLISH_DIV"

    # ── Cek Bearish Divergence: Harga HH, Stoch RSI LH ──────────────────────
    if len(price_highs) >= 2 and len(k_highs) >= 2:
        ph1_i, ph1_p = price_highs[-2]
        ph2_i, ph2_p = price_highs[-1]
        kh1 = min(k_highs, key=lambda x: abs(x[0] - ph1_i), default=None)
        kh2 = min(k_highs, key=lambda x: abs(x[0] - ph2_i), default=None)
        if kh1 and kh2:
            price_hh = ph2_p > ph1_p    # harga: Higher High
            stoch_lh = kh2[1] < kh1[1]  # stoch: Lower High
            if price_hh and stoch_lh and kh1[1] > 60:   # konfirmasi: stoch dari zona tinggi
                return "BEARISH_DIV"

    return "NONE"


# ═══════════════════════════════════════════════════════════════════════════
# ██  BTC MULTI-TF ANALYSIS — Daily → H4 → H1
#     Strategy: Analisa Daily dulu, lalu konfirmasi H4, entry di H1
#     Stochastic RSI 5,3,3 dipakai di setiap TF
#     Jika BTC RANGING di Daily → tidak ada posisi sama sekali
# ═══════════════════════════════════════════════════════════════════════════

# Cache hasil analisa BTC agar tidak re-fetch setiap pair
_btc_multitf_cache: dict = {"result": None, "ts": 0.0}
_BTC_MULTITF_CACHE_TTL = 120   # 2 menit cache

@dataclass
class BtcMultiTfResult:
    """Hasil analisa BTC multi-timeframe."""
    bias: str           # 'BULLISH' | 'BEARISH' | 'RANGING'
    daily_bias: str     # bias dari Daily candle
    h4_bias: str        # bias dari H4 candle
    h1_bias: str        # bias dari H1 candle
    daily_stoch_k: float
    daily_stoch_d: float
    daily_stoch_state: str   # OVERBOUGHT / OVERSOLD / NEUTRAL / CROSSING_UP / CROSSING_DOWN
    h4_stoch_k: float
    h4_stoch_d: float
    h4_stoch_state: str
    h1_stoch_k: float
    h1_stoch_d: float
    h1_stoch_state: str
    h1_divergence: str   # BULLISH_DIV / BEARISH_DIV / NONE
    h4_divergence: str
    setup_valid: bool    # True = ada setup BTC yang jelas (bukan ranging)
    reason: str          # penjelasan singkat untuk log/Telegram
    allow_long: bool     # boleh ambil LONG alt
    allow_short: bool    # boleh ambil SHORT alt


def analyze_btc_multitf() -> BtcMultiTfResult:
    """
    Analisa BTC secara bertingkat: Daily → H4 → H1.

    LOGIKA HIERARKI:
    ─────────────────────────────────────────────────────
    1. DAILY: Tentukan bias utama (BULLISH / BEARISH / RANGING)
       - Pakai detect_structure + Stochastic RSI (5,3,3)
       - Jika Daily RANGING → setup_valid=False → skip semua posisi
       - Jika Daily OVERBOUGHT + BEARISH → konfirmasi short lebih kuat
       - Jika Daily OVERSOLD + BULLISH → konfirmasi long lebih kuat

    2. H4: Konfirmasi arah Daily
       - Jika H4 sejalan dengan Daily → bias dikonfirmasi
       - Jika H4 berlawanan → bias lemah (tapi tidak di-skip, hanya noted)
       - Cek divergence H4 untuk konfirmasi reversal

    3. H1: Cari entry timing
       - Cek divergence H1 (paling penting untuk entry LTF)
       - Stoch RSI H1 yang oversold = timing entry LONG
       - Stoch RSI H1 yang overbought = timing entry SHORT
    ─────────────────────────────────────────────────────
    """
    global _btc_multitf_cache

    now_ts = time.time()
    if (
        _btc_multitf_cache["result"] is not None
        and (now_ts - _btc_multitf_cache["ts"]) < _BTC_MULTITF_CACHE_TTL
    ):
        return _btc_multitf_cache["result"]

    # ── Default fallback result ────────────────────────────────────────────────
    def _fallback(reason: str) -> BtcMultiTfResult:
        return BtcMultiTfResult(
            bias="RANGING", daily_bias="RANGING", h4_bias="RANGING", h1_bias="RANGING",
            daily_stoch_k=50.0, daily_stoch_d=50.0, daily_stoch_state="NEUTRAL",
            h4_stoch_k=50.0, h4_stoch_d=50.0, h4_stoch_state="NEUTRAL",
            h1_stoch_k=50.0, h1_stoch_d=50.0, h1_stoch_state="NEUTRAL",
            h1_divergence="NONE", h4_divergence="NONE",
            setup_valid=False, reason=reason, allow_long=False, allow_short=False,
        )

    try:
        # ── STEP 1: Fetch data BTC semua TF ──────────────────────────────────────
        df_daily = fetch_ohlcv("BTC/USDT", "1d",  limit=200)
        df_h4    = fetch_ohlcv("BTC/USDT", "4h",  limit=200)
        df_h1    = fetch_ohlcv("BTC/USDT", "1h",  limit=100)

        if df_daily is None or len(df_daily) < 50:
            return _fallback("BTC Daily data tidak cukup")
        if df_h4 is None or len(df_h4) < 50:
            return _fallback("BTC H4 data tidak cukup")

        # ── STEP 2: Analisa Daily ──────────────────────────────────────────────
        daily_bias, _, _, _ = detect_structure(df_daily)
        d_k, d_d  = calc_stoch_rsi(df_daily, rsi_len=5, stoch_len=5, k_smooth=3, d_smooth=3)
        d_state   = get_stoch_rsi_state(d_k, d_d)

        # ── STEP 3: Analisa H4 ────────────────────────────────────────────────
        h4_bias, _, _, _ = detect_structure(df_h4)
        h4_k, h4_d = calc_stoch_rsi(df_h4, rsi_len=5, stoch_len=5, k_smooth=3, d_smooth=3)
        h4_state   = get_stoch_rsi_state(h4_k, h4_d)
        h4_div     = detect_divergence(df_h4, lookback=40)

        # ── STEP 4: Analisa H1 ────────────────────────────────────────────────
        h1_bias, _, _, _ = detect_structure(df_h1) if df_h1 is not None and len(df_h1) >= 30 else ("RANGING", None, None, None)
        h1_k, h1_d = calc_stoch_rsi(df_h1, rsi_len=5, stoch_len=5, k_smooth=3, d_smooth=3) if df_h1 is not None else (50.0, 50.0)
        h1_state   = get_stoch_rsi_state(h1_k, h1_d)
        h1_div     = detect_divergence(df_h1, lookback=30) if df_h1 is not None else "NONE"

        # ── STEP 5: Tentukan Bias Final & Izin Trading ────────────────────────
        # RULE: Daily RANGING → tidak ada setup → stop semua posisi
        if daily_bias == "RANGING":
            result = BtcMultiTfResult(
                bias="RANGING", daily_bias=daily_bias, h4_bias=h4_bias, h1_bias=h1_bias,
                daily_stoch_k=d_k, daily_stoch_d=d_d, daily_stoch_state=d_state,
                h4_stoch_k=h4_k, h4_stoch_d=h4_d, h4_stoch_state=h4_state,
                h1_stoch_k=h1_k, h1_stoch_d=h1_d, h1_stoch_state=h1_state,
                h1_divergence=h1_div, h4_divergence=h4_div,
                setup_valid=False,
                reason=(
                    f"🔀 BTC Daily RANGING — tidak ada setup jelas. "
                    f"Stoch Daily K={d_k:.1f}/D={d_d:.1f} ({d_state}). "
                    f"Semua posisi DITUNDA sampai BTC punya arah."
                ),
                allow_long=False, allow_short=False,
            )
            _btc_multitf_cache = {"result": result, "ts": now_ts}
            return result

        # Daily punya bias (BULLISH atau BEARISH)
        final_bias  = daily_bias
        allow_long  = False
        allow_short = False
        reasons     = []

        # ── LONG condition ────────────────────────────────────────────────────
        if daily_bias == "BULLISH":
            allow_long = True   # BTC Daily BULLISH → Long alt diizinkan
            reasons.append(f"✅ BTC Daily BULLISH → Long alt OK")

            # H4 konfirmasi
            if h4_bias == "BULLISH":
                reasons.append(f"✅ H4 konfirmasi BULLISH")
            elif h4_bias == "RANGING":
                reasons.append(f"⚠️ H4 RANGING — Long masih OK tapi kurang ideal")
            else:
                reasons.append(f"⚠️ H4 counter-BEARISH — hati-hati, Long tetap boleh tapi score kurang")

            # Stoch Daily OVERBOUGHT → short mungkin lebih aman dari long
            if d_state == "OVERBOUGHT":
                reasons.append(f"⚠️ Daily Stoch OVERBOUGHT (K={d_k:.1f}) — momentum bisa lemah")
            elif d_state == "OVERSOLD":
                reasons.append(f"✅ Daily Stoch OVERSOLD (K={d_k:.1f}) — momentum bullish fresh")

            # H1 Divergence: Bullish Div di H1 = timing entry Long ideal
            if h1_div == "BULLISH_DIV":
                reasons.append(f"✅ H1 Bullish Divergence — timing entry LONG sangat baik")
            elif h1_div == "BEARISH_DIV":
                reasons.append(f"⚠️ H1 Bearish Divergence — kurangi size atau tunda Long")

            # H4 Bearish Divergence saat Daily bullish = waspada long
            if h4_div == "BEARISH_DIV":
                reasons.append(f"⚠️ H4 Bearish Divergence — pertimbangkan tunda Long")

            # H1 Stoch oversold = timing entry ideal
            if h1_state == "OVERSOLD":
                reasons.append(f"✅ H1 Stoch OVERSOLD (K={h1_k:.1f}) — pullback selesai, entry timing OK")

        # ── SHORT condition ───────────────────────────────────────────────────
        elif daily_bias == "BEARISH":
            allow_short = True   # BTC Daily BEARISH → Short alt diizinkan
            reasons.append(f"✅ BTC Daily BEARISH → Short alt OK")

            if h4_bias == "BEARISH":
                reasons.append(f"✅ H4 konfirmasi BEARISH")
            elif h4_bias == "RANGING":
                reasons.append(f"⚠️ H4 RANGING — Short masih OK tapi kurang ideal")
            else:
                reasons.append(f"⚠️ H4 counter-BULLISH — hati-hati, Short tetap boleh tapi score kurang")

            if d_state == "OVERSOLD":
                reasons.append(f"⚠️ Daily Stoch OVERSOLD (K={d_k:.1f}) — momentum bear bisa lemah")
            elif d_state == "OVERBOUGHT":
                reasons.append(f"✅ Daily Stoch OVERBOUGHT (K={d_k:.1f}) — momentum bearish fresh")

            if h1_div == "BEARISH_DIV":
                reasons.append(f"✅ H1 Bearish Divergence — timing entry SHORT sangat baik")
            elif h1_div == "BULLISH_DIV":
                reasons.append(f"⚠️ H1 Bullish Divergence — kurangi size atau tunda Short")

            if h4_div == "BULLISH_DIV":
                reasons.append(f"⚠️ H4 Bullish Divergence — pertimbangkan tunda Short")

            if h1_state == "OVERBOUGHT":
                reasons.append(f"✅ H1 Stoch OVERBOUGHT (K={h1_k:.1f}) — pullback selesai, entry timing OK")

        reason_str = " | ".join(reasons)

        result = BtcMultiTfResult(
            bias=final_bias, daily_bias=daily_bias, h4_bias=h4_bias, h1_bias=h1_bias,
            daily_stoch_k=d_k, daily_stoch_d=d_d, daily_stoch_state=d_state,
            h4_stoch_k=h4_k, h4_stoch_d=h4_d, h4_stoch_state=h4_state,
            h1_stoch_k=h1_k, h1_stoch_d=h1_d, h1_stoch_state=h1_state,
            h1_divergence=h1_div, h4_divergence=h4_div,
            setup_valid=True, reason=reason_str,
            allow_long=allow_long, allow_short=allow_short,
        )
        _btc_multitf_cache = {"result": result, "ts": now_ts}
        return result

    except Exception as e:
        print(f"  ❌ analyze_btc_multitf error: {e}")
        return _fallback(f"Error analisa BTC: {e}")


def get_btc_bias() -> str:
    """
    Wrapper kompatibilitas: return string bias BTC dari analisa multi-TF baru.
    Digunakan oleh kode lama yang memanggil get_btc_bias().
    """
    try:
        result = analyze_btc_multitf()
        return result.bias
    except Exception:
        return "RANGING"


def macro_score(pair: str, direction: str, btc_bias: str, btcd_trend: str) -> tuple:
    if pair in ALTCOIN_EXEMPTIONS:
        return 0, "BTC/ETH exempt"
    if btc_bias == "RANGING" or btcd_trend == "FLAT":
        return 0, "Macro data insufficient"

    if direction == "BULLISH":
        if btc_bias == "BULLISH" and btcd_trend == "FALLING":
            return SCORE_MACRO_ALIGNED, "BTC Bull + BTC.D↓ → Alt season ✅"
        if btc_bias == "BEARISH" and btcd_trend == "RISING":
            return SCORE_MACRO_CONFLICT, "BTC Bear + BTC.D↑ → Alts bleeding ⚠️"
        if btc_bias == "BULLISH" and btcd_trend == "RISING":
            return SCORE_MACRO_CONFLICT // 2, "BTC Bull + BTC.D↑ → BTC season"
        return 0, "Macro neutral"

    if direction == "BEARISH":
        if btc_bias == "BEARISH" and btcd_trend == "RISING":
            return SCORE_MACRO_ALIGNED, "BTC Bear + BTC.D↑ → Alt bleed ✅"
        if btc_bias == "BULLISH" and btcd_trend == "FALLING":
            return SCORE_MACRO_CONFLICT, "BTC Bull + BTC.D↓ → Alt season, avoid SHORT ⚠️"
        if btc_bias == "BEARISH" and btcd_trend == "FALLING":
            return SCORE_MACRO_CONFLICT // 2, "BTC Bear + BTC.D↓ → Alts may bounce"
        return 0, "Macro neutral"

    return 0, "No match"


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 11b — BTC/BTCD LOW TIMEFRAME CORRELATION FILTER
# ═══════════════════════════════════════════════════════════════════════════

# Cache BTC LTF data agar tidak re-fetch setiap pair
_btc_ltf_cache: dict = {"data": None, "ts": 0.0, "tf": None}

# Timeframe yang digunakan untuk analisis LTF BTC/BTCD
BTC_LTF_TF = "15m"          # timeframe LTF untuk BTC price
BTCD_LTF_CANDLES = 20       # jumlah periode untuk deteksi trend BTCD LTF
BTC_LTF_CANDLES  = 20       # jumlah periode untuk deteksi trend BTC LTF
BTC_LTF_CACHE_TTL = 60      # cache TTL detik (1 menit — sinkron dengan scan)

# Skor filter sinyal dari korelasi BTC/BTCD LTF
# Positif = korelasi mendukung sinyal → bonus score
# Negatif = korelasi berlawanan → penalty atau blok
BTCD_CORR_BONUS   = 12    # kondisi ideal → bonus skor
BTCD_CORR_NEUTRAL = 0     # kondisi netral → tidak ada efek
BTCD_CORR_PENALTY = -15   # kondisi berlawanan → penalty skor
BTCD_CORR_BLOCK   = -999  # kondisi sangat berlawanan → blok sinyal


def fetch_btc_ltf_data(tf: str = BTC_LTF_TF, limit: int = 60) -> Optional[pd.DataFrame]:
    """
    Ambil OHLCV BTC/USDT untuk low timeframe dengan cache 60 detik.
    Digunakan untuk analisis BTC trend jangka pendek.
    """
    global _btc_ltf_cache
    now = time.time()
    if (
        _btc_ltf_cache["data"] is not None
        and _btc_ltf_cache["tf"] == tf
        and (now - _btc_ltf_cache["ts"]) < BTC_LTF_CACHE_TTL
    ):
        return _btc_ltf_cache["data"]
    try:
        df = fetch_ohlcv("BTC/USDT", tf, limit=limit)
        _btc_ltf_cache = {"data": df, "ts": now, "tf": tf}
        return df
    except Exception as e:
        print(f"  ⚠️  BTC LTF fetch error ({tf}): {e}")
        return None


def get_btc_ltf_direction(df_btc_ltf: Optional[pd.DataFrame], period: int = BTC_LTF_CANDLES) -> str:
    """
    Deteksi arah BTC LTF berdasarkan:
    - EMA slope (EMA cepat vs EMA lambat)
    - Candle momentum (N candle terakhir)
    Return: 'BULLISH', 'BEARISH', atau 'FLAT'
    """
    if df_btc_ltf is None or len(df_btc_ltf) < period + 5:
        return "FLAT"

    closes = df_btc_ltf["close"].values
    # EMA cepat (7) vs EMA lambat (20)
    def ema_calc(data, n):
        k = 2 / (n + 1)
        e = data[0]
        for v in data[1:]:
            e = v * k + e * (1 - k)
        return e

    recent = closes[-(period + 5):]
    ema_fast = ema_calc(recent, 7)
    ema_slow = ema_calc(recent, 20)

    # Momentum: berapa candle dari 5 terakhir yang searah
    last5 = df_btc_ltf.iloc[-5:]
    bull_count = sum(1 for _, r in last5.iterrows() if float(r["close"]) > float(r["open"]))
    bear_count = 5 - bull_count

    # Kombinasi EMA slope + momentum
    if ema_fast > ema_slow * 1.0005 and bull_count >= 3:
        return "BULLISH"
    if ema_fast < ema_slow * 0.9995 and bear_count >= 3:
        return "BEARISH"
    return "FLAT"


def get_btcd_ltf_direction(df_btcd: Optional[pd.DataFrame], period: int = BTCD_LTF_CANDLES) -> str:
    """
    Deteksi arah BTCD LTF dari DataFrame OHLCV BTCDOMUSDT (Binance 15m).
    Sama persis dengan get_btc_ltf_direction — pakai EMA slope + momentum candle.
    Return: 'RISING', 'FALLING', atau 'FLAT'
    """
    if df_btcd is None or len(df_btcd) < period + 5:
        return "FLAT"

    closes = df_btcd["close"].values

    def ema_calc(data, n):
        k = 2 / (n + 1)
        e = data[0]
        for v in data[1:]:
            e = v * k + e * (1 - k)
        return e

    recent   = closes[-(period + 5):]
    ema_fast = ema_calc(recent, 7)
    ema_slow = ema_calc(recent, 20)

    last5      = df_btcd.iloc[-5:]
    bull_count = sum(1 for _, r in last5.iterrows() if float(r["close"]) > float(r["open"]))
    bear_count = 5 - bull_count

    if ema_fast > ema_slow * 1.0005 and bull_count >= 3:
        return "RISING"
    if ema_fast < ema_slow * 0.9995 and bear_count >= 3:
        return "FALLING"
    return "FLAT"



def analyze_btc_situation(
    df_btc_ltf: Optional[pd.DataFrame],
    df_btc_htf: Optional[pd.DataFrame],
    btc_bias_htf: str,
    btcd_ltf_dir: str,
    btcd_trend_htf: str,
) -> dict:
    """
    ═══════════════════════════════════════════════════════════════════════
    BTC SITUATIONAL AWARENESS — Smart Macro Context untuk Alt Signals
    ═══════════════════════════════════════════════════════════════════════

    Tujuan: Deteksi kondisi BTC yang "menjebak" sinyal alt — terutama:

    KASUS UTAMA yang ditangani:
    1. BTC NEAR DEMAND + BEARISH LTF → BTC mau bounce → SHORT alt berbahaya
       (alt SHORT bisa ketarik naik karena BTC rebound dari demand)

    2. BTC NEAR SUPPLY + BULLISH LTF → BTC mau reject → LONG alt berbahaya
       (alt LONG bisa tertekan karena BTC gagal di supply)

    3. BTC EXTENDED (jauh dari structure) → momentum lemah → semua sinyal
       harus lebih selektif (bonus score lebih ketat)

    4. BTC STRONG TRENDING → kondisi ideal → bonus score untuk arah searah

    Output dict:
    {
        "situation": str,         # label kondisi: "BTC_NEAR_DEMAND", "BTC_NEAR_SUPPLY",
                                  #   "BTC_TRENDING_BULLISH", "BTC_TRENDING_BEARISH",
                                  #   "BTC_EXTENDED", "BTC_NEUTRAL"
        "score_adj": int,         # bonus/penalty untuk scoring sinyal alt
        "block_short": bool,      # True = blok SHORT alt (BTC mau bounce)
        "block_long":  bool,      # True = blok LONG alt (BTC mau reject)
        "reason": str,            # penjelasan ringkas untuk log
        "warn_short": bool,       # True = SHORT alt berisiko (peringatan, tidak blok)
        "warn_long":  bool,       # True = LONG alt berisiko (peringatan, tidak blok)
    }
    ═══════════════════════════════════════════════════════════════════════
    """
    result = {
        "situation": "BTC_NEUTRAL",
        "score_adj": 0,
        "block_short": False,
        "block_long":  False,
        "warn_short":  False,
        "warn_long":   False,
        "reason": "BTC situasi netral",
    }

    if df_btc_ltf is None or len(df_btc_ltf) < 30:
        result["reason"] = "BTC LTF data tidak cukup — skip situational check"
        return result

    # ── Ambil data harga BTC ───────────────────────────────────────────────────
    current_price = float(df_btc_ltf["close"].iloc[-1])

    # ── Deteksi swing high/low BTC HTF (demand/supply zone proxy) ─────────────
    df_for_swing = df_btc_htf if (df_btc_htf is not None and len(df_btc_htf) >= 30) else df_btc_ltf
    htf_highs, htf_lows = find_swings(df_for_swing, window=5)

    # Nearest swing low (demand proxy) dan swing high (supply proxy)
    recent_lows  = sorted([p for _, p in htf_lows],  reverse=False)  # ascending
    recent_highs = sorted([p for _, p in htf_highs], reverse=True)   # descending

    nearest_demand = recent_lows[-1]  if recent_lows  else None  # highest recent low = demand
    nearest_supply = recent_highs[-1] if recent_highs else None  # lowest recent high = supply

    # ── ATR BTC untuk ukur "dekat" ────────────────────────────────────────────
    btc_atr = calculate_atr(df_btc_ltf, period=14)
    proximity_factor = 1.5   # dalam 1.5× ATR = "dekat" zona

    near_demand = (
        nearest_demand is not None
        and current_price <= nearest_demand + btc_atr * proximity_factor
        and current_price >= nearest_demand - btc_atr * 0.5
    )
    near_supply = (
        nearest_supply is not None
        and current_price >= nearest_supply - btc_atr * proximity_factor
        and current_price <= nearest_supply + btc_atr * 0.5
    )

    # ── BTC LTF momentum ──────────────────────────────────────────────────────
    btc_ltf_dir = get_btc_ltf_direction(df_btc_ltf)

    # ─────────────────────────────────────────────────────────────────────────
    # KASUS 1: BTC dekat DEMAND + LTF mulai berbalik / masih turun
    # → BTC mau bounce → SHORT alt sangat berbahaya
    # ─────────────────────────────────────────────────────────────────────────
    if near_demand and btc_bias_htf == "BEARISH":
        if btc_ltf_dir in ("BULLISH", "FLAT"):
            # LTF sudah mulai balik → bounce hampir terjadi → BLOK SHORT
            result.update({
                "situation": "BTC_NEAR_DEMAND_BOUNCE",
                "score_adj": -20,
                "block_short": True,
                "warn_long":   False,
                "reason": (
                    f"⚠️  BTC HTF BEARISH tapi sudah di demand zone "
                    f"(~{nearest_demand:.0f}) + LTF {btc_ltf_dir} → bounce imminent "
                    f"→ SHORT alt DIBLOK (ketarik naik)"
                ),
            })
            return result
        else:
            # LTF masih turun tapi harga sudah di demand → waspada SHORT
            result.update({
                "situation": "BTC_NEAR_DEMAND_FALLING",
                "score_adj": -10,
                "warn_short": True,
                "reason": (
                    f"⚠️  BTC di demand zone (~{nearest_demand:.0f}) + LTF masih turun "
                    f"→ SHORT alt berisiko, bisa snap back kapan saja"
                ),
            })
            return result

    # ─────────────────────────────────────────────────────────────────────────
    # KASUS 2: BTC dekat SUPPLY + LTF mulai melemah / masih naik
    # → BTC mau reject → LONG alt berbahaya
    # ─────────────────────────────────────────────────────────────────────────
    if near_supply and btc_bias_htf == "BULLISH":
        if btc_ltf_dir in ("BEARISH", "FLAT"):
            # LTF sudah mulai balik → rejection hampir terjadi → BLOK LONG
            result.update({
                "situation": "BTC_NEAR_SUPPLY_REJECT",
                "score_adj": -20,
                "block_long": True,
                "warn_short": False,
                "reason": (
                    f"⚠️  BTC HTF BULLISH tapi sudah di supply zone "
                    f"(~{nearest_supply:.0f}) + LTF {btc_ltf_dir} → rejection imminent "
                    f"→ LONG alt DIBLOK (kena tekanan BTC turun)"
                ),
            })
            return result
        else:
            # LTF masih naik tapi di supply → waspada LONG
            result.update({
                "situation": "BTC_NEAR_SUPPLY_RISING",
                "score_adj": -8,
                "warn_long": True,
                "reason": (
                    f"⚠️  BTC di supply zone (~{nearest_supply:.0f}) + LTF masih naik "
                    f"→ LONG alt berisiko, BTC bisa reject kapan saja"
                ),
            })
            return result

    # ─────────────────────────────────────────────────────────────────────────
    # KASUS 3: BTC STRONG TREND — kondisi ideal (mid-trend, bukan di ekstrem)
    # ─────────────────────────────────────────────────────────────────────────
    if btc_bias_htf == "BULLISH" and btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FALLING":
        result.update({
            "situation": "BTC_TRENDING_BULLISH",
            "score_adj": +8,
            "reason": "✅ BTC HTF BULLISH + LTF naik + BTCD turun → Alt season momentum → bonus LONG",
        })
        return result

    if btc_bias_htf == "BEARISH" and btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "RISING":
        result.update({
            "situation": "BTC_TRENDING_BEARISH",
            "score_adj": +8,
            "reason": "✅ BTC HTF BEARISH + LTF turun + BTCD naik → Alt bleed momentum → bonus SHORT",
        })
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # KASUS 4: BTC EXTENDED — harga jauh dari struktur, reversion risk tinggi
    # Heuristik: jika tidak near demand/supply tapi juga tidak trending jelas
    # ─────────────────────────────────────────────────────────────────────────
    if btc_bias_htf == "RANGING" and btc_ltf_dir == "FLAT":
        result.update({
            "situation": "BTC_EXTENDED",
            "score_adj": -5,
            "reason": "⚡ BTC ranging + LTF flat → pasar konsolidasi, sinyal alt lebih berisiko",
        })
        return result

    # Default: netral
    result["reason"] = f"BTC HTF={btc_bias_htf} LTF={btc_ltf_dir} BTCD={btcd_ltf_dir} → situasi netral"
    return result

def analyze_btcd_correlation(
    direction: str,
    btc_ltf_dir: str,
    btcd_ltf_dir: str,
    btc_bias_htf: str,
    btcd_trend_htf: str,
    pair: str,
) -> tuple:
    """
    Analisis korelasi BTC + BTC Dominance untuk menentukan apakah sinyal
    layak dikirim berdasarkan kondisi makro LTF.

    LOGIKA UTAMA:
    ─────────────────────────────────────────────────────────────────
    LONG (alt coin naik):
      ✅ IDEAL      : BTC LTF naik  + BTCD LTF turun  → Alt season  → +bonus
      ✅ OK         : BTC LTF naik  + BTCD LTF flat   → BTC rally, neutral alt
      ⚠️ WASPADA   : BTC LTF flat  + BTCD LTF turun  → Mungkin alt season
      ❌ KONFLIK    : BTC LTF turun + BTCD LTF naik   → Alts bleeding → penalty
      🚫 BLOK      : BTC LTF turun + BTCD LTF naik (keduanya kuat) → blok

    SHORT (alt coin turun):
      ✅ IDEAL      : BTC LTF turun + BTCD LTF naik   → Alt bleed   → +bonus
      ✅ OK         : BTC LTF turun + BTCD LTF flat   → BTC dump, alts ikut
      ⚠️ WASPADA   : BTC LTF flat  + BTCD LTF naik   → Alt bleed mungkin
      ❌ KONFLIK    : BTC LTF naik  + BTCD LTF turun  → Alt season  → penalty
      🚫 BLOK      : BTC LTF naik  + BTCD LTF turun (keduanya kuat) → blok

    BTC/ETH: hanya cek BTC LTF (tidak terpengaruh BTCD)
    ─────────────────────────────────────────────────────────────────
    Return: (score_adj: int, reason: str, signal_blocked: bool)
    """
    # BTC/ETH: korelasi BTCD tidak relevan, hanya cek momentum LTF
    if pair in ALTCOIN_EXEMPTIONS:
        if direction == "BULLISH" and btc_ltf_dir == "BULLISH":
            return BTCD_CORR_BONUS, "BTC LTF↑ — momentum konfirmasi LONG ✅", False
        if direction == "BEARISH" and btc_ltf_dir == "BEARISH":
            return BTCD_CORR_BONUS, "BTC LTF↓ — momentum konfirmasi SHORT ✅", False
        if btc_ltf_dir == "FLAT":
            return BTCD_CORR_NEUTRAL, "BTC LTF flat — netral", False
        # Berlawanan
        dir_str = "LONG" if direction == "BULLISH" else "SHORT"
        return BTCD_CORR_PENALTY, f"BTC LTF berlawanan arah {dir_str} ⚠️", False

    # Altcoin: cek kombinasi BTC LTF + BTCD LTF
    if direction == "BULLISH":
        # ── KONDISI IDEAL untuk LONG alt ──────────────────────────────────────
        if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FALLING":
            return BTCD_CORR_BONUS, "BTC LTF↑ + BTCD LTF↓ → Alt season LTF ✅ IDEAL untuk LONG", False

        # ── Kondisi OK ────────────────────────────────────────────────────────
        if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_NEUTRAL, "BTC LTF↑ + BTCD flat → BTC rally, alt netral ✅", False
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "FALLING":
            return BTCD_CORR_NEUTRAL, "BTC flat + BTCD LTF↓ → Mungkin alt season ⚡", False

        # ── Kondisi netral / ambigu ───────────────────────────────────────────
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_NEUTRAL, "BTC flat + BTCD flat → Pasar netral", False
        if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "RISING":
            return BTCD_CORR_NEUTRAL // 2 if BTCD_CORR_NEUTRAL != 0 else 0, \
                   "BTC LTF↑ + BTCD LTF↑ → BTC season, alt tertinggal ⚠️", False

        # ── Kondisi berbahaya untuk LONG ──────────────────────────────────────
        if btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_PENALTY // 2, "BTC LTF↓ + BTCD flat → Risiko LONG meningkat ⚠️", False
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "RISING":
            return BTCD_CORR_PENALTY // 2, "BTC flat + BTCD LTF↑ → Alts mulai bleeding ⚠️", False

        # ── BLOK: kondisi paling berbahaya untuk LONG ─────────────────────────
        if btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "RISING":
            return BTCD_CORR_BLOCK, "🚫 BTC LTF↓ + BTCD LTF↑ → Alts BLEEDING — LONG DIBLOK", True

        return BTCD_CORR_NEUTRAL, "Korelasi BTC/BTCD netral", False

    if direction == "BEARISH":
        # ── KONDISI IDEAL untuk SHORT alt ─────────────────────────────────────
        if btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "RISING":
            return BTCD_CORR_BONUS, "BTC LTF↓ + BTCD LTF↑ → Alt bleed LTF ✅ IDEAL untuk SHORT", False

        # ── Kondisi OK ────────────────────────────────────────────────────────
        if btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_NEUTRAL, "BTC LTF↓ + BTCD flat → BTC dump, alt ikut ✅", False
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "RISING":
            return BTCD_CORR_NEUTRAL, "BTC flat + BTCD LTF↑ → Alt bleed mungkin ⚡", False

        # ── Kondisi netral / ambigu ───────────────────────────────────────────
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_NEUTRAL, "BTC flat + BTCD flat → Pasar netral", False
        if btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "FALLING":
            return BTCD_CORR_NEUTRAL, "BTC LTF↓ + BTCD LTF↓ → Alts mungkin bounce ⚠️", False

        # ── Kondisi berbahaya untuk SHORT ─────────────────────────────────────
        if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_PENALTY // 2, "BTC LTF↑ + BTCD flat → Risiko SHORT meningkat ⚠️", False
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "FALLING":
            return BTCD_CORR_PENALTY // 2, "BTC flat + BTCD LTF↓ → Alts mulai recover ⚠️", False

        # ── BLOK: kondisi paling berbahaya untuk SHORT ────────────────────────
        if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FALLING":
            return BTCD_CORR_BLOCK, "🚫 BTC LTF↑ + BTCD LTF↓ → Alt SEASON — SHORT DIBLOK", True

        return BTCD_CORR_NEUTRAL, "Korelasi BTC/BTCD netral", False

    return BTCD_CORR_NEUTRAL, "Direction tidak dikenal", False


def get_btcd_correlation_summary(
    btc_ltf_dir: str,
    btcd_ltf_dir: str,
    btc_bias_htf: str,
    btcd_trend_htf: str,
) -> str:
    """
    Buat ringkasan teks kondisi BTC/BTCD untuk laporan /btcdcorrelation.
    """
    lines = []

    # HTF context
    btc_em  = "📈" if btc_bias_htf == "BULLISH" else ("📉" if btc_bias_htf == "BEARISH" else "➡️")
    btcd_em = "📈" if btcd_trend_htf == "RISING" else ("📉" if btcd_trend_htf == "FALLING" else "➡️")
    lines.append(f"{btc_em} BTC HTF  : <b>{btc_bias_htf}</b>")
    lines.append(f"{btcd_em} BTCD HTF : <b>{btcd_trend_htf}</b>")
    lines.append("─" * 34)

    # LTF context
    btc_ltf_em  = "📈" if btc_ltf_dir == "BULLISH" else ("📉" if btc_ltf_dir == "BEARISH" else "➡️")
    btcd_ltf_em = "📈" if btcd_ltf_dir == "RISING" else ("📉" if btcd_ltf_dir == "FALLING" else "➡️")
    lines.append(f"{btc_ltf_em} BTC LTF  ({BTC_LTF_TF}): <b>{btc_ltf_dir}</b>")
    lines.append(f"{btcd_ltf_em} BTCD LTF ({BTC_LTF_TF}): <b>{btcd_ltf_dir}</b>")
    lines.append("─" * 34)

    # Interpretasi
    if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FALLING":
        lines.append("🟢 <b>ALT SEASON MODE</b>")
        lines.append("   BTC naik, dominance turun → Altcoin pump")
        lines.append("   ✅ LONG alt sangat favorable")
        lines.append("   🚫 SHORT alt diblok")
    elif btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "RISING":
        lines.append("🔴 <b>ALT BLEEDING MODE</b>")
        lines.append("   BTC turun, dominance naik → Altcoin dump")
        lines.append("   ✅ SHORT alt sangat favorable")
        lines.append("   🚫 LONG alt diblok")
    elif btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "RISING":
        lines.append("🟡 <b>BTC SEASON MODE</b>")
        lines.append("   BTC naik, dominance naik → Modal masuk BTC")
        lines.append("   ⚠️ Alt tertinggal — selektif LONG")
        lines.append("   ✅ SHORT alt boleh (relatif underperform)")
    elif btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "FALLING":
        lines.append("🟡 <b>MARKET DUMP MODE</b>")
        lines.append("   BTC turun, dominance turun → Semua turun")
        lines.append("   ⚠️ Alt mungkin bounce relatif terhadap BTC")
        lines.append("   ⚠️ SHORT alt dengan hati-hati")
    elif btc_ltf_dir == "FLAT" and btcd_ltf_dir == "FLAT":
        lines.append("⚪ <b>KONSOLIDASI / NETRAL</b>")
        lines.append("   Tidak ada sinyal kuat dari BTC/BTCD")
        lines.append("   ⚡ Sinyal berdasarkan SMC murni")
    elif btc_ltf_dir == "BULLISH":
        lines.append("🟢 <b>BTC MOMENTUM NAIK</b>")
        lines.append(f"   BTCD {btcd_ltf_dir} — kondisi campur")
        lines.append("   ✅ LONG tetap dipertimbangkan")
    elif btc_ltf_dir == "BEARISH":
        lines.append("🔴 <b>BTC MOMENTUM TURUN</b>")
        lines.append(f"   BTCD {btcd_ltf_dir} — kondisi campur")
        lines.append("   ⚠️ Waspadai posisi LONG alt")
    else:
        lines.append("⚪ <b>KONDISI CAMPURAN</b>")
        lines.append("   Korelasi BTC/BTCD tidak konklusif")

    lines.append("─" * 34)

    # Rekomendasi
    long_ok  = not (btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "RISING")
    short_ok = not (btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FALLING")
    lines.append(f"📋 <b>Rekomendasi Saat Ini:</b>")
    lines.append(f"   LONG  alt : {'✅ OK' if long_ok  else '🚫 DIBLOK'}")
    lines.append(f"   SHORT alt : {'✅ OK' if short_ok else '🚫 DIBLOK'}")

    return "\n".join(lines)


def send_btcd_correlation_report(chat_id: str = None):
    """
    Kirim laporan lengkap kondisi BTC/BTCD LTF + HTF ke Telegram.
    Dipanggil oleh command /btcdcorrelation.
    """
    now_str = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")

    # Fetch data
    try:
        df_btc_ltf = fetch_btc_ltf_data(BTC_LTF_TF, limit=60)
        btc_ltf_dir = get_btc_ltf_direction(df_btc_ltf)
    except Exception as e:
        btc_ltf_dir = "FLAT"
        print(f"  ⚠️  BTC LTF error: {e}")

    try:
        df_btcd      = fetch_btcd_ohlcv(tf=BTCDOM_LTF_TF, limit=80)
        btcd_ltf_dir = get_btcd_ltf_direction(df_btcd)
        # HTF: fetch dengan timeframe lebih besar untuk trend
        df_btcd_htf  = fetch_btcd_ohlcv(tf=BTCDOM_HTF_TF, limit=60)
        btcd_htf_dir = get_btcd_trend(df_btcd_htf)
    except Exception as e:
        btcd_ltf_dir = "FLAT"
        btcd_htf_dir = "FLAT"
        print(f"  ⚠️  BTCD fetch error: {e}")

    try:
        btc_bias_htf = get_btc_bias()
    except Exception:
        btc_bias_htf = "RANGING"

    summary = get_btcd_correlation_summary(btc_ltf_dir, btcd_ltf_dir, btc_bias_htf, btcd_htf_dir)

    # Contoh dampak per skenario
    _, long_reason, long_blocked  = analyze_btcd_correlation(
        "BULLISH", btc_ltf_dir, btcd_ltf_dir, btc_bias_htf, btcd_htf_dir, "SOL/USDT"
    )
    _, short_reason, short_blocked = analyze_btcd_correlation(
        "BEARISH", btc_ltf_dir, btcd_ltf_dir, btc_bias_htf, btcd_htf_dir, "SOL/USDT"
    )

    msg = (
        f"🔗 <b>BTC/BTCD CORRELATION REPORT</b>\n"
        f"📅 {now_str}\n"
        f"{'═' * 34}\n"
        f"{summary}\n"
        f"{'═' * 34}\n"
        f"📊 <b>Dampak ke Sinyal Alt (contoh SOL):</b>\n"
        f"   LONG  : {long_reason}\n"
        f"   SHORT : {short_reason}\n"
        f"{'─' * 34}\n"
        f"ℹ️ Filter ini aktif otomatis di setiap sinyal.\n"
        f"   BTC LTF TF: {BTC_LTF_TF} | BTCD: Binance {BTCDOM_TICKER} {BTCDOM_LTF_TF}"
    )
    _tg_send(msg, chat_id)
    print(f"  🔗 BTC/BTCD correlation report dikirim → {chat_id or TELEGRAM_CHAT_ID}")


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 12 — SESSION
# ═══════════════════════════════════════════════════════════════════════════

def get_session() -> str:
    h = datetime.now(timezone.utc).hour
    if 7  <= h < 13: return "London"
    if 12 <= h < 21: return "New York"
    if 0  <= h < 8:  return "Asia"
    return "Off-Hours"


def session_score(session: str) -> int:
    return SCORE_SESSION if session in ("London", "New York") else 0


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 13 — ATR & RISK-REWARD
# ═══════════════════════════════════════════════════════════════════════════

def calculate_atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> float:
    trs = []
    for i in range(1, len(df)):
        h  = float(df.iloc[i]["high"])
        l  = float(df.iloc[i]["low"])
        pc = float(df.iloc[i - 1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return float(df["high"].iloc[-1] - df["low"].iloc[-1])
    return sum(trs[-period:]) / min(period, len(trs))


def calculate_adx(df: pd.DataFrame, period: int = 14) -> float:
    """
    Hitung ADX (Average Directional Index) untuk mengukur kekuatan trend.
    ADX > 25 = trending kuat (layak trade directional)
    ADX < 20 = ranging / choppy (sinyal directional tidak reliable)

    Implementasi standard Wilder's ADX:
      +DM = selisih high positif | -DM = selisih low negatif
      ATR smoothed → +DI, -DI → DX → ADX (EMA dari DX)
    """
    if len(df) < period * 2 + 1:
        return 25.0   # fallback: anggap trending jika data tidak cukup

    highs  = df["high"].astype(float).values
    lows   = df["low"].astype(float).values
    closes = df["close"].astype(float).values

    tr_list, pdm_list, ndm_list = [], [], []
    for i in range(1, len(df)):
        tr  = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        pdm = max(highs[i] - highs[i-1], 0.0) if (highs[i] - highs[i-1]) > (lows[i-1] - lows[i]) else 0.0
        ndm = max(lows[i-1] - lows[i],   0.0) if (lows[i-1] - lows[i]) > (highs[i] - highs[i-1]) else 0.0
        tr_list.append(tr)
        pdm_list.append(pdm)
        ndm_list.append(ndm)

    def wilder_smooth(data, n):
        """Wilder smoothing (running sum, bukan EMA biasa)."""
        result = [sum(data[:n])]
        for v in data[n:]:
            result.append(result[-1] - result[-1] / n + v)
        return result

    atr_s  = wilder_smooth(tr_list,  period)
    pdm_s  = wilder_smooth(pdm_list, period)
    ndm_s  = wilder_smooth(ndm_list, period)

    dx_list = []
    for i in range(len(atr_s)):
        pdi = 100 * pdm_s[i] / atr_s[i] if atr_s[i] > 0 else 0.0
        ndi = 100 * ndm_s[i] / atr_s[i] if atr_s[i] > 0 else 0.0
        denom = pdi + ndi
        dx = 100 * abs(pdi - ndi) / denom if denom > 0 else 0.0
        dx_list.append(dx)

    # ADX = Wilder smooth dari DX
    adx_s = wilder_smooth(dx_list, period)
    return adx_s[-1] if adx_s else 25.0


# ── Threshold ADX ──────────────────────────────────────────────────────────────
ADX_TRENDING_MIN  = 20   # ADX ≥ 20 → diizinkan trade
ADX_TRENDING_FULL = 25   # ADX ≥ 25 → trending kuat (bonus)
# Catatan: ADX filter TIDAK berlaku untuk mode LOW_TF / LTF_30M (scalping)
# karena scalping sering justru trade di awal breakout sebelum ADX naik.
# Filter ini hanya untuk INTRADAY dan SCALPING (HTF entry).


def calculate_rr(df, direction, ob, fvg, mode_label: str = "INTRADAY") -> tuple:
    """
    Hitung entry, SL, TP1, TP2 berdasarkan struktur pasar nyata.

    ── STRATEGI ENTRY: SUPPLY & DEMAND ZONE / OB / FVG ────────────────────────
    Entry ditentukan dari level struktural pasar — bukan level matematis.
    Fibo 0.45 dihapus karena sering sudah terlewat saat LIMIT order terisi.

    Prioritas BULLISH (LONG):
      1. OB low zone (30% dari low ke mid) — area buyer paling agresif di OB
      2. FVG bottom zone (30% dari bottom ke mid) — area imbalance belum terisi
      3. Swing low terakhir + buffer — demand struktural
      4. Market price — last resort jika tidak ada referensi struktural

    Prioritas BEARISH (SHORT):
      1. OB top zone (30% dari high turun ke mid) — area seller paling agresif di OB
      2. FVG top zone (30% dari top turun ke mid) — area imbalance supply
      3. Swing high terakhir - buffer — supply struktural
      4. Market price — last resort jika tidak ada referensi struktural

    SL: selalu di swing low (LONG) atau swing high (SHORT) terakhir + ATR buffer.
    ─────────────────────────────────────────────────────────────────────────────

    Prinsip lainnya:
    - TP1 → swing struktur terdekat DI ARAH trade (supply zone / swing high untuk SHORT,
             demand zone / swing low untuk LONG) — harus ada alasan struktural.
    - TP2 → swing struktur berikutnya yang lebih jauh, atau TP1 × 1.5 jika tidak ada.
    - RR  dihitung dari struktur ini, bukan dibalik dari target RR tertentu.
    """
    market_price = float(df["close"].iloc[-1])
    atr          = calculate_atr(df)

    # ── ATR buffer per mode ───────────────────────────────────────────────────
    if mode_label in ("LOW_TF", "LTF_30M"):
        buf = atr * 0.3
    elif mode_label == "SCALPING":
        buf = atr * 0.5
    else:
        buf = atr * 1.0

    highs, lows = find_swings(df)

    # ── Cari swing HIGH dan swing LOW terakhir dari df (lookback 30 candle) ────
    # Swing terakhir = yang paling baru muncul di chart sebelum candle saat ini
    def _last_significant_swing_high(highs_list, lows_list):
        """Swing high terakhir yang signifikan (di antara lows sebelum dan sesudahnya)."""
        if not highs_list:
            return None
        # Urutkan dari yang paling baru (index paling besar)
        sorted_highs = sorted(highs_list, key=lambda x: x[0], reverse=True)
        return sorted_highs[0][1] if sorted_highs else None

    def _last_significant_swing_low(lows_list):
        """Swing low terakhir yang signifikan."""
        if not lows_list:
            return None
        sorted_lows = sorted(lows_list, key=lambda x: x[0], reverse=True)
        return sorted_lows[0][1] if sorted_lows else None

    last_swing_high = _last_significant_swing_high(highs, lows)
    last_swing_low  = _last_significant_swing_low(lows)

    # ── Cap per mode ──────────────────────────────────────────────────────────
    _caps       = SL_TP_CAPS.get(mode_label, (MAX_SL_DISTANCE_PCT, 0.09))
    _max_sl_pct = _caps[0]
    _max_tp_pct = _caps[1]

    if direction == "BULLISH":
        # ── ENTRY ADAPTIF: S&D zone bottom → OB mid → FVG mid → market price ──
        # Prioritas: zona demand (bottom zona = area paling kuat buyer masuk) →
        # order block mid → fair value gap mid → market price sebagai last resort.
        # Fibo 0.45 DIHAPUS — entry harus selalu di level struktural yang valid,
        # bukan level matematis yang sering sudah terlewat saat limit terisi.
        if ob and ob.get("low") is not None and market_price >= ob["low"] * 0.998:
            # Harga di dalam atau sangat dekat OB → entry di bottom OB
            # (lebih konservatif dari mid, memberi ruang harga pull ke zona)
            entry = ob["low"] + (ob["mid"] - ob["low"]) * 0.3
        elif fvg and fvg.get("bottom") is not None and market_price >= fvg["bottom"] * 0.998:
            # Harga di dalam FVG → entry di bottom FVG (area paling kuat)
            entry = fvg["bottom"] + (fvg["mid"] - fvg["bottom"]) * 0.3
        elif last_swing_low is not None:
            # Fallback: entry sedikit di atas swing low terakhir (demand struktural)
            # Pakai swing low + buffer kecil agar LIMIT bisa terisi saat retest
            entry = last_swing_low + buf * 0.5
            # Validasi: entry tidak boleh lebih tinggi dari market price (sudah terlewat)
            if entry >= market_price * 0.998:
                entry = market_price
        else:
            entry = market_price

        # ── SL: swing LOW terakhir (bawah struktur demand) ─────────────────
        if last_swing_low is not None:
            sl = last_swing_low - buf   # sedikit di bawah swing low terakhir
        elif ob:
            sl = ob["low"] - buf
        elif fvg:
            sl = fvg["bottom"] - buf
        else:
            sl = entry - atr * 2.5

        # Pastikan SL di bawah entry
        if sl >= entry:
            sl = entry - atr * 2.0

        # Cap SL per mode
        _max_sl_dist = entry * _max_sl_pct
        if (entry - sl) > _max_sl_dist:
            sl = entry - _max_sl_dist
        dist = entry - sl   # jarak SL

        # ── TP: cari swing HIGH di atas entry sebagai resistance target ───────
        # Urutkan dari yang paling dekat ke entry (ascending) agar TP1 = swing terdekat valid
        highs_above = sorted(
            [(i, p) for i, p in highs if p > entry + atr * 0.5],
            key=lambda x: x[1]
        )

        _max_tp_dist = entry * _max_tp_pct

        # ── TP1: swing HIGH terdekat yang memenuhi RR_MIN_PER_MODE DAN dalam cap ──
        _rr_min  = get_rr_min_for_mode(mode_label)
        _rr_tp2  = RR_GRADE_A_PER_MODE.get(mode_label, RR_GRADE_A)
        tp1_struct    = None
        tp1_anchored  = False
        for _, sh in highs_above:
            _candidate = sh - buf * 0.5
            _dist_from_entry = _candidate - entry
            # Harus memenuhi RR minimum per mode dan tidak melampaui cap
            if _dist_from_entry >= dist * _rr_min and _dist_from_entry <= _max_tp_dist:
                tp1_struct   = _candidate
                tp1_anchored = True
                break

        # Tidak ada swing struktural valid untuk TP1 → tolak sinyal (tp_anchored=False)
        # Fallback ATR× DILARANG — bot akan di-skip di analyze_pair
        if tp1_struct is None:
            tp1_struct   = entry + dist * _rr_min   # nilai placeholder saja
            tp1_anchored = False                     # sinyal akan ditolak

        # ── TP2: swing HIGH berikutnya DI ATAS TP1 (bukan sembarang swing) ─────
        tp2_struct   = None
        tp2_anchored = False
        for _, sh in highs_above:
            _candidate = sh - buf * 0.5
            _dist_from_entry = _candidate - entry
            # Harus di atas TP1 + buffer, memenuhi RR_GRADE_A per mode, dan dalam cap
            if (_candidate > tp1_struct + atr * 0.3
                    and _dist_from_entry >= dist * _rr_tp2
                    and _dist_from_entry <= _max_tp_dist):
                tp2_struct   = _candidate
                tp2_anchored = True
                break

        # TP2 fallback: gunakan TP1 + 50% jarak TP1 (hanya jika TP1 sudah anchored)
        # TP2 fallback ini tidak membuat tp_anchored=True — trailing stop tetap jalan dari TP1
        if tp2_struct is None:
            if tp1_anchored:
                _tp2_fallback = tp1_struct + (tp1_struct - entry) * 0.5
                # Cap TP2 fallback agar tidak melampaui mode cap
                if (_tp2_fallback - entry) <= _max_tp_dist:
                    tp2_struct = _tp2_fallback
                else:
                    tp2_struct = entry + _max_tp_dist
            else:
                tp2_struct = entry + dist * _rr_tp2   # placeholder, sinyal tetap ditolak

        tp1 = tp1_struct
        tp2 = tp2_struct

        # ── Pastikan tp1 tidak melampaui cap (sudah dicek saat loop, ini safety net) ──
        if (tp1 - entry) > _max_tp_dist:
            tp1          = entry + _max_tp_dist
            tp1_anchored = False   # cap paksa = bukan struktur → tolak

        # ── Pastikan tp2 tidak melampaui cap dan tp2 > tp1 ─────────────────────
        if (tp2 - entry) > _max_tp_dist:
            tp2 = entry + _max_tp_dist
        if tp2 <= tp1:
            _tp_gap = max(atr * 0.5, (tp1 - entry) * 0.3)
            tp2 = tp1 + _tp_gap
            # Jika tp2 baru masih melampaui cap, clamp ke cap
            if (tp2 - entry) > _max_tp_dist:
                tp2 = entry + _max_tp_dist

        tp_anchored = tp1_anchored

    else:  # BEARISH / SHORT
        # ── ENTRY ADAPTIF: OB top → FVG top → swing HIGH struktural → market price ──
        # Prioritas: order block top (area paling kuat seller masuk) →
        # fair value gap top → swing high terakhir sebagai supply → market price.
        # Fibo 0.45 DIHAPUS — entry harus selalu di level struktural yang valid,
        # bukan level matematis yang sering sudah terlewat saat limit terisi.
        if ob and ob.get("high") is not None and market_price <= ob["high"] * 1.002:
            # Harga di dalam atau sangat dekat OB → entry di area top OB
            # (lebih konservatif dari mid, memberi ruang harga pull ke zona supply)
            entry = ob["high"] - (ob["high"] - ob["mid"]) * 0.3
        elif fvg and fvg.get("top") is not None and market_price <= fvg["top"] * 1.002:
            # Harga di dalam FVG → entry di area top FVG (area paling kuat)
            entry = fvg["top"] - (fvg["top"] - fvg["mid"]) * 0.3
        elif last_swing_high is not None:
            # Fallback: entry sedikit di bawah swing high terakhir (supply struktural)
            # Pakai swing high - buffer kecil agar LIMIT bisa terisi saat retest
            entry = last_swing_high - buf * 0.5
            # Validasi: entry tidak boleh lebih rendah dari market price (sudah terlewat)
            if entry <= market_price * 1.002:
                entry = market_price
        else:
            entry = market_price

        # ── SL: swing HIGH terakhir (atas struktur) ──────────────────────────
        if last_swing_high is not None:
            sl = last_swing_high + buf   # sedikit di atas swing high terakhir
        elif ob:
            sl = ob["high"] + buf
        elif fvg:
            sl = fvg["top"] + buf
        else:
            sl = entry + atr * 2.5

        # Pastikan SL di atas entry
        if sl <= entry:
            sl = entry + atr * 2.0

        # Cap SL per mode
        _max_sl_dist = entry * _max_sl_pct
        if (sl - entry) > _max_sl_dist:
            sl = entry + _max_sl_dist
        dist = sl - entry   # jarak SL

        # ── TP: cari swing LOW di bawah entry sebagai support target ─────────
        # Urutkan dari yang paling dekat ke entry (descending) agar TP1 = swing terdekat valid
        lows_below = sorted(
            [(i, p) for i, p in lows if p < entry - atr * 0.5],
            key=lambda x: x[1], reverse=True
        )

        _max_tp_dist = entry * _max_tp_pct

        # ── TP1: swing LOW terdekat yang memenuhi RR_MIN_PER_MODE DAN dalam cap ──
        _rr_min  = get_rr_min_for_mode(mode_label)
        _rr_tp2  = RR_GRADE_A_PER_MODE.get(mode_label, RR_GRADE_A)
        tp1_struct    = None
        tp1_anchored  = False
        for _, sl_lv in lows_below:
            _candidate = sl_lv + buf * 0.5
            _dist_from_entry = entry - _candidate
            # Harus memenuhi RR minimum per mode dan tidak melampaui cap mode
            if _dist_from_entry >= dist * _rr_min and _dist_from_entry <= _max_tp_dist:
                tp1_struct   = _candidate
                tp1_anchored = True
                break

        # Tidak ada swing struktural valid untuk TP1 → tolak sinyal (tp_anchored=False)
        if tp1_struct is None:
            tp1_struct   = entry - dist * _rr_min   # nilai placeholder saja
            tp1_anchored = False                     # sinyal akan ditolak

        # ── TP2: swing LOW berikutnya DI BAWAH TP1 (bukan sembarang swing) ─────
        tp2_struct   = None
        tp2_anchored = False
        for _, sl_lv in lows_below:
            _candidate = sl_lv + buf * 0.5
            _dist_from_entry = entry - _candidate
            # Harus di bawah TP1 - buffer, memenuhi RR_GRADE_A per mode, dan dalam cap
            if (_candidate < tp1_struct - atr * 0.3
                    and _dist_from_entry >= dist * _rr_tp2
                    and _dist_from_entry <= _max_tp_dist):
                tp2_struct   = _candidate
                tp2_anchored = True
                break

        # TP2 fallback: gunakan TP1 - 50% jarak TP1 (hanya jika TP1 sudah anchored)
        if tp2_struct is None:
            if tp1_anchored:
                _tp2_fallback = tp1_struct - (entry - tp1_struct) * 0.5
                if (entry - _tp2_fallback) <= _max_tp_dist:
                    tp2_struct = _tp2_fallback
                else:
                    tp2_struct = entry - _max_tp_dist
            else:
                tp2_struct = entry - dist * _rr_tp2   # placeholder, sinyal tetap ditolak

        tp1 = tp1_struct
        tp2 = tp2_struct

        # ── Pastikan tp1 tidak melampaui cap ────────────────────────────────────
        if (entry - tp1) > _max_tp_dist:
            tp1          = entry - _max_tp_dist
            tp1_anchored = False   # cap paksa = bukan struktur → tolak

        # ── Pastikan tp2 tidak melampaui cap dan tp2 < tp1 ─────────────────────
        if (entry - tp2) > _max_tp_dist:
            tp2 = entry - _max_tp_dist
        if tp2 >= tp1:
            _tp_gap = max(atr * 0.5, (entry - tp1) * 0.3)
            tp2 = tp1 - _tp_gap
            if (entry - tp2) > _max_tp_dist:
                tp2 = entry - _max_tp_dist

        tp_anchored = tp1_anchored

    sl_dist = abs(entry - sl)
    rr1     = round(abs(tp1 - entry) / sl_dist, 2) if sl_dist > 0 else 0.0
    rr2     = round(abs(tp2 - entry) / sl_dist, 2) if sl_dist > 0 else 0.0
    # tp_anchored ikut dikembalikan agar analyze_pair bisa reject sinyal tanpa struktur TP
    return entry, sl, tp1, tp2, rr1, rr2, tp_anchored


def apply_adaptive_sl(
    entry: float,
    sl: float,
    direction: str,
    atr: float,
    tp1: float,
    tp2: float,
    mode_label: str = "INTRADAY",
    rr_min: float = 1.5,
    max_sl_pct: float = 0.06,
) -> tuple:
    """
    Pastikan SL tidak lebih sempit dari 1× ATR (adaptive SL minimum).

    Logika:
    - Hitung jarak SL saat ini dari entry
    - Jika jarak < 1× ATR → perlebar SL ke 1× ATR
    - Setelah pelebaran, re-check RR: jika RR jatuh di bawah rr_min → return None (skip)
    - Jika SL baru melampaui max_sl_pct dari entry → clamp ke max_sl_pct

    Mengapa 1× ATR?
    ATR merepresentasikan gerakan "normal" dalam 1 candle. SL yang lebih sempit
    dari ATR hampir pasti kena noise sebelum harga sempat bergerak ke arah target.

    Return: (new_sl, new_rr1, ok: bool)
      ok=False → sinyal harus di-skip (RR tidak memenuhi setelah SL dilebarkan)
    """
    raw_dist = abs(entry - sl)
    atr_min  = atr * 1.0   # minimum 1× ATR

    # Mode scalping: gunakan batas lebih longgar (0.6× ATR)
    # karena scalping entry TF pendek, pergerakan lebih kecil
    if mode_label in ("LOW_TF", "LTF_30M"):
        atr_min = atr * 0.6

    if raw_dist >= atr_min:
        # SL sudah cukup lebar — tidak perlu diubah
        sl_dist = raw_dist
        rr1 = round(abs(tp1 - entry) / sl_dist, 2) if sl_dist > 0 else 0.0
        return sl, rr1, True

    # Perlebar SL ke atr_min
    if direction == "BULLISH":
        new_sl = entry - atr_min
    else:
        new_sl = entry + atr_min

    # Cap ke max_sl_pct agar tidak terlalu jauh
    max_dist = entry * max_sl_pct
    if abs(new_sl - entry) > max_dist:
        new_sl = (entry - max_dist) if direction == "BULLISH" else (entry + max_dist)

    new_sl_dist = abs(entry - new_sl)
    if new_sl_dist <= 0:
        return sl, 0.0, False

    new_rr1 = round(abs(tp1 - entry) / new_sl_dist, 2)

    # Re-check RR setelah SL dilebarkan
    if new_rr1 < rr_min:
        return new_sl, new_rr1, False   # RR tidak layak lagi → skip

    return new_sl, new_rr1, True


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 14 — SCORING ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def compute_score(
    # ── Tier 1: HTF ──────────────────────────────────────────
    htf_aligned: bool,
    # ── Tier 2: BOS / Market Structure ───────────────────────
    bos_confirmed: bool,
    bos_strength: str,          # "STRONG" | "WEAK" | "NONE"
    # ── Tier 3: Momentum ──────────────────────────────────────
    momentum_aligned: bool,     # N candle berturut arah sama
    momentum_present: bool,     # ada momentum, meski campuran
    # ── Tier 4: Liquidity ─────────────────────────────────────
    liq_swept: bool,
    sweep_strength: str,        # "STRONG" | "RELAXED" | None
    # ── Tier 5: Entry Zone ────────────────────────────────────
    in_ob: bool,
    in_fvg: bool,
    in_imb: bool = False,       # Body Imbalance zone
    in_gap: bool = False,       # Price Gap zone
    # ── Supporting factors ────────────────────────────────────
    displacement: bool = False,
    vol_rat: float = 0.0,
    session: str = "Off-Hours",
    rsi: float = 50.0,
    macro_pts: int = 0,
    ema_pts: int = 0,
    ref_aligned: bool = False,
    # ── Direction context ─────────────────────────────────────
    direction: str = "BULLISH",     # "BULLISH" | "BEARISH" — untuk RSI directional check
    # ── Legacy params (kept for backward compat, ignored) ─────
    choch_confirmed: bool = False,
    ob_quality: float = 0.0,
) -> tuple:
    """
    Scoring-based engine mengikuti SMC flow (versi main(1) — IMB+Gap aware):
      HTF bias → Liquidity target → Sweep/reaksi → BOS/momentum → Entry OB/FVG/IMB/Gap

    Tidak ada hard-gate; semua elemen berkontribusi ke total score.
    Sinyal dikirim jika total ≥ threshold.
    """
    bd = {}

    # ── Tier 1: HTF (−10 … +15) ──────────────────────────────────────────────
    bd["htf"] = SCORE_HTF_ALIGNED if htf_aligned else SCORE_HTF_PENALTY

    # ── Tier 2: BOS (−10 … +25) ──────────────────────────────────────────────
    if bos_confirmed:
        bd["bos"] = SCORE_BOS_STRONG if bos_strength == "STRONG" else SCORE_BOS_WEAK
    else:
        bd["bos"] = SCORE_BOS_PENALTY

    # ── Tier 3: Momentum (−5 … +15) ──────────────────────────────────────────
    if momentum_aligned:
        bd["momentum"] = SCORE_MOMENTUM
    elif momentum_present:
        bd["momentum"] = SCORE_MOMENTUM_NONE
    else:
        bd["momentum"] = SCORE_MOMENTUM_ANTI

    # ── Tier 4: Liquidity sweep (−5 … +15) ───────────────────────────────────
    if liq_swept and sweep_strength == "STRONG":
        bd["liquidity"] = SCORE_LIQUIDITY
    elif liq_swept:
        bd["liquidity"] = SCORE_LIQUIDITY_WEAK
    else:
        bd["liquidity"] = SCORE_LIQUIDITY_NONE

    # ── Tier 5: Entry zone OB/FVG/IMB/Gap (0 … +15) ─────────────────────────
    # Hitung berapa zona yang aktif (OB, FVG, IMB, Gap)
    _zone_count = sum([in_ob, in_fvg, in_imb, in_gap])
    if _zone_count >= 2:
        bd["entry_zone"] = SCORE_OB_AND_FVG          # +15: confluence ≥2 zona
    elif _zone_count == 1:
        bd["entry_zone"] = SCORE_OB_OR_FVG           # +10: satu zona valid
    else:
        bd["entry_zone"] = SCORE_ZONE_NONE            # 0: tidak di zona manapun

    # ── Supporting ────────────────────────────────────────────────────────────
    bd["displacement"] = SCORE_DISPLACEMENT if displacement else 0
    bd["volume"]       = SCORE_VOLUME if vol_rat >= 1.5 else (2 if vol_rat >= 1.2 else 0)
    bd["session"]      = session_score(session)
    bd["rsi"]          = rsi_score(rsi, direction)
    bd["macro"]        = macro_pts
    bd["ema"]          = ema_pts
    bd["ref_tf"]       = SCORE_REF_TF if ref_aligned else 0

    total = sum(bd.values())
    return total, bd


def grade_signal(rr: float) -> Optional[str]:
    if rr >= RR_GRADE_A:  return "A"
    if rr >= RR_GRADE_B:  return "B"
    return None


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 15 — VOLATILITY & CONFIRMATION
# ═══════════════════════════════════════════════════════════════════════════

def check_volatility(df: pd.DataFrame) -> tuple:
    atr     = calculate_atr(df)
    price   = float(df["close"].iloc[-1])
    atr_pct = (atr / price) * 100 if price > 0 else 0.0

    if atr_pct < ATR_MIN_PCT:
        return False, f"Low volatility: ATR={atr_pct:.3f}%"

    recent    = df.iloc[-10:]
    r_high    = float(recent["high"].max())
    r_low     = float(recent["low"].min())
    range_pct = ((r_high - r_low) / r_low * 100) if r_low > 0 else 0.0
    if range_pct < ATR_MIN_PCT * 3:
        return False, f"Ranging: 10-candle range={range_pct:.3f}%"

    return True, f"Volatility OK: ATR={atr_pct:.3f}%"


def check_confirmation_candle(df: pd.DataFrame, direction: str) -> bool:
    c    = df.iloc[-1]
    prev = df.iloc[-2]
    o,  h,  l,  cl  = float(c["open"]),    float(c["high"]),    float(c["low"]),    float(c["close"])
    po, ph, pl, pcl = float(prev["open"]), float(prev["high"]), float(prev["low"]), float(prev["close"])

    candle_range = h - l
    if candle_range < 1e-9:
        return False

    body_ratio = abs(cl - o) / candle_range
    if direction == "BULLISH":
        return (cl > o and body_ratio >= CONFIRM_BODY_RATIO) or (cl > o and cl > po and o < pcl)
    else:
        return (cl < o and body_ratio >= CONFIRM_BODY_RATIO) or (cl < o and cl < po and o > pcl)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 16 — CORRELATION FILTER
# ═══════════════════════════════════════════════════════════════════════════

def _get_correlation_group(pair: str) -> Optional[set]:
    for group in CORRELATION_GROUPS:
        if pair in group:
            return group
    return None


def apply_correlation_filter(candidates: list) -> list:
    surviving  = []
    used_groups: dict = {}

    for cand in candidates:
        group = _get_correlation_group(cand["pair"])
        if group is None:
            surviving.append(cand)
            continue
        key      = (frozenset(group), cand["direction"])
        existing = used_groups.get(key)
        if existing is None:
            used_groups[key] = cand
        elif (cand["score"] > existing["score"] or
              (cand["score"] == existing["score"] and cand["rr"] > existing["rr"])):
            used_groups[key] = cand

    surviving.extend(used_groups.values())
    return surviving


# ██  SECTION 17 — DYNAMIC RISK & LOT SIZE
# ═══════════════════════════════════════════════════════════════════════════

_exchange_info_cache: dict = {}


def _parse_symbol_filters(s: dict) -> dict:
    """
    Parse semua filter dari satu entry symbol di exchangeInfo.

    Kunci penting:
    - lot_maxQty  : maxQty dari LOT_SIZE  (untuk LIMIT order)
    - maxQty      : maxQty untuk MARKET order = min(LOT_SIZE.maxQty, MARKET_LOT_SIZE.maxQty)
                    KECUALI jika MARKET_LOT_SIZE.maxQty × tickPrice < minNotional
                    (kontradiksi) → fallback ke LOT_SIZE.maxQty
    - minNotional : dari MIN_NOTIONAL filter, field "notional"
    - tickSize    : diambil dari pricePrecision (level symbol) jika PRICE_FILTER terlalu kasar
    """
    filters = {}

    # ── Ambil pricePrecision dari level symbol (lebih akurat untuk harga kecil) ──
    # Binance menyimpan pricePrecision di root symbol dict, bukan di filters.
    # Contoh: ADAUSDT pricePrecision=4 → tickSize efektif = 0.0001
    price_precision = s.get("pricePrecision")   # int, misal 4
    qty_precision   = s.get("quantityPrecision") # int, misal 0

    for f in s.get("filters", []):
        ft = f["filterType"]
        if ft == "LOT_SIZE":
            filters["stepSize"]    = float(f["stepSize"])
            filters["minQty"]      = float(f["minQty"])
            filters["lot_maxQty"]  = float(f["maxQty"])   # simpan LOT_SIZE.maxQty terpisah
            filters["maxQty"]      = float(f["maxQty"])   # default, akan di-update
        elif ft == "MARKET_LOT_SIZE":
            filters["mkt_maxQty"]   = float(f.get("maxQty",  9999999.0))
            filters["mkt_minQty"]   = float(f.get("minQty",  0.0))
            filters["mkt_stepSize"] = float(f.get("stepSize", 0.0))
        elif ft == "PRICE_FILTER":
            raw_tick = float(f["tickSize"])
            filters["tickSize"] = raw_tick
            filters["price_filter_tick"] = raw_tick  # simpan asli untuk referensi
        elif ft == "MIN_NOTIONAL":
            filters["minNotional"] = float(f.get("notional", 5.0))

    # ── Koreksi tickSize menggunakan pricePrecision jika PRICE_FILTER terlalu kasar ──
    # Kasus: ADAUSDT PRICE_FILTER tickSize=0.1, tapi pricePrecision=4
    # → harga 0.2443 dibulatkan ke 0.2 (salah) → gunakan 10^-pricePrecision = 0.0001
    raw_tick = filters.get("tickSize", 0.0001)
    if price_precision is not None:
        precise_tick = 10 ** (-int(price_precision))
        if precise_tick < raw_tick:
            # pricePrecision lebih granular dari PRICE_FILTER → pakai pricePrecision
            filters["tickSize"] = precise_tick

    # ── Simpan quantityPrecision — dipakai untuk format_qty agar tidak -1111 ──
    # Binance Futures memvalidasi quantity berdasarkan quantityPrecision (root symbol),
    # bukan hanya stepSize. Contoh: RENDERUSDT quantityPrecision=0 → qty harus bulat.
    # Jika quantityPrecision tidak ada di response (misal testnet), derive dari stepSize.
    if qty_precision is not None:
        filters["quantityPrecision"] = int(qty_precision)
    elif "stepSize" in filters:
        # Derive dari stepSize: 0.001 → prec=3, 1.0 → prec=0, 0.01 → prec=2
        import math as _m
        _s = filters["stepSize"]
        _derived = max(0, round(-_m.log10(_s))) if _s > 0 else 3
        filters["quantityPrecision"] = _derived

    mkt_max  = filters.get("mkt_maxQty",   9999999.0)   # tetap disimpan di dict untuk cek di execute_trade
    mkt_min  = filters.pop("mkt_minQty",   0.0)
    mkt_step = filters.pop("mkt_stepSize", 0.0)

    lot_max      = filters.get("lot_maxQty", 9999999.0)
    min_notional = filters.get("minNotional", 5.0)
    tick_size    = filters.get("tickSize", 0.0001)

    # Simpan mkt_stepSize terpisah — dipakai untuk format quantity MARKET order.
    # MARKET_LOT_SIZE.stepSize bisa lebih besar dari LOT_SIZE.stepSize (misal 1 vs 0.001).
    # Binance memvalidasi quantity MARKET order berdasarkan MARKET_LOT_SIZE.stepSize.
    # Jika mkt_step = 0 atau tidak ada → jangan simpan, biarkan execute_trade fallback ke stepSize
    if mkt_step > 0:
        filters["mkt_stepSize"] = mkt_step
    else:
        # Pastikan tidak ada key mkt_stepSize dengan nilai 0 di cache
        filters.pop("mkt_stepSize", None)

    # stepSize (untuk LIMIT order & lot sizing): tetap pakai LOT_SIZE.stepSize
    # Jangan override dengan mkt_step karena itu khusus MARKET order validation

    # minQty: pakai terbesar antara LOT_SIZE dan MARKET_LOT_SIZE
    if mkt_min > 0:
        filters["minQty"] = max(filters.get("minQty", 0.0), mkt_min)

    # maxQty untuk MARKET order:
    # Jika MARKET_LOT_SIZE.maxQty sangat kecil sehingga notionalnya < minNotional,
    # itu berarti MARKET_LOT_SIZE.maxQty bukan hard limit untuk akun normal
    # (biasanya limit untuk HFT/algo). Gunakan LOT_SIZE.maxQty.
    if mkt_max < lot_max:
        # Estimasi notional maksimal dengan MARKET_LOT_SIZE.maxQty
        # pakai tickSize sebagai proxy harga minimum (harga nyata akan lebih besar)
        # Jika mkt_max * tickSize < minNotional → kontradiksi → pakai lot_maxQty
        if tick_size > 0 and (mkt_max * tick_size) < min_notional:
            # Kontradiksi: MARKET_LOT_SIZE.maxQty terlalu kecil, fallback ke LOT_SIZE.maxQty
            filters["maxQty"] = lot_max
        else:
            filters["maxQty"] = mkt_max
    else:
        filters["maxQty"] = lot_max

    return filters


def load_exchange_info():
    global _exchange_info_cache
    try:
        info = api_get("/fapi/v1/exchangeInfo")
        for s in info.get("symbols", []):
            filters = _parse_symbol_filters(s)
            _exchange_info_cache[s["symbol"]] = filters
            if s["symbol"] in ("APTUSDT", "XLMUSDT", "PEPEUSDT", "SHIBUSDT", "GALAUSDT", "COMPUSDT", "ADAUSDT", "ETCUSDT"):
                print(f"  📋 {s['symbol']} filters: {filters}")
        print(f"✅ Exchange info loaded: {len(_exchange_info_cache)} symbols")
    except Exception as e:
        print(f"⚠️ Gagal load exchange info: {e}")

def get_lot_filters(symbol: str) -> dict:
    if not _exchange_info_cache:
        load_exchange_info()
    return _exchange_info_cache.get(symbol, {
        "stepSize":    0.001,
        "minQty":      0.001,
        "maxQty":      9999999.0,
        "minNotional": 5.0,
        "tickSize":    0.01,
    })


# Set berisi symbol yang sudah terbukti error 400 berkali-kali (cooldown auto-skip)
_untradeable_symbols: dict = {}   # symbol -> fail_count

# ── SL Blacklist: pair yang kena SL beruntun → cooldown ekstra ───────────────
# Struktur: { "SOLUSDT": {"count": 2, "first_sl_ts": 1700000000.0} }
# Jika 1 pair kena SL 2× dalam 48 jam → cooldown 24 jam extra (blok semua arah)
_pair_sl_tracker: dict = {}
SL_BLACKLIST_WINDOW_HOURS = 48    # window deteksi SL beruntun (jam)
SL_BLACKLIST_TRIGGER      = 2     # jumlah SL dalam window yang trigger blacklist
SL_BLACKLIST_COOLDOWN_HOURS = 24  # lama cooldown ekstra setelah kena blacklist


def record_pair_sl(symbol: str):
    """
    Catat satu kejadian SL untuk symbol ini.
    Jika dalam SL_BLACKLIST_WINDOW_HOURS jam terjadi SL_BLACKLIST_TRIGGER kali → cooldown ekstra.
    Dipanggil oleh monitor loop setiap kali posisi close karena SL.
    """
    from datetime import timedelta
    now_ts = time.time()
    now_dt = datetime.now(timezone.utc)

    entry = _pair_sl_tracker.get(symbol)
    if entry:
        # Cek apakah masih dalam window
        window_secs = SL_BLACKLIST_WINDOW_HOURS * 3600
        if now_ts - entry["first_sl_ts"] > window_secs:
            # Window habis → reset counter
            _pair_sl_tracker[symbol] = {"count": 1, "first_sl_ts": now_ts}
            print(f"  🔄 [{symbol}] SL tracker reset (window {SL_BLACKLIST_WINDOW_HOURS}j habis) → count=1")
            return
        # Masih dalam window → tambah count
        new_count = entry["count"] + 1
        _pair_sl_tracker[symbol]["count"] = new_count
        print(f"  📊 [{symbol}] SL count dalam {SL_BLACKLIST_WINDOW_HOURS}j: {new_count}/{SL_BLACKLIST_TRIGGER}")

        if new_count >= SL_BLACKLIST_TRIGGER:
            # Trigger cooldown ekstra — blok semua arah
            until = now_dt + timedelta(hours=SL_BLACKLIST_COOLDOWN_HOURS)
            key   = f"{symbol}|CLOSE"
            _cooldown_map[key] = until
            # Reset tracker setelah trigger agar tidak trigger terus
            _pair_sl_tracker[symbol] = {"count": 0, "first_sl_ts": now_ts}
            msg = (
                f"🚫 <b>PAIR BLACKLIST — {symbol}</b>\n"
                f"{'─'*34}\n"
                f"❌ {new_count}× SL dalam {SL_BLACKLIST_WINDOW_HOURS} jam terakhir\n"
                f"⏳ Cooldown ekstra: <b>{SL_BLACKLIST_COOLDOWN_HOURS} jam</b>\n"
                f"🔄 Sampai: {until.strftime('%d %b %H:%M')} UTC\n"
                f"ℹ️ Bot tidak akan buka posisi {symbol} selama periode ini."
            )
            send_telegram_raw(msg)
            print(
                f"  🚫 [{symbol}] BLACKLIST aktif — {new_count}× SL dalam {SL_BLACKLIST_WINDOW_HOURS}j "
                f"→ cooldown {SL_BLACKLIST_COOLDOWN_HOURS}j sampai {until.strftime('%H:%M')} UTC"
            )
    else:
        # Pertama kali kena SL untuk pair ini
        _pair_sl_tracker[symbol] = {"count": 1, "first_sl_ts": now_ts}
        print(f"  📊 [{symbol}] SL pertama dicatat (window {SL_BLACKLIST_WINDOW_HOURS}j, trigger di ×{SL_BLACKLIST_TRIGGER})")


def validate_pair_tradeable(symbol: str, price: float) -> bool:
    """
    Skip pair yang sudah gagal ≥3 kali berturut-turut.
    Reset setiap 6 jam. Bukan skip permanen — Binance bisa update filter kapan saja.
    """
    import time as _time
    now = _time.time()
    entry = _untradeable_symbols.get(symbol)
    if entry:
        count, first_fail = entry
        # Reset setelah 6 jam
        if now - first_fail > 21600:
            del _untradeable_symbols[symbol]
            return True
        if count >= 3:
            return False
    return True


def record_pair_fail(symbol: str):
    """Catat satu kegagalan order untuk symbol ini."""
    import time as _time
    now = _time.time()
    entry = _untradeable_symbols.get(symbol)
    if entry:
        count, first_fail = entry
        _untradeable_symbols[symbol] = (count + 1, first_fail)
    else:
        _untradeable_symbols[symbol] = (1, now)
    count = _untradeable_symbols[symbol][0]
    if count >= 3:
        print(f"  ⚠️  [{symbol}] di-skip sementara setelah {count}x gagal (reset 6 jam)")


def _step_precision(step_size: float) -> int:
    """Hitung jumlah desimal dari step_size (misal 0.001 → 3, 1.0 → 0)."""
    if step_size <= 0:
        return 8
    return max(0, round(-math.log10(step_size)))


def round_lot_to_step(quantity: float, step_size: float) -> float:
    """Floor ke step_size terdekat, bebas floating-point drift."""
    if step_size <= 0:
        return quantity
    precision = _step_precision(step_size)
    # Gunakan Decimal untuk menghindari floating-point error
    from decimal import Decimal, ROUND_DOWN
    d_qty  = Decimal(str(quantity))
    d_step = Decimal(str(step_size))
    qty = float((d_qty / d_step).to_integral_value(rounding=ROUND_DOWN) * d_step)
    return round(qty, precision)


def round_lot_up_to_step(quantity: float, step_size: float) -> float:
    """Ceil ke step_size terdekat, bebas floating-point drift."""
    if step_size <= 0:
        return quantity
    precision = _step_precision(step_size)
    from decimal import Decimal, ROUND_UP
    d_qty  = Decimal(str(quantity))
    d_step = Decimal(str(step_size))
    qty = float((d_qty / d_step).to_integral_value(rounding=ROUND_UP) * d_step)
    return round(qty, precision)


def format_qty(quantity: float, step_size: float, qty_precision: int = None) -> str:
    """
    Format quantity sebagai string dengan presisi exact sesuai stepSize + quantityPrecision.

    Aturan Binance:
    - stepSize menentukan granularitas lot (misal 0.001 = 3 desimal)
    - quantityPrecision (root symbol) adalah hard cap desimal untuk MARKET order
    - Jika keduanya ada, ambil yang PALING KETAT (desimal terkecil = presisi lebih sedikit)
    - Floor quantity sebelum format — jangan hanya round, Binance tidak suka rounding naik

    Contoh bug yang dicegah:
    - HYPEUSDT stepSize=0.0001 tapi quantityPrecision=1 → qty "100.7848" → REJECT -1111
      Fix: min(4, 1) = 1 → floor → "100.7" ✅
    - RENDERUSDT stepSize=0.001 tapi quantityPrecision=0 → qty "50.113" → REJECT -1111
      Fix: min(3, 0) = 0 → floor → "50" ✅
    """
    precision = _step_precision(step_size)
    if qty_precision is not None:
        precision = min(precision, int(qty_precision))
    # FLOOR dulu sebelum format — Binance strict, tidak boleh ada digit lebih
    import math as _mf
    if precision >= 0:
        factor = 10 ** precision
        quantity = _mf.floor(quantity * factor) / factor
    return f"{quantity:.{precision}f}"


def format_price(price: float, tick_size: float) -> str:
    """
    Format harga sebagai string dengan presisi exact sesuai tickSize.

    Binance algoOrder endpoint (-1111) menolak triggerPrice dengan presisi
    lebih banyak dari yang didefinisikan exchange untuk pair tersebut.
    Contoh: IMXUSDT tickSize=0.0001 → harga harus '0.1305', bukan '0.13050000000001'.

    PENTING: Selalu gunakan ini untuk semua triggerPrice di algoOrder.
    """
    if tick_size <= 0:
        # fallback: hitung presisi dari nilai harga itu sendiri
        if price >= 100:    precision = 2
        elif price >= 10:   precision = 3
        elif price >= 1:    precision = 4
        elif price >= 0.1:  precision = 4
        elif price >= 0.01: precision = 5
        else:               precision = 8
    else:
        precision = _step_precision(tick_size)
    return f"{price:.{precision}f}"


def round_price_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return price
    precision = max(0, round(-math.log10(tick_size)))
    precision = min(precision, 8)
    price_rounded = round(round(price / tick_size) * tick_size, precision)
    return price_rounded


def get_dynamic_risk():
    """
    Hitung risk per trade berdasarkan jumlah posisi yang BOLEH dibuka (dynamic_max).

    Logika:
      RISK_PER_TRADE = total risk maksimal jika semua posisi kena SL sekaligus.
      Per trade = RISK_PER_TRADE / dynamic_max_trades

      Pakai get_dynamic_max_trades() bukan MAX_OPEN_TRADES langsung,
      agar jika user set /maxopentrade 3 dengan balance $50,
      risk per trade = 30%/3 = 10% → notional $50×10%×15x = $75 (terlalu kecil!)

      FIX: gunakan max(dynamic_max, 1) tapi cap ke actual open + 1
      Jika saat ini hanya 0 posisi terbuka dan mau buka 1,
      risk per trade = RISK_PER_TRADE / 1 = 30% → notional $225 ✅
    """
    dynamic_max = get_dynamic_max_trades()
    current_open = count_open_positions()
    # Slots yang masih tersedia — ini menentukan berapa "slot" yang bersaing
    # Jika sudah ada 2 posisi dan max=3 → slot tersisa 1 → risk untuk 1 trade
    slots_remaining = max(1, dynamic_max - current_open)
    # Risk per trade = total risk dibagi slot yang masih tersedia
    base = RISK_PER_TRADE / slots_remaining
    # Tetap ada batas atas: tidak boleh lebih dari RISK_PER_TRADE penuh (jika slots=1)
    base = min(base, RISK_PER_TRADE)
    if bot_state["lose_streak"] >= 3:
        return base * 0.5
    if bot_state["win_streak"] >= 3:
        return base * 1.5
    return base


def calculate_lot(entry, sl, balance, symbol=None, leverage=None):
    """
    Hitung lot size dengan money management adaptif.

    Sistem:
    1. Leverage dipilih otomatis berdasarkan harga (get_leverage_for_price)
    2. risk_usdt = totalBalance x (RISK_PER_TRADE / MAX_OPEN_TRADES)
       -> risk per trade sudah dibagi jumlah max posisi agar total exposure
          tidak melebihi RISK_PER_TRADE meski semua posisi kena SL sekaligus
    3. Lot = risk_usdt / SL_distance -> berapa unit supaya loss = risk_usdt
    4. Notional di-cap: balance x leverage x MAX_NOTIONAL_RATIO (di execute_trade)
    """
    if entry <= 0:
        return 0

    if leverage is None:
        leverage = get_leverage_for_price(entry)

    risk_usdt = balance * get_dynamic_risk()

    # ── PROTEKSI 1: enforce minimum SL distance ───────────────────────────────
    raw_distance = abs(entry - sl)
    min_distance = entry * MIN_SL_DISTANCE_PCT
    effective_distance = max(raw_distance, min_distance)

    # Lot dari risk: loss jika SL kena = lot × distance = risk_usdt
    raw_lot = risk_usdt / effective_distance

    # ── PROTEKSI 2: cap notional terhadap leverage yang dipilih ──────────────
    # Gunakan full leverage capacity (MAX_NOTIONAL_RATIO diaplikasikan di execute_trade)
    max_notional = balance * leverage
    raw_lot      = min(raw_lot, max_notional / entry)

    # ── PROTEKSI 3: maxQty dari exchange info ─────────────────────────────────
    # Gunakan lot_maxQty (LOT_SIZE filter), BUKAN maxQty (MARKET_LOT_SIZE).
    # MARKET_LOT_SIZE.maxQty untuk pair nano-price (PEPE/SHIB) sangat kecil
    # sehingga raw_lot ter-cap kecil sebelum sampai execute_trade.
    # lot_maxQty adalah true hard cap per order untuk akun normal.
    if symbol:
        filters   = get_lot_filters(symbol)
        lot_max   = filters.get("lot_maxQty", filters.get("maxQty", 9999999.0))
        step_size = filters.get("stepSize", 0.001)
        raw_lot   = min(raw_lot, lot_max)
        raw_lot   = round_lot_to_step(raw_lot, step_size)

    return max(raw_lot, 0)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 18 — EXECUTE TRADE
# ═══════════════════════════════════════════════════════════════════════════

def set_leverage(symbol, leverage):
    try:
        api_post("/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
    except Exception as e:
        print(f"⚠️ Set leverage error: {e}")


def _cancel_algo_orders(symbol: str):
    """
    Cancel semua algo/conditional orders untuk symbol.

    Layer 1: Bulk cancel via DELETE /fapi/v1/algoOrder/openOrders
    Layer 2: Fetch list → cancel satu per satu via algoId
    404 = testnet atau akun tidak support → diabaikan diam-diam (fallback ke allOpenOrders)
    """
    # ── Layer 1: Bulk cancel ──────────────────────────────────────────────────
    try:
        api_delete("/fapi/v1/algoOrder/openOrders", {"symbol": symbol})
        return  # berhasil, selesai
    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else 0
        if code == 404:
            pass  # testnet / tidak support — lanjut ke layer 2
        else:
            print(f"  ⚠️ Bulk cancel algo {symbol} HTTP {code} — coba per algoId")
    except Exception:
        pass

    # ── Layer 2: Fetch list → cancel per algoId ───────────────────────────────
    try:
        algo_resp = api_get("/fapi/v1/algoOrder/openOrders", {"symbol": symbol}, signed=True)
        if isinstance(algo_resp, list):
            orders = algo_resp
        elif isinstance(algo_resp, dict):
            orders = algo_resp.get("orders", [])
        else:
            orders = []

        for o in orders:
            algo_id = o.get("algoId") or o.get("orderId")
            if not algo_id:
                continue
            try:
                api_delete("/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id})
            except Exception as ce:
                print(f"  ⚠️  [{symbol}] Gagal cancel algoId {algo_id}: {ce}")
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            pass  # testnet — aman diabaikan
        else:
            print(f"  ⚠️ Fetch algo orders {symbol}: {e}")
    except Exception as e:
        print(f"  ⚠️ Fetch algo orders {symbol}: {e}")


def cancel_open_orders(symbol: str):
    # ── Step 1: cancel algo orders (SL/TP via algoOrder endpoint) ────────────
    _cancel_algo_orders(symbol)
    time.sleep(0.1)

    # ── Step 2: cancel per orderId (STOP_MARKET, TAKE_PROFIT_MARKET, dll) ────
    # Tidak andalkan bulk allOpenOrders — conditional orders (reduceOnly=true)
    # sering tidak ikut bulk cancel. Per orderId adalah cara paling pasti.
    try:
        open_ords = api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
        for o in open_ords:
            oid = o.get("orderId")
            if oid:
                try:
                    api_delete("/fapi/v1/order", {"symbol": symbol, "orderId": oid})
                    time.sleep(0.05)
                except Exception as ce:
                    print(f"⚠️ Cancel orderId {oid} {symbol}: {ce}")
    except Exception as fe:
        print(f"⚠️ Fetch openOrders {symbol}: {fe}")

    # ── Step 3: bulk sebagai catch-all tambahan ───────────────────────────────
    try:
        api_delete("/fapi/v1/allOpenOrders", {"symbol": symbol})
    except Exception:
        pass


def cleanup_stale_orders(dry_run: bool = False, silent_if_clean: bool = False):
    """
    Deteksi (dan opsional cancel) open orders untuk pair yang TIDAK memiliki
    posisi aktif di Binance DAN TIDAK sedang dalam pending limit order.

    dry_run=True  → hanya laporan, tidak cancel (default dari main loop — DIHAPUS)
    dry_run=False → cancel semua orphan (hanya dipanggil via /cleanuporders)

    ⚠️  Pending limit order TIDAK dianggap orphan — mereka memang belum punya
        posisi karena masih menunggu terisi di Binance.
    """
    try:
        # ── Ambil semua posisi yang benar-benar open ──────────────────────────
        all_pos = api_get("/fapi/v2/positionRisk", signed=True)
        symbols_with_position = set()
        for p in all_pos:
            if abs(float(p.get("positionAmt", 0))) > 0:
                symbols_with_position.add(p["symbol"])

        # ── Kecualikan symbol yang punya pending limit order ──────────────────
        # Limit order yang belum terisi memang tidak punya posisi — bukan orphan!
        symbols_pending_limit = set(pending_limit_orders.keys())
        symbols_safe = symbols_with_position | symbols_pending_limit

        print(f"  🔍 cleanup: posisi aktif={sorted(symbols_with_position) or 'none'} | pending limit={sorted(symbols_pending_limit) or 'none'}")

        # ── Ambil semua open regular orders ──────────────────────────────────
        stale_symbols_regular = set()
        open_orders = []
        try:
            open_orders = api_get("/fapi/v1/openOrders", signed=True)
            for o in open_orders:
                sym = o.get("symbol", "")
                if sym and sym not in symbols_safe:
                    stale_symbols_regular.add(sym)
            print(f"  🔍 cleanup: regular openOrders={len(open_orders)} total | orphan={sorted(stale_symbols_regular) or 'none'}")
        except Exception as e:
            print(f"  ⚠️  cleanup: gagal fetch openOrders: {e}")

        # ── Ambil algo/conditional orders (mainnet only, 404 di testnet) ──────
        stale_algo_by_symbol: dict = {}
        try:
            algo_resp = api_get("/fapi/v1/algoOrder/openOrders", signed=True)
            algo_list = algo_resp if isinstance(algo_resp, list) else algo_resp.get("orders", [])
            for o in algo_list:
                sym     = o.get("symbol", "")
                algo_id = o.get("algoId") or o.get("orderId")
                if sym and algo_id and sym not in symbols_safe:
                    stale_algo_by_symbol.setdefault(sym, []).append(algo_id)
            print(f"  🔍 cleanup: algo orders={len(algo_list)} total | orphan={sorted(stale_algo_by_symbol.keys()) or 'none'}")
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"  🔍 cleanup: algoOrder endpoint tidak tersedia (testnet) — skip")
            else:
                print(f"  ⚠️  cleanup algoOrders HTTP error: {e}")
        except Exception as e:
            print(f"  ⚠️  cleanup algoOrders error: {e}")

        all_stale = stale_symbols_regular | set(stale_algo_by_symbol.keys())

        if not all_stale:
            print(f"  ✅ cleanup: tidak ada orphan orders — semua bersih")
            if not dry_run and not silent_if_clean:
                send_telegram_raw("✅ <b>Cleanup Orders</b>\nTidak ada orphan orders — semua bersih.")
            return

        print(f"  {'🔍 DRY-RUN' if dry_run else '🧹'} cleanup: {len(all_stale)} orphan → {sorted(all_stale)}")

        if dry_run:
            # Hanya laporan — tidak cancel apapun
            pending_note = f"\n⚠️ Pending limit dikecualikan: {sorted(symbols_pending_limit)}" if symbols_pending_limit else ""
            send_telegram_raw(
                f"🔍 <b>Orphan Orders Terdeteksi</b>\n"
                f"{'─'*34}\n"
                f"<b>{len(all_stale)}</b> pair punya orders tanpa posisi:\n"
                + "\n".join([f"  ⚠️ {s}" for s in sorted(all_stale)])
                + pending_note + "\n"
                f"{'─'*34}\n"
                f"Ketik <b>/cleanuporders</b> untuk cancel semua orphan."
            )
            return

        # ── Eksekusi cancel (hanya jika dry_run=False, via /cleanuporders) ────
        cancelled_ok  = []
        cancelled_fail = []

        for sym in all_stale:
            sym_ok = True

            # ── Step 1: fetch semua open orders lalu cancel SATU PER SATU ────────
            # Tidak andalkan bulk allOpenOrders — conditional orders (STOP_MARKET,
            # TAKE_PROFIT_MARKET dengan reduceOnly) sering tidak ikut bulk cancel.
            # Cancel per orderId adalah cara paling pasti.
            try:
                all_open = api_get("/fapi/v1/openOrders", {"symbol": sym}, signed=True)
                cancel_ok = 0
                for o in all_open:
                    oid = o.get("orderId")
                    if not oid:
                        continue
                    try:
                        api_delete("/fapi/v1/order", {"symbol": sym, "orderId": oid})
                        cancel_ok += 1
                        time.sleep(0.05)
                    except Exception as ce:
                        print(f"  ⚠️  [{sym}] Gagal cancel orderId {oid}: {ce}")
                        sym_ok = False
                print(f"  🗑️  [{sym}] Per-orderId: {cancel_ok}/{len(all_open)} orders cancelled")
                # Bulk sebagai tambahan (catch-all jika ada order tidak muncul di openOrders)
                try:
                    api_delete("/fapi/v1/allOpenOrders", {"symbol": sym})
                except Exception:
                    pass
            except Exception as fe:
                print(f"  ⚠️  [{sym}] Gagal fetch openOrders: {fe}")
                # Last resort: coba bulk saja
                try:
                    api_delete("/fapi/v1/allOpenOrders", {"symbol": sym})
                    print(f"  🗑️  [{sym}] Bulk cancel (last resort) OK")
                except Exception as be:
                    print(f"  ⚠️  [{sym}] Bulk cancel juga gagal: {be}")
                    sym_ok = False

            # ── Step 2: cancel algo/conditional orders via algoOrder endpoint ─────
            # Beberapa SL/TP dikirim via algo endpoint (terpisah dari openOrders)
            if sym in stale_algo_by_symbol:
                algo_ids = stale_algo_by_symbol[sym]
                ok = 0
                try:
                    api_delete("/fapi/v1/algoOrder/openOrders", {"symbol": sym})
                    ok = len(algo_ids)
                    print(f"  🗑️  [{sym}] Bulk algo cancel OK ({ok} orders)")
                except Exception:
                    for algo_id in algo_ids:
                        try:
                            api_delete("/fapi/v1/algoOrder", {"symbol": sym, "algoId": algo_id})
                            ok += 1
                            time.sleep(0.05)
                        except Exception as fe:
                            print(f"  ⚠️  [{sym}] Gagal cancel algoId {algo_id}: {fe}")
                            sym_ok = False
                print(f"  🗑️  [{sym}] {ok}/{len(algo_ids)} algo orders cancelled")

            if sym_ok:
                cancelled_ok.append(sym)
            else:
                cancelled_fail.append(sym)

        pending_note = (
            f"\n⏳ Pending limit (tidak disentuh): {sorted(symbols_pending_limit)}"
            if symbols_pending_limit else ""
        )
        fail_note = (
            f"\n⚠️ Gagal cancel: {sorted(cancelled_fail)}"
            if cancelled_fail else ""
        )
        send_telegram_raw(
            f"🧹 <b>Stale Orders Dibersihkan</b>\n"
            f"{'─'*34}\n"
            f"✅ Berhasil cancel: <b>{len(cancelled_ok)}</b> pair\n"
            + "\n".join([f"  🗑️ {s}" for s in sorted(cancelled_ok)])
            + fail_note + pending_note + "\n"
            f"{'─'*34}\n"
            f"Total orphan ditemukan: {len(all_stale)}"
        )

    except Exception as e:
        print(f"  ⚠️  cleanup_stale_orders error: {e}")
        try:
            send_telegram_raw(f"❌ <b>Cleanup Orders Gagal</b>\nError: <code>{e}</code>")
        except Exception:
            pass


def algo_post_sl(symbol: str, side: str, trigger_price: float,
                 quantity: float = 0.0,
                 working_type: str = "MARK_PRICE",
                 position_side: str = "BOTH") -> dict:
    """
    Kirim SL order via /fapi/v1/algoOrder (CONDITIONAL STOP_MARKET).

    Strategi: pakai quantity + reduceOnly=true, BUKAN closePosition=true.
    Alasan: closePosition=true memicu -4130 jika sudah ada order sejenis.
    Dengan quantity+reduceOnly tidak ada konflik.
    Dengan quantity+reduceOnly tidak ada konflik, tidak perlu cancel dulu.

    Jika quantity=0 (tidak diketahui), fallback ke closePosition sekali saja
    setelah cancel allOpenOrders (yang memang tersedia).
    """
    filters   = get_lot_filters(symbol)
    tick_size = filters.get("tickSize", 0.01)   # sudah di-koreksi via pricePrecision
    step_size = filters.get("stepSize", 0.001)
    _sl_qty_prec = filters.get("quantityPrecision", None)
    # Derive qty_prec dari stepSize jika tidak tersedia (testnet sering tidak return quantityPrecision)
    if _sl_qty_prec is None and step_size > 0:
        _mkt_step = filters.get("mkt_stepSize", step_size)
        _eff_step = _mkt_step if _mkt_step > 0 else step_size
        _sl_qty_prec = max(0, round(-math.log10(_eff_step)))
    if _sl_qty_prec is not None:
        quantity = round(math.floor(quantity * (10 ** _sl_qty_prec)) / (10 ** _sl_qty_prec), _sl_qty_prec)
    direction = "LONG" if side == "SELL" else "SHORT"
    trigger_type = "MARK_PRICE" if working_type == "MARK_PRICE" else "CONTRACT_PRICE"

    def _safe_trigger(price: float) -> str:
        """Validasi harga trigger vs mark price, lalu format ke string presisi."""
        try:
            mark = get_current_price(symbol)
            if mark is not None:
                if direction == "LONG" and price >= mark:
                    price = round_price_to_tick(mark * 0.994, tick_size)
                elif direction == "SHORT" and price <= mark:
                    price = round_price_to_tick(mark * 1.006, tick_size)
        except Exception:
            pass
        return format_price(price, tick_size)

    # ── Helper: pasang SL via endpoint standard /fapi/v1/order ──────────────
    def _post_sl_standard(trigger_price: float) -> dict:
        """
        Pasang STOP_MARKET via endpoint standard Binance Futures.
        Lebih kompatibel dari algoOrder — tersedia di semua akun termasuk testnet.
        """
        sl_trigger = _safe_trigger(trigger_price)
        _params = {
            "symbol":           symbol,
            "side":             side,
            "type":             "STOP_MARKET",
            "stopPrice":        sl_trigger,
            "workingType":      working_type,
            "timeInForce":      "GTC",
        }
        if quantity > 0:
            _params["quantity"] = format_qty(quantity, step_size, _sl_qty_prec)
            if position_side != "BOTH":
                _params["positionSide"] = position_side
            else:
                _params["reduceOnly"] = "true"
        else:
            _params["closePosition"] = "true"
            if position_side != "BOTH":
                _params["positionSide"] = position_side
        r = api_post("/fapi/v1/order", _params)
        return r

    # ── Coba algoOrder dulu, fallback ke standard jika tidak support ─────────
    if quantity > 0:
        buffers = [0.000, 0.005, 0.010, 0.015]
        algo_supported = True   # asumsi support, akan di-set False jika -4120
        for i, _buf in enumerate(buffers):
            try:
                mark = get_current_price(symbol) or trigger_price
                adj_price = trigger_price
                if _buf > 0:
                    adj_price = adjust_sl_price(trigger_price, mark, direction, tick_size, buffer_pct=_buf)
                sl_trigger = _safe_trigger(adj_price)

                if not algo_supported:
                    # AlgoOrder tidak support → langsung pakai standard
                    return _post_sl_standard(adj_price)

                _sl_params = {
                    "algoType":     "CONDITIONAL",
                    "symbol":       symbol,
                    "side":         side,
                    "type":         "STOP_MARKET",
                    "triggerPrice": sl_trigger,
                    "triggerType":  trigger_type,
                    "quantity":     format_qty(quantity, step_size, _sl_qty_prec),
                }
                if position_side != "BOTH":
                    _sl_params["positionSide"] = position_side
                else:
                    _sl_params["reduceOnly"] = "true"
                params = sign_request(_sl_params)
                r = requests.post(get_base_url() + "/fapi/v1/algoOrder", params=params, headers=get_headers())
                if r.status_code == 200:
                    return r.json()
                err = r.json() if "application/json" in r.headers.get("Content-Type", "") else {"code": 0, "msg": r.text}
                code = err.get("code", 0)
                # -4120 = algoOrder tidak support di akun ini → fallback ke standard
                if code in (-4120, -1121, -4003):
                    print(f"  ⚠️  algoOrder tidak support [{symbol}] (code {code}) — fallback ke STOP_MARKET standard")
                    algo_supported = False
                    return _post_sl_standard(adj_price)
                print(f"  ⚠️  algoOrder {r.status_code} [{symbol}] attempt {i}: {err.get('msg', r.text)}")
                if i < len(buffers) - 1:
                    time.sleep(0.3)
                    continue
                # Semua buffer habis → coba standard sebagai last resort
                print(f"  ⚠️  algoOrder semua buffer gagal [{symbol}] — fallback ke standard")
                return _post_sl_standard(trigger_price)
            except Exception as e:
                if i < len(buffers) - 1:
                    print(f"  ⚠️  algoOrder exception [{symbol}] attempt {i}: {e}")
                    time.sleep(0.3)
                else:
                    print(f"  ⚠️  algoOrder exception final [{symbol}]: {e} — fallback ke standard")
                    return _post_sl_standard(trigger_price)
        raise RuntimeError(f"algo_post_sl gagal [{symbol}]")

    # ── Fallback: quantity tidak diketahui → standard closePosition ──────────
    return _post_sl_standard(trigger_price)


def algo_post_tp(symbol: str, side: str, trigger_price: float, quantity: float,
                 working_type: str = "MARK_PRICE",
                 position_side: str = "BOTH") -> dict:
    """
    Pasang TP order via /fapi/v1/algoOrder (CONDITIONAL TAKE_PROFIT_MARKET).

    Perbaikan vs versi lama:
    1. Fallback ke standard /fapi/v1/order jika algoOrder tidak support (-4120, testnet)
    2. Re-sign SETELAH update triggerPrice (bukan sebelum) — cegah stale params
    3. Pakai mkt_stepSize untuk format_qty (TP adalah market order saat trigger)
    4. Guard: quantity tidak boleh < min_qty setelah floor
    """
    trigger_type  = "MARK_PRICE" if working_type == "MARK_PRICE" else "CONTRACT_PRICE"
    filters       = get_lot_filters(symbol)
    tick_size     = filters.get("tickSize",    0.01)
    step_size     = filters.get("stepSize",    0.001)
    mkt_step_size = filters.get("mkt_stepSize", step_size)   # FIX: pakai market lot step
    if mkt_step_size <= 0:
        mkt_step_size = step_size
    min_qty       = filters.get("minQty",      0.001)
    _tp_qty_prec  = filters.get("quantityPrecision", None)
    # Derive qty_prec dari mkt_stepSize jika tidak tersedia (testnet sering tidak return quantityPrecision)
    if _tp_qty_prec is None and mkt_step_size > 0:
        _tp_qty_prec = max(0, round(-math.log10(mkt_step_size)))

    # Floor quantity ke presisi exchange
    if _tp_qty_prec is not None:
        quantity = round(math.floor(quantity * (10 ** _tp_qty_prec)) / (10 ** _tp_qty_prec), _tp_qty_prec)

    # Guard: quantity tidak boleh 0 atau di bawah minQty
    if quantity < min_qty:
        quantity = round_lot_to_step(min_qty, mkt_step_size)
        if _tp_qty_prec is not None:
            quantity = round(math.floor(quantity * (10 ** _tp_qty_prec)) / (10 ** _tp_qty_prec), _tp_qty_prec)

    direction_str = "LONG" if side == "SELL" else "SHORT"  # side = sisi close

    # ── Helper: pasang TP via standard endpoint (fallback jika algoOrder tidak support) ──
    def _post_tp_standard(tp_price: float) -> dict:
        """
        Pasang TAKE_PROFIT_MARKET via /fapi/v1/order standard.
        Dipakai saat algoOrder tidak support (testnet, akun tanpa fitur algo).
        """
        _p = {
            "symbol":      symbol,
            "side":        side,
            "type":        "TAKE_PROFIT_MARKET",
            "stopPrice":   format_price(tp_price, tick_size),
            "workingType": working_type,
            "timeInForce": "GTC",
            "quantity":    format_qty(quantity, mkt_step_size, _tp_qty_prec),
        }
        if position_side != "BOTH":
            _p["positionSide"] = position_side
        else:
            _p["reduceOnly"] = "true"
        return api_post("/fapi/v1/order", _p)

    # ── Coba algoOrder dulu, fallback ke standard jika -4120 ─────────────────
    algo_supported = True
    buffers        = [0.000, 0.003, 0.006, 0.010]
    last_err       = None

    for _i, _buf in enumerate(buffers):
        try:
            # Hitung adjusted trigger price (sebelum sign!)
            adj_price = trigger_price
            if _buf > 0:
                mark      = get_current_price(symbol) or trigger_price
                adj_price = adjust_tp_price(trigger_price, mark, direction_str, tick_size, buffer_pct=_buf)

            if not algo_supported:
                # AlgoOrder tidak support → langsung fallback ke standard
                return _post_tp_standard(adj_price)

            _tp_params = {
                "algoType":     "CONDITIONAL",
                "symbol":       symbol,
                "side":         side,
                "type":         "TAKE_PROFIT_MARKET",
                "triggerPrice": format_price(adj_price, tick_size),   # FIX: sign SETELAH update
                "triggerType":  trigger_type,
                "quantity":     format_qty(quantity, mkt_step_size, _tp_qty_prec),
            }
            if position_side != "BOTH":
                _tp_params["positionSide"] = position_side
            else:
                _tp_params["reduceOnly"] = "true"

            # FIX: sign params SETELAH triggerPrice diupdate, bukan sebelumnya
            params = sign_request(_tp_params)
            _r = requests.post(get_base_url() + "/fapi/v1/algoOrder", params=params, headers=get_headers())

            if _r.status_code == 200:
                return _r.json()

            last_err = _r.json() if "application/json" in _r.headers.get("Content-Type", "") else {"msg": _r.text}
            code     = last_err.get("code", 0)

            # -4120 / -1121 / -4003 = algoOrder tidak support → fallback ke standard
            if code in (-4120, -1121, -4003):
                print(f"  ⚠️  algo_post_tp: algoOrder tidak support [{symbol}] (code {code}) — fallback ke standard TAKE_PROFIT_MARKET")
                algo_supported = False
                return _post_tp_standard(adj_price)

            print(f"  ⚠️  algo_post_tp {_r.status_code} [{symbol}] attempt {_i}: {last_err.get('msg', _r.text)}")
            if _i < len(buffers) - 1:
                time.sleep(0.3)
                continue
            # Semua buffer habis → coba standard sebagai last resort
            print(f"  ⚠️  algo_post_tp semua buffer gagal [{symbol}] — fallback ke standard")
            return _post_tp_standard(trigger_price)

        except Exception as _te:
            if _i < len(buffers) - 1:
                time.sleep(0.3)
                continue
            # Last attempt gagal → coba standard satu kali lagi sebelum raise
            try:
                print(f"  ⚠️  algo_post_tp exception [{symbol}] attempt {_i}: {_te} — fallback ke standard")
                return _post_tp_standard(trigger_price)
            except Exception as _se:
                raise RuntimeError(f"algo_post_tp gagal total [{symbol}]: algoOrder={_te} standard={_se}")

    raise RuntimeError(f"algo_post_tp gagal [{symbol}]: {last_err}")


def get_contract_multiplier(symbol: str) -> int:
    """
    Pair dengan prefix '1000' di Binance Futures artinya 1 kontrak = 1000 unit token.
    markPrice yang dikembalikan Binance adalah harga per unit token (sangat kecil),
    tapi lot/qty yang dikirim ke order adalah dalam satuan kontrak.
    Jadi notional sesungguhnya = qty_kontrak × markPrice × multiplier.
    Contoh: 1000PEPEUSDT → multiplier=1000
            1000SHIBUSDT → multiplier=1000
    Pair normal → multiplier=1
    """
    if symbol.startswith("1000"):
        return 1000
    return 1


def get_current_price(symbol: str) -> float | None:
    try:
        data = api_get("/fapi/v1/premiumIndex", {"symbol": symbol})
        return float(data["markPrice"])
    except Exception:
        try:
            data = api_get("/fapi/v1/ticker/price", {"symbol": symbol})
            return float(data["price"])
        except Exception:
            return None


def adjust_sl_price(sl: float, mark_price: float, direction: str, tick_size: float,
                    buffer_pct: float = 0.005) -> float:
    min_ticks = max(1, math.ceil(mark_price * buffer_pct / tick_size))
    if direction == "LONG":
        ceiling = round_price_to_tick(mark_price - tick_size * min_ticks, tick_size)
        if sl >= ceiling:
            sl = ceiling
    else:
        floor_ = round_price_to_tick(mark_price + tick_size * min_ticks, tick_size)
        if sl <= floor_:
            sl = floor_
    return round_price_to_tick(sl, tick_size)


def adjust_tp_price(tp: float, mark_price: float, direction: str, tick_size: float,
                    buffer_pct: float = 0.002) -> float:
    min_ticks = max(1, math.ceil(mark_price * buffer_pct / tick_size))
    if direction == "LONG":
        floor_ = round_price_to_tick(mark_price + tick_size * min_ticks, tick_size)
        if tp < floor_:
            tp = floor_
    else:
        ceiling = round_price_to_tick(mark_price - tick_size * min_ticks, tick_size)
        if tp > ceiling:
            tp = ceiling
    return round_price_to_tick(tp, tick_size)


def execute_trade(signal: dict, mode: dict):
    if not AUTO_TRADING:
        return
    if bot_paused:
        print("⏸ Bot paused — skip trade")
        return  # no tele notif for paused (too frequent)

    symbol  = signal["pair"]
    side    = "BUY" if signal["direction"] == "LONG" else "SELL"
    sl_side = "SELL" if side == "BUY" else "BUY"
    direction = signal["direction"]

    if has_position(symbol):
        print("⛔ Already in position")
        return  # silent: normal dedup

    dynamic_max = get_dynamic_max_trades()
    current_open = count_open_positions()
    if current_open >= dynamic_max:
        print(f"⛔ Max trades reached ({current_open}/{dynamic_max})")
        return  # silent: normal capacity limit

    balance  = get_total_balance()   # pakai total balance (konsisten, tidak turun tiap trade buka)

    # ── Ambil filter Binance lebih dulu (dibutuhkan untuk kalkulasi leverage) ──
    _pre_filters      = get_lot_filters(symbol)
    _pre_min_notional = _pre_filters.get("minNotional", 5.0)

    available_bal = get_balance()
    if available_bal <= 0:
        _msg = f"⛔ <b>SKIP ENTRY — {symbol}</b>\nBalance tidak cukup untuk entry."
        print(f"⛔ [{symbol}] available balance={available_bal:.2f} tidak cukup, skip")
        send_telegram_raw(_msg)
        return

    # ── MAX SL LOSS: Lot disesuaikan agar loss jika SL kena ≤ MAX_SL_LOSS_PCT × balance ──
    #
    # Logika baru (SL-based sizing):
    #   1. max_loss_usdt = MAX_SL_LOSS_PCT × total_balance
    #      (misal 1% × $10.000 = $100 — ini kerugian MAKSIMAL jika SL kena)
    #   2. sl_distance = |entry - stop_loss|  (dalam unit harga)
    #   3. lot = max_loss_usdt / sl_distance
    #      → berapa unit agar loss aktual saat SL kena = max_loss_usdt
    #   4. Lot di-clamp ke exchange limits dan cap notional
    #
    # Tujuan: berapa pun jarak SL-nya, LOSS MAKSIMAL TETAP = X% dari balance.
    # Lot otomatis menyesuaikan: SL dekat → lot besar, SL jauh → lot kecil.
    _entry       = signal["entry"]
    _sl          = signal["stop_loss"]
    _sl_dist_abs = abs(_entry - _sl)
    _sl_dist_pct = _sl_dist_abs / max(_entry, 1e-9)

    if FIXED_LEVERAGE > 0:
        trade_leverage = FIXED_LEVERAGE
    else:
        trade_leverage = get_leverage_for_price(_entry)

    # ── Hitung lot dari max SL loss ───────────────────────────────────────────
    max_loss_usdt = balance * MAX_SL_LOSS_PCT            # kerugian maks jika SL kena
    min_sl_dist   = _entry * MIN_SL_DISTANCE_PCT         # jarak SL minimum (anti-noise)
    eff_sl_dist   = max(_sl_dist_abs, min_sl_dist)       # pakai yang lebih besar
    raw_lot       = max_loss_usdt / max(eff_sl_dist, 1e-9)

    # ── Cap notional agar tidak over-margin ──────────────────────────────────
    # Meski lot dari SL bisa besar, notional (posisi) tidak boleh melebihi
    # available_bal × leverage × MAX_RISK_PER_TRADE_USDT_RATIO
    _margin_cap      = available_bal * MAX_RISK_PER_TRADE_USDT_RATIO
    _max_notional    = _margin_cap * trade_leverage
    _lot_cap_notional = _max_notional / max(_entry, 1e-9)
    if raw_lot > _lot_cap_notional:
        print(f"  ⚠️  [{symbol}] lot dari SL={raw_lot:.4f} > cap notional={_lot_cap_notional:.4f} → di-cap")
        raw_lot = _lot_cap_notional

    # Margin aktual yang akan digunakan (untuk log/info)
    margin_usdt     = (raw_lot * _entry) / max(trade_leverage, 1)
    target_notional = raw_lot * _entry
    print(
        f"  🎯 MAX SL LOSS: {MAX_SL_LOSS_PCT*100:.2f}% | max_loss={max_loss_usdt:.2f} USDT | "
        f"sl_dist={eff_sl_dist:.4f} ({_sl_dist_pct*100:.2f}%) | "
        f"lev={trade_leverage}x | lot={raw_lot:.4f} | notional={target_notional:.2f} | margin={margin_usdt:.2f}"
    )

    # ── Pre-check minNotional dengan leverage yang sudah ditetapkan ──────────
    _pre_capacity = available_bal * trade_leverage * MAX_RISK_PER_TRADE_USDT_RATIO
    if _pre_min_notional > _pre_capacity * 1.05:
        print(f"  ⚡ [{symbol}] PRE-CHECK skip: minNotional={_pre_min_notional} > capacity={_pre_capacity:.2f}")
        return

    print(f"  📐 [{symbol}] max_loss={max_loss_usdt:.2f} USDT | sl_dist={eff_sl_dist:.4f} × {trade_leverage}x → notional={target_notional:.2f} → raw_lot={raw_lot:.4f}")

    # ── Re-fetch filter real-time untuk symbol ini ──────────────────────────
    # Cache mungkin stale atau MARKET_LOT_SIZE berubah — query langsung lebih aman
    try:
        rt_info = api_get("/fapi/v1/exchangeInfo", {"symbol": symbol})
        sym_list = rt_info.get("symbols", [])
        if sym_list:
            rt_filters = _parse_symbol_filters(sym_list[0])
            if rt_filters:
                _exchange_info_cache[symbol] = rt_filters
                print(f"  🔄 Real-time filters {symbol}: {rt_filters}")
    except Exception as _rte:
        print(f"  ⚠️  Real-time filter fetch gagal {symbol}: {_rte} — pakai cache")

    filters      = get_lot_filters(symbol)
    step_size    = filters.get("stepSize",    1.0)
    mkt_step_size = filters.get("mkt_stepSize", step_size)  # MARKET_LOT_SIZE.stepSize, bisa beda dari LOT_SIZE
    if mkt_step_size <= 0:
        mkt_step_size = step_size  # fallback jika mkt_stepSize tidak tersedia atau 0
    min_qty      = filters.get("minQty",      1.0)
    max_qty      = filters.get("maxQty",      9999999.0)
    min_notional = filters.get("minNotional", 5.0)
    tick_size    = filters.get("tickSize",    0.0001)
    qty_prec     = filters.get("quantityPrecision", None)  # hard cap desimal dari Binance

    # ═══════════════════════════════════════════════════════════════════════
    # LOT SIZING — balance-first approach:
    # 1. available_bal & margin_usdt sudah dihitung di atas
    # 2. Cek apakah pair bisa di-trade (lot_maxQty × entry >= minNotional)
    # 3. Cek apakah balance cukup untuk minNotional
    # 4. Jika notional < minNotional, naikkan margin sampai bisa (jika masih aman)
    # 5. Clamp lot ke exchange limits (minQty, maxQty, mkt_maxQty)
    # ═══════════════════════════════════════════════════════════════════════

    raw_mkt_max   = filters.get("mkt_maxQty", max_qty)
    lot_max       = filters.get("lot_maxQty", max_qty)

    # ── Hard cap raw_lot ke lot_maxQty SEBELUM processing apapun ─────────────
    # Ini mencegah lot blow-up (seperti THETA 1,103,080 padahal maxQty jauh lebih kecil)
    # yang terjadi saat leverage tinggi × balance besar menghasilkan raw_lot > exchange limit.
    if raw_lot > lot_max:
        print(f"  🔒 [{symbol}] raw_lot={raw_lot:.4f} > lot_maxQty={lot_max} → di-cap ke {lot_max}")
        raw_lot = lot_max
        # Recalculate target_notional agar konsisten
        target_notional = raw_lot * signal["entry"]

    # ── Tentukan effective_max untuk MARKET order ─────────────────────────────
    # Jika mkt_maxQty × harga entry < minNotional, ini kontradiksi Binance:
    # MARKET_LOT_SIZE.maxQty terlalu kecil (limit HFT/algo) → gunakan lot_maxQty sebagai cap.
    # Juga jika mkt_maxQty tidak ada / 0, fallback ke lot_maxQty.
    if raw_mkt_max <= 0 or (raw_mkt_max * signal["entry"] < min_notional):
        effective_max = lot_max
        print(f"  ℹ️  [{symbol}] mkt_maxQty={raw_mkt_max} × entry={signal['entry']:.4f} < minNotional={min_notional} → gunakan lot_maxQty={lot_max}")
    else:
        effective_max = min(lot_max, raw_mkt_max)

    # ── Cek apakah pair bisa di-trade sama sekali ────────────────────────────
    _cmult = get_contract_multiplier(symbol)
    max_achievable_notional = lot_max * signal["entry"] * _cmult
    if max_achievable_notional < min_notional:
        _msg = f"⛔ <b>SKIP ENTRY — {symbol}</b>\nPair tidak bisa di-trade: lot_maxQty={lot_max} × entry={signal['entry']:.4f} = {max_achievable_notional:.2f} < minNotional={min_notional}"
        print(f"⛔ [{symbol}] max_achievable={max_achievable_notional:.2f} < minNotional={min_notional}, skip")
        send_telegram_raw(_msg)
        record_pair_fail(symbol)
        return

    # ── Cek kapasitas balance vs minNotional ─────────────────────────────────
    # Gunakan max dari available_bal dan (total_balance × 80%) untuk kapasitas
    # Ini mencegah skip karena available_bal sedikit di bawah threshold
    actual_capacity = max(available_bal, balance * 0.80) * trade_leverage
    if min_notional > actual_capacity:
        _msg = f"⛔ <b>SKIP ENTRY — {symbol}</b>\nBalance tidak cukup untuk minNotional={min_notional}\n(total={balance:.2f} × {trade_leverage}x = {actual_capacity:.2f})\nTambah balance atau pilih pair dengan minNotional lebih kecil."
        print(f"⛔ [{symbol}] minNotional={min_notional} > kapasitas={actual_capacity:.2f} (total_bal={balance:.2f}), skip")
        send_telegram_raw(_msg)
        record_pair_fail(symbol)
        return

    # ── Sesuaikan target_notional dengan minNotional jika perlu ──────────────
    # Kalau margin dari risk% menghasilkan notional < minNotional,
    # naikkan notional ke minNotional × 1.15 buffer (15% buffer dari minimum).
    # Ini memastikan setelah rounding step_size, notional masih di atas minimum.
    if target_notional < min_notional:
        # Hitung kapasitas penuh — pakai total balance bukan available_bal
        # karena untuk 1 posisi, total balance ≈ available balance
        full_capacity = available_bal * trade_leverage
        adjusted_notional = min(min_notional * 1.15, full_capacity * 0.95)
        if adjusted_notional < min_notional:
            adjusted_notional = min_notional * 1.15  # paksa naik, cek kapasitas di bawah
        print(f"  ⚠️  [{symbol}] target_notional={target_notional:.2f} < minNotional={min_notional} → naik ke {adjusted_notional:.2f}")
        raw_lot = adjusted_notional / max(signal["entry"], 1e-9)

    # Clamp raw_lot ke effective_max
    capped_lot = min(raw_lot, effective_max)
    lot = round_lot_to_step(capped_lot, step_size)

    # ── Cek notional vs minNotional ──────────────────────────────────────────
    notional = lot * signal["entry"]
    if notional < min_notional:
        # Setelah rounding step_size, lot mungkin turun sedikit di bawah minNotional.
        # Naikkan lot ke nilai minimum yang memenuhi minNotional.
        lot_needed  = round_lot_up_to_step(
            math.ceil(min_notional / max(signal["entry"], 1e-9) / step_size) * step_size,
            step_size
        )
        notional_up = lot_needed * signal["entry"]
        within_cap  = lot_needed <= effective_max and lot_needed * signal["entry"] <= actual_capacity
        if notional_up >= min_notional and within_cap:
            print(f"  ℹ️  [{symbol}] lot {lot}→{lot_needed} (naik agar penuhi minNotional={min_notional})")
            lot      = lot_needed
            notional = notional_up
        else:
            min_bal_needed = min_notional / trade_leverage
            _msg = f"⛔ <b>SKIP ENTRY — {symbol}</b>\nNotional={notional:.2f} < minNotional={min_notional}\nButuh balance ≥${min_bal_needed:.0f} untuk pair ini.\n(available={available_bal:.2f} | leverage={trade_leverage}x)"
            print(f"⛔ [{symbol}] notional={notional:.2f} < minNotional={min_notional}, skip")
            send_telegram_raw(_msg)
            record_pair_fail(symbol)
            return

    if lot < min_qty:
        _msg = f"⛔ <b>SKIP ENTRY — {symbol}</b>\nLot={lot} < minQty={min_qty} (lot terlalu kecil)"
        print(f"⛔ [{symbol}] lot={lot} < minQty={min_qty}, skip")
        send_telegram_raw(_msg)
        record_pair_fail(symbol)
        return

    # ── Round lot ke MARKET_LOT_SIZE.stepSize sebelum kirim ke Binance ──────
    # Selalu lakukan ini — mkt_step_size bisa sama atau lebih besar dari step_size.
    # format_qty harus pakai mkt_step_size agar Binance tidak reject -1111.
    lot = round_lot_to_step(lot, mkt_step_size)
    # ── Enforce quantityPrecision (hard cap desimal dari Binance) ─────────────
    # Contoh: RENDERUSDT quantityPrecision=0 → qty HARUS bulat, meski stepSize=0.001.
    # Tanpa ini, qty seperti 50.113 dikirim ke Binance → error -1111.
    if qty_prec is not None:
        lot = round(math.floor(lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
    notional = lot * signal["entry"]

    # Setelah round mkt_step, cek ulang — boleh naik 1 step lagi jika perlu
    if notional < min_notional:
        lot_up2 = round_lot_up_to_step(lot + mkt_step_size * 0.5, mkt_step_size)
        # Enforce qty_prec dulu sebelum cek — lot_up2 harus valid secara presisi
        if qty_prec is not None:
            lot_up2 = round(math.floor(lot_up2 * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
        if lot_up2 <= effective_max and lot_up2 * signal["entry"] >= min_notional:
            lot      = lot_up2
            notional = lot * signal["entry"]
    # Enforce qty_prec sekali lagi setelah semua penyesuaian lot selesai
    if qty_prec is not None:
        lot = round(math.floor(lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
        notional = lot * signal["entry"]
    if lot < min_qty or notional < min_notional:
        _msg = f"⛔ <b>SKIP ENTRY — {symbol}</b>\nSetelah rounding: lot={lot} notional={notional:.2f} tidak valid (mkt_step={mkt_step_size})"
        print(f"⛔ [{symbol}] setelah round mkt_step={mkt_step_size}: lot={lot} notional={notional:.2f} tidak valid, skip")
        send_telegram_raw(_msg)
        record_pair_fail(symbol)
        return

    print(f"  📤 LOT CALC {symbol}: raw={raw_lot:.4f} final={lot} | eff_max={effective_max} mkt_max={raw_mkt_max} notional={notional:.2f} margin={margin_usdt:.2f}")

    market_price = get_current_price(symbol)
    if market_price is None:
        _msg = f"⛔ <b>SKIP ENTRY — {symbol}</b>\nGagal ambil harga market. Cek koneksi API Binance."
        print(f"⛔ Gagal ambil harga market {symbol}, skip")
        send_telegram_raw(_msg)
        return

    # Validasi pair bisa di-trade (sudah gagal ≥3x berturut → cooldown sementara)
    if not validate_pair_tradeable(symbol, market_price):
        print(f"⛔ [{symbol}] di-skip sementara (cooldown gagal berturut) — tidak kirim notif")
        return   # silent skip — sudah ada notif dari record_pair_fail sebelumnya

    # ── Gunakan presisi natural harga, bukan tickSize dari PRICE_FILTER ──────
    # Binance algo/stop orders jauh lebih fleksibel soal presisi dibanding
    # limit orders. tickSize dari PRICE_FILTER (misal 0.1 atau 0.01) terlalu
    # kasar untuk harga seperti ADA 0.2437 — akan membulatkan jadi 0.2 atau 0.24.
    # Solusi: hitung presisi dari harga market itu sendiri (4-6 sig. digit).
    def smart_price_precision(price: float) -> int:
        """Hitung desimal presisi yang tepat berdasarkan harga."""
        if price <= 0:
            return 4
        if price >= 1000:  return 2
        if price >= 100:   return 2
        if price >= 10:    return 3
        if price >= 1:     return 4
        if price >= 0.1:   return 4
        if price >= 0.01:  return 5
        if price >= 0.001: return 6
        return 8

    price_prec = smart_price_precision(market_price)
    # Pastikan tidak lebih kasar dari tickSize asli exchange
    tick_prec  = max(0, round(-math.log10(tick_size))) if tick_size > 0 else 4
    eff_prec   = max(price_prec, tick_prec)

    def rnd(p): return round(p, eff_prec)

    entry = rnd(signal["entry"])
    sl    = rnd(signal["stop_loss"])
    tp1   = rnd(signal["take_profit"][0])
    tp2   = rnd(signal["take_profit"][1])

    # Debug: print nilai SL/TP vs entry & market sebelum validasi
    print(f"  🔍 SL/TP CHECK {symbol} [{direction}] entry={entry} sl={sl} tp1={tp1} tp2={tp2} market={market_price}")

    # ── Validasi jarak SL minimum (cegah SL terlalu tipis kena market noise) ────
    _sl_dist_pct = abs(entry - sl) / max(entry, 1e-9)
    if _sl_dist_pct < MIN_SL_DISTANCE_PCT:
        # SL terlalu dekat — perlebar secara otomatis ke minimum
        if direction == "LONG":
            sl = rnd(entry * (1.0 - MIN_SL_DISTANCE_PCT))
        else:
            sl = rnd(entry * (1.0 + MIN_SL_DISTANCE_PCT))
        print(f"  ⚠️  [{symbol}] SL terlalu dekat ({_sl_dist_pct*100:.3f}% < {MIN_SL_DISTANCE_PCT*100:.1f}%) — diperlebar ke {sl}")
        # ── Recalc TP agar RR tetap valid setelah SL diperlebar ───────────────
        _new_sl_dist = abs(entry - sl)
        if direction == "LONG":
            if tp1 <= entry + _new_sl_dist * RR_GRADE_B:
                tp1 = rnd(entry + _new_sl_dist * RR_GRADE_B)
            if tp2 <= tp1:
                tp2 = rnd(entry + _new_sl_dist * (RR_GRADE_A + 0.5))
        else:
            if tp1 >= entry - _new_sl_dist * RR_GRADE_B:
                tp1 = rnd(entry - _new_sl_dist * RR_GRADE_B)
            if tp2 >= tp1:
                tp2 = rnd(entry - _new_sl_dist * (RR_GRADE_A + 0.5))

    if direction == "LONG":
        if sl >= entry:
            _msg = f"⛔ <b>SKIP ENTRY — {symbol} LONG</b>\nSL invalid: sl={sl} >= entry={entry}"
            print(f"⛔ SL/TP invalid LONG {symbol}: sl={sl} >= entry={entry}, skip")
            send_telegram_raw(_msg)
            return
        if tp1 <= entry or tp2 <= entry or tp2 <= tp1:
            min_move = entry * 0.003
            tp1 = rnd(entry + min_move * RR_GRADE_B)
            tp2 = rnd(entry + min_move * (RR_GRADE_A + 0.5))
            if tp1 <= entry or tp2 <= entry or tp2 <= tp1:
                _msg = f"⛔ <b>SKIP ENTRY — {symbol} LONG</b>\nTP invalid: tp1={tp1} tp2={tp2} vs entry={entry}"
                print(f"⛔ SL/TP invalid LONG {symbol}: tp1={tp1} tp2={tp2} vs entry={entry}, skip")
                send_telegram_raw(_msg)
                return
    else:
        if sl <= entry:
            _msg = f"⛔ <b>SKIP ENTRY — {symbol} SHORT</b>\nSL invalid: sl={sl} <= entry={entry}"
            print(f"⛔ SL/TP invalid SHORT {symbol}: sl={sl} <= entry={entry}, skip")
            send_telegram_raw(_msg)
            return
        if tp1 >= entry or tp2 >= entry or tp2 >= tp1:
            # Rekalkulasi TP dengan buffer minimal dari entry
            min_move = entry * 0.003  # minimal 0.3% dari entry
            tp1 = rnd(entry - min_move * RR_GRADE_B)
            tp2 = rnd(entry - min_move * (RR_GRADE_A + 0.5))
            if tp1 >= entry or tp2 >= entry or tp2 >= tp1:
                _msg = f"⛔ <b>SKIP ENTRY — {symbol} SHORT</b>\nTP invalid: tp1={tp1} tp2={tp2} vs entry={entry}"
                print(f"⛔ SL/TP invalid SHORT {symbol}: tp1={tp1} tp2={tp2} vs entry={entry}, skip")
                send_telegram_raw(_msg)
                return

    # ── Guard: TP1 tidak boleh sama atau lebih buruk dari SL ────────────────
    # Bug: bisa terjadi saat SL distance sangat kecil dan RR recalc menghasilkan
    # TP1 yang jatuh ke level SL atau bahkan di bawah entry (untuk LONG).
    _sl_dist_fix = abs(entry - sl)
    if direction == "LONG" and tp1 <= sl:
        tp1 = rnd(entry + _sl_dist_fix * RR_GRADE_B)
        tp2 = rnd(entry + _sl_dist_fix * (RR_GRADE_A + 0.5))
        print(f"  ⚠️  [{symbol}] FIX: tp1 was <= sl → recalc tp1={tp1} tp2={tp2}")
    elif direction == "SHORT" and tp1 >= sl:
        tp1 = rnd(entry - _sl_dist_fix * RR_GRADE_B)
        tp2 = rnd(entry - _sl_dist_fix * (RR_GRADE_A + 0.5))
        print(f"  ⚠️  [{symbol}] FIX: tp1 was >= sl → recalc tp1={tp1} tp2={tp2}")

    # Guard final: pastikan TP1 dan TP2 masih valid setelah fix
    if direction == "LONG":
        if tp1 <= entry:
            tp1 = rnd(entry + _sl_dist_fix * RR_GRADE_B)
        if tp2 <= tp1:
            tp2 = rnd(tp1 + _sl_dist_fix * 0.5)
    else:
        if tp1 >= entry:
            tp1 = rnd(entry - _sl_dist_fix * RR_GRADE_B)
        if tp2 >= tp1:
            tp2 = rnd(tp1 - _sl_dist_fix * 0.5)

    try:
        # ── Bersihkan conditional orders lama sebelum buka posisi baru ──────────
        # Skenario: posisi sebelumnya sudah close (TP/SL kena atau manual) tapi
        # conditional order TP/SL-nya belum ikut terhapus di Binance.
        # Jika langsung buka posisi baru tanpa clean-up → TP/SL lama masih aktif
        # dengan struktur/harga yang sudah tidak valid untuk posisi baru.
        # Solusi: cancel semua open orders symbol ini sebelum apapun.
        try:
            cancel_open_orders(symbol)
            print(f"  🧹 [{symbol}] Pre-entry cleanup: conditional orders lama dibersihkan.")
        except Exception as _pre_cancel_err:
            print(f"  ⚠️  [{symbol}] Pre-entry cleanup gagal (lanjut): {_pre_cancel_err}")

        set_leverage(symbol, trade_leverage)

        hedge = is_hedge_mode()
        position_side = ("SHORT" if side == "SELL" else "LONG") if hedge else "BOTH"
        # ── Re-fetch quantityPrecision langsung dari exchange sebelum kirim ──────
        # Binance bisa update filter kapan saja — pakai nilai paling fresh.
        # Ini mencegah -1111 akibat qty_prec stale di cache.
        try:
            _live_info = api_get("/fapi/v1/exchangeInfo", {"symbol": symbol})
            _live_syms = _live_info.get("symbols", [])
            if _live_syms:
                _live_qty_prec = _live_syms[0].get("quantityPrecision", None)
                if _live_qty_prec is not None:
                    _live_qty_prec = int(_live_qty_prec)
                    if qty_prec != _live_qty_prec:
                        print(f"  🔄 [{symbol}] quantityPrecision updated: {qty_prec} → {_live_qty_prec}")
                    qty_prec = _live_qty_prec
        except Exception as _qpe:
            print(f"  ⚠️  [{symbol}] gagal re-fetch qty_prec: {_qpe} — pakai cache qty_prec={qty_prec}")

        # Floor lot ulang sesuai presisi paling ketat sebelum format & kirim
        # Hitung presisi dari mkt_step_size terlebih dahulu
        _mkt_step_prec = max(0, round(-math.log10(mkt_step_size))) if mkt_step_size > 0 else 3
        # Ambil presisi paling ketat: min antara step precision dan qty_prec
        _final_prec = _mkt_step_prec if qty_prec is None else min(_mkt_step_prec, int(qty_prec))
        # Floor lot ke presisi final — BUKAN round, floor (Binance strict)
        _factor = 10 ** _final_prec
        lot = math.floor(lot * _factor) / _factor
        lot = round(lot, _final_prec)   # hapus floating-point noise setelah floor

        # ── Kirim LIMIT order (GTC) dengan auto-retry presisi ────────────────────
        #
        # Entry menggunakan LIMIT order di harga sinyal (bukan MARKET).
        # Jika tidak terisi dalam LIMIT_ORDER_TIMEOUT_MINUTES menit → otomatis
        # di-cancel oleh monitor_pending_limit_orders() di main loop.
        #
        # Layer retry:
        #   presisi (-1111): turunkan desimal lot satu per satu (final_prec → 0)
        #   lot-adjust (non-1111): ±5% margin, 10 variasi
        _order_ok   = False
        _order_resp = None
        _cur_lot    = lot
        _cur_prec   = _final_prec

        # Harga limit = harga sinyal (sudah di-rnd di atas)
        _limit_price     = entry
        _limit_price_str = format_price(_limit_price, tick_size)

        # Batas lot-adjustment ±30% (diperlebar dari ±5% agar pair mahal seperti BTC bisa lolos)
        # Untuk BTC harga ~$84.000, stepSize=0.001 → 1 lot = $84 ≈ minNotional $100
        # Perlu flex yang cukup agar lot bisa dinaikkan sampai penuhi minNotional
        _adj_margin_max = margin_usdt * 1.30
        _adj_margin_min = margin_usdt * 0.70

        # Fallback: jika margin_usdt × lev < minNotional, paksa target ke minNotional × 1.1
        _min_notional_lot = math.ceil(min_notional * 1.1 / max(signal["entry"], 1e-9) / step_size) * step_size
        _notional_from_margin = margin_usdt * trade_leverage

        _lot_adj_max = min((_adj_margin_max * trade_leverage) / max(signal["entry"], 1e-9), effective_max)
        _lot_adj_min = max(
            (_adj_margin_min * trade_leverage) / max(signal["entry"], 1e-9),
            min_qty,
            _min_notional_lot,  # pastikan selalu bisa penuhi minNotional
        )

        # Jika bahkan _lot_adj_max masih di bawah minNotional, paksa ke minNotional lot
        if _lot_adj_max * signal["entry"] < min_notional:
            _lot_adj_max = min(_min_notional_lot * 1.5, effective_max)
            print(f"  ⚠️  [{symbol}] lot_adj_max terlalu kecil → paksa ke {_lot_adj_max:.4f} (penuhi minNotional={min_notional})")

        _lot_adj_offsets = [0]
        for _s in range(1, 7):
            _lot_adj_offsets.append(+_s * 0.05)   # naik dulu (prioritas agar penuhi minNotional)
        for _s in range(1, 4):
            _lot_adj_offsets.append(-_s * 0.05)   # turun terakhir

        for _adj_idx, _offset_pct in enumerate(_lot_adj_offsets):
            _adj_notional = margin_usdt * (1.0 + _offset_pct) * trade_leverage
            _adj_lot_raw  = _adj_notional / max(signal["entry"], 1e-9)
            _adj_lot_raw  = max(min(_adj_lot_raw, _lot_adj_max), _lot_adj_min)
            _adj_lot = round_lot_to_step(_adj_lot_raw, mkt_step_size)
            if qty_prec is not None:
                _adj_lot = round(math.floor(_adj_lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
            if _adj_lot < min_qty:
                continue
            if _adj_lot * signal["entry"] < min_notional:
                continue

            _try_prec = _final_prec
            if qty_prec is not None:
                _try_prec = min(_final_prec, int(qty_prec))

            if _adj_idx > 0:
                print(f"  🔄 [{symbol}] LOT ADJUST [{_adj_idx}] offset={_offset_pct:+.1%} → lot={_adj_lot}")

            for _prec_attempt in range(_try_prec + 1):
                _cur_prec   = max(0, _try_prec - _prec_attempt)
                _cur_factor = 10 ** _cur_prec
                _cur_lot    = math.floor(_adj_lot * _cur_factor) / _cur_factor
                _cur_lot    = round(_cur_lot, _cur_prec)
                _qty_str    = f"{_cur_lot:.{_cur_prec}f}"
                notional    = _cur_lot * signal["entry"]

                order_params = {
                    "symbol":      symbol,
                    "side":        side,
                    "type":        "LIMIT",
                    "timeInForce": "GTC",
                    "quantity":    _qty_str,
                    "price":       _limit_price_str,
                }
                if hedge:
                    order_params["positionSide"] = position_side
                print(f"  📤 LIMIT ORDER: {symbol} | side={side} | qty={_qty_str} | price={_limit_price_str} | notional={notional:.2f} | lev={trade_leverage}x")
                try:
                    _o_params = sign_request(dict(order_params))
                    _o_r = requests.post(get_base_url() + "/fapi/v1/order", params=_o_params, headers=get_headers())
                    if _o_r.ok:
                        _order_resp = _o_r.json()
                        _order_ok   = True
                        if _prec_attempt > 0:
                            _exchange_info_cache.setdefault(symbol, {})["quantityPrecision"] = _cur_prec
                        break
                    _o_err = {}
                    try: _o_err = _o_r.json()
                    except Exception: pass
                    _o_code = _o_err.get("code", 0)
                    print(f"  ⚠️  LIMIT ORDER adj={_adj_idx} prec={_cur_prec} → HTTP {_o_r.status_code} code={_o_code} msg={_o_err.get('msg','?')}")
                    if _o_code != -1111:
                        break
                    if _cur_prec == 0:
                        break
                    time.sleep(0.2)
                except Exception:
                    raise

            if _order_ok:
                break

        if not _order_ok:
            raise RuntimeError(f"LIMIT ORDER gagal setelah precision + lot-adjust [{symbol}]")

        lot = _cur_lot
        _limit_order_id = _order_resp.get("orderId") if _order_resp else None

        # Reset fail counter
        if symbol in _untradeable_symbols:
            del _untradeable_symbols[symbol]

        tp1_size = round_lot_to_step(lot * TP1_PARTIAL, mkt_step_size)
        tp2_size = round_lot_to_step(lot - tp1_size,    mkt_step_size)
        if tp1_size < min_qty: tp1_size = round_lot_to_step(min_qty, mkt_step_size)
        if tp2_size < min_qty: tp2_size = round_lot_to_step(min_qty, mkt_step_size)

        # ── TP1 Fixed Profit Override (/settp1profit) ─────────────────────────
        # Jika TP1_PROFIT_PCT > 0, override TP1 dengan persentase fixed dari entry.
        # TP2 tetap dari struktur market (tidak diubah), hanya TP1 yang diganti.
        if TP1_PROFIT_PCT > 0.0:
            if direction == "LONG":
                tp1 = entry * (1 + TP1_PROFIT_PCT / 100)
            else:
                tp1 = entry * (1 - TP1_PROFIT_PCT / 100)
            print(f"  🎯 TP1 OVERRIDE ({TP1_PROFIT_PCT:.2f}%): {tp1:.{eff_prec}f} (entry={entry:.{eff_prec}f})")

        def rtick(p): return round_price_to_tick(p, tick_size)
        sl  = rtick(sl)
        tp1 = rtick(tp1)
        tp2 = rtick(tp2)

        # ── Guard: pastikan tp1 ≠ tp2 setelah rounding ke tick ───────────────
        # Bisa terjadi jika gap tp1-tp2 < 1 tick (ATR sangat kecil relatif ke harga)
        _min_tick_gap = max(tick_size * 3, entry * 0.002)  # minimal 3 tick atau 0.2% entry
        if direction == "LONG":
            if tp2 <= tp1:
                tp2 = rtick(tp1 + _min_tick_gap)
        else:
            if tp2 >= tp1:
                tp2 = rtick(tp1 - _min_tick_gap)

        # ── Simpan ke pending_limit_orders — SL/TP dipasang SETELAH order terisi ──
        # Monitor loop (monitor_pending_limit_orders) akan cek status setiap scan.
        # Jika terisi → pasang SL/TP dan pindah ke active_positions.
        # Jika timeout LIMIT_ORDER_TIMEOUT_MINUTES menit → cancel order.
        pending_limit_orders[symbol] = {
            "order_id":      _limit_order_id,
            "direction":     direction,
            "entry":         entry,
            "sl":            sl,
            "tp1":           tp1,
            "tp2":           tp2,
            "lot":           lot,
            "tp1_size":      tp1_size,
            "tp2_size":      tp2_size,
            "side":          side,
            "sl_side":       sl_side,
            "position_side": position_side,
            "tick_size":     tick_size,
            "eff_prec":      eff_prec,
            "step_size":     step_size,
            "mkt_step_size": mkt_step_size,
            "min_qty":       min_qty,
            "mode":          mode,
            "signal":        signal,
            "placed_at":     datetime.now(timezone.utc),
        }

        bot_state["signals_today"] += 1

        dir_em = "🟢" if direction == "LONG" else "🔴"
        # ── Gunakan eff_prec untuk notif — lebih akurat dari tick_size mentah ──
        # tick_size dari PRICE_FILTER bisa terlalu kasar (misal 0.001 untuk IMXUSDT
        # harga 0.16xx) sehingga entry/SL/TP tampak sama semua di Telegram.
        # eff_prec = max(smart_price_precision(market_price), tick_prec) → presisi benar.
        def _fmt_notif(p: float) -> str:
            return f"{p:.{eff_prec}f}"
        send_telegram_raw(
            f"⏳ <b>LIMIT ORDER DIKIRIM — {symbol}</b>\n"
            f"{'─'*34}\n"
            f"{dir_em} Arah       : <b>{direction}</b>\n"
            f"💲 Entry Limit: <b>{_fmt_notif(entry)}</b>\n"
            f"🛑 Stop Loss  : <b>{_fmt_notif(sl)}</b>\n"
            f"🎯 TP1        : <b>{_fmt_notif(tp1)}</b>\n"
            f"🎯 TP2        : <b>{_fmt_notif(tp2)}</b>\n"
            f"📦 Lot        : <b>{lot}</b>\n"
            f"⚡ Leverage   : <b>{trade_leverage}x</b>\n"
            f"{'─'*34}\n"
            f"⏱ Auto-cancel jika tidak terisi dalam <b>{LIMIT_ORDER_TIMEOUT_MINUTES} menit</b>"
        )
        print(f"⏳ [LIMIT ORDER] {symbol} | {direction} | Limit:{entry} | Lot:{lot} | OrderID:{_limit_order_id}")

    except Exception as e:
        err_str = str(e)
        print(f"❌ ERROR execute {symbol}: {e}")
        send_telegram_raw(f"❌ <b>ERROR ENTRY</b> {symbol}\n<code>{e}</code>")
        # Jangan record_pair_fail untuk -1111 (precision) — sudah di-retry di atas.
        # record_pair_fail hanya untuk error fundamental (balance, margin, dll)
        if "-1111" not in err_str and "precision" not in err_str.lower():
            record_pair_fail(symbol)



# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 18b — PENDING LIMIT ORDER MONITOR
# ═══════════════════════════════════════════════════════════════════════════

def _activate_position_from_limit(symbol: str, pend: dict):
    """
    Dipanggil saat limit order terisi (status FILLED).
    Pasang SL + TP lalu pindahkan ke active_positions.
    """
    entry         = pend["entry"]
    sl            = pend["sl"]
    tp1           = pend["tp1"]
    tp2           = pend["tp2"]
    lot           = pend["lot"]
    side          = pend["side"]
    sl_side       = pend["sl_side"]
    direction     = pend["direction"]
    position_side = pend["position_side"]
    tick_size     = pend["tick_size"]
    step_size     = pend["step_size"]
    mkt_step_size = pend["mkt_step_size"]
    min_qty       = pend["min_qty"]
    eff_prec      = pend["eff_prec"]
    mode          = pend["mode"]
    signal        = pend["signal"]

    # ── Recalculate tp1_size/tp2_size menggunakan TP1_PARTIAL terkini ─────────
    # Selalu pakai nilai TP1_PARTIAL saat order FILLED (bukan saat order dibuat),
    # agar perubahan via /settp1partial langsung efektif di trade berikutnya.
    tp1_size = round_lot_to_step(lot * TP1_PARTIAL, mkt_step_size)
    tp2_size = round_lot_to_step(lot - tp1_size,    mkt_step_size)
    if tp1_size < min_qty: tp1_size = round_lot_to_step(min_qty, mkt_step_size)
    if tp2_size < min_qty: tp2_size = round_lot_to_step(min_qty, mkt_step_size)
    print(f"  ⚙️  [{symbol}] tp1_size={tp1_size} ({int(TP1_PARTIAL*100)}%) tp2_size={tp2_size} ({100-int(TP1_PARTIAL*100)}%)")

    # Adjust SL/TP dari harga fill fresh
    fresh_price = get_current_price(symbol) or entry
    def rtick(p): return round_price_to_tick(p, tick_size)
    min_sl_buf = max(1, round(fresh_price * 0.005 / tick_size)) * tick_size if tick_size > 0 else fresh_price * 0.005
    min_tp_buf = max(1, round(fresh_price * 0.002 / tick_size)) * tick_size if tick_size > 0 else fresh_price * 0.002
    if direction == "LONG":
        sl  = rtick(min(sl,  fresh_price - min_sl_buf))
        tp1 = rtick(max(tp1, fresh_price + min_tp_buf))
        tp2 = rtick(max(tp2, tp1 + min_tp_buf))
    else:
        sl  = rtick(max(sl,  fresh_price + min_sl_buf))
        tp1 = rtick(min(tp1, fresh_price - min_tp_buf))
        tp2 = rtick(min(tp2, tp1 - min_tp_buf))

    print(f"  📐 SL/TP FINAL [{symbol}] sl={sl:.{eff_prec}f} tp1={tp1:.{eff_prec}f} tp2={tp2:.{eff_prec}f}")

    def _emergency_close():
        """
        Tutup posisi darurat via MARKET order.
        Multi-attempt dengan fallback presisi makin ketat:
          1. Pakai mkt_stepSize + quantityPrecision dari filter (re-fetch fresh)
          2. Jika gagal precision (-1111): floor ke integer (quantityPrecision=0)
          3. Jika masih gagal: coba closePosition=true (tanpa quantity)
        """
        em_filters  = get_lot_filters(symbol)
        em_step     = em_filters.get("mkt_stepSize", em_filters.get("stepSize", step_size))
        em_qty_prec = em_filters.get("quantityPrecision", None)

        # Derive qty_prec dari em_step jika tidak tersedia (testnet sering tidak return)
        if em_qty_prec is None and em_step > 0:
            em_qty_prec = max(0, round(-math.log10(em_step)))

        def _build_params(qty_str: str, use_close_position: bool = False) -> dict:
            p = {"symbol": symbol, "side": sl_side, "type": "MARKET"}
            if use_close_position:
                p["closePosition"] = "true"
                if position_side != "BOTH":
                    p["positionSide"] = position_side
            else:
                p["quantity"] = qty_str
                if position_side != "BOTH":
                    p["positionSide"] = position_side
                else:
                    p["reduceOnly"] = "true"
            return p

        # ── Attempt 1: floor ke quantityPrecision yang ter-derive ────────────
        em_lot = lot
        if em_qty_prec is not None:
            em_lot = round(math.floor(em_lot * (10 ** em_qty_prec)) / (10 ** em_qty_prec), em_qty_prec)
        em_qty = format_qty(em_lot, em_step, em_qty_prec)
        try:
            api_post("/fapi/v1/order", _build_params(em_qty))
            print(f"  🚨 [{symbol}] EMERGENCY CLOSE berhasil (attempt 1) qty={em_qty}")
            send_telegram_raw(f"🚨 <b>EMERGENCY CLOSE — {symbol}</b>\nPosisi ditutup otomatis karena SL/TP gagal dipasang.")
            return
        except Exception as ec1:
            print(f"  ⚠️  [{symbol}] EMERGENCY attempt 1 gagal (qty={em_qty}): {ec1}")

        # ── Attempt 2: floor ke integer (quantityPrecision=0) ────────────────
        em_lot_int = math.floor(lot)
        em_qty_int = str(int(em_lot_int))
        if em_lot_int > 0:
            try:
                api_post("/fapi/v1/order", _build_params(em_qty_int))
                print(f"  🚨 [{symbol}] EMERGENCY CLOSE berhasil (attempt 2, integer qty={em_qty_int})")
                send_telegram_raw(f"🚨 <b>EMERGENCY CLOSE — {symbol}</b>\nPosisi ditutup (qty integer={em_qty_int}) karena SL/TP gagal dipasang.")
                return
            except Exception as ec2:
                print(f"  ⚠️  [{symbol}] EMERGENCY attempt 2 gagal (qty={em_qty_int}): {ec2}")

        # ── Attempt 3: closePosition=true (tanpa quantity, Binance tutup semua) ──
        try:
            api_post("/fapi/v1/order", _build_params("", use_close_position=True))
            print(f"  🚨 [{symbol}] EMERGENCY CLOSE berhasil (attempt 3, closePosition=true)")
            send_telegram_raw(f"🚨 <b>EMERGENCY CLOSE — {symbol}</b>\nPosisi ditutup via closePosition karena SL/TP gagal dipasang.")
            return
        except Exception as ec3:
            print(f"  ❌ [{symbol}] EMERGENCY CLOSE GAGAL semua attempt: {ec3}")
            send_telegram_raw(f"🆘 <b>EMERGENCY CLOSE GAGAL — {symbol}</b>\nCek dan tutup manual di Binance!\nError: <code>{ec3}</code>")

    # ── Pasang SL ──
    sl_ok = False
    for _a in range(5):
        try:
            algo_post_sl(symbol, sl_side, sl, quantity=lot, position_side=position_side)
            sl_ok = True
            break
        except Exception as _e:
            print(f"  ⚠️  [{symbol}] SL attempt {_a+1}/5: {_e}")
            if _a < 4: time.sleep(0.5 * (_a + 1))
    if not sl_ok:
        send_telegram_raw(f"❌ <b>SL GAGAL — {symbol}</b>\nEmergency close dimulai...")
        _emergency_close()
        set_cooldown_post_close(symbol, reason="SL gagal setelah limit fill")
        return

    # ── Pasang TP1 ──
    tp1_ok = False
    for _a in range(5):
        try:
            algo_post_tp(symbol, sl_side, tp1, tp1_size, position_side=position_side)
            tp1_ok = True
            break
        except Exception as _e:
            print(f"  ⚠️  [{symbol}] TP1 attempt {_a+1}/5: {_e}")
            if _a < 4: time.sleep(0.5 * (_a + 1))
    if not tp1_ok:
        try:
            open_ords = get_open_orders(symbol)
            for o in open_ords:
                try: api_delete("/fapi/v1/order", {"symbol": symbol, "orderId": o["orderId"]})
                except Exception: pass
        except Exception: pass
        send_telegram_raw(f"❌ <b>TP1 GAGAL — {symbol}</b>\nEmergency close dimulai...")
        _emergency_close()
        set_cooldown_post_close(symbol, reason="TP1 gagal setelah limit fill")
        return

    # ── Pasang TP2 ──
    for _a in range(5):
        try:
            algo_post_tp(symbol, sl_side, tp2, tp2_size, position_side=position_side)
            break
        except Exception as _e:
            print(f"  ⚠️  [{symbol}] TP2 attempt {_a+1}/5: {_e}")
            if _a < 4: time.sleep(0.5 * (_a + 1))

    # ── Daftarkan ke active_positions ──
    active_positions[symbol] = {
        "entry":           entry,
        "sl":              sl,
        "original_sl":     sl,      # ← simpan SL awal untuk kalkulasi RR trailing
        "tp1":             tp1,
        "tp2":             tp2,
        "lot":             lot,
        "tp1_size":        tp1_size,
        "tp2_size":        tp2_size,
        "side":            side,
        "sl_side":         sl_side,
        "direction":       direction,
        "position_side":   position_side,
        "tick_size":       tick_size,
        "eff_prec":        eff_prec,
        "step_size":       step_size,
        "mkt_step_size":   mkt_step_size,
        "min_qty":         min_qty,
        "tp1_hit":         False,
        "trailing_active": False,
        "open_time":       datetime.now(timezone.utc),
        "pre_existing":    False,
    }

    set_cooldown(symbol, mode["label"], mode["entry_tf"])

    signal_display = dict(signal)
    signal_display["entry"]       = entry
    signal_display["stop_loss"]   = sl
    signal_display["take_profit"] = [tp1, tp2]
    send_telegram_signal(
        signal       = signal_display,
        mode         = mode,
        score_bd     = signal["_score_bd"],
        session      = signal["_session"],
        pa_name      = signal.get("_pa_name", "-"),
        btc_bias     = signal["_btc_bias"],
        btcd_trend   = signal["_btcd_trend"],
        macro_reason = signal["_macro_reason"],
    )
    # Gunakan eff_prec untuk format harga di notif fill — lebih akurat dari tick_size
    _eff_p = pend.get("eff_prec", max(0, round(-math.log10(tick_size))) if tick_size > 0 else 4)
    def _fmt_fill(p: float) -> str:
        return f"{p:.{_eff_p}f}"
    send_telegram_raw(
        f"✅ <b>LIMIT ORDER TERISI — {symbol}</b>\n"
        f"{'─'*34}\n"
        f"{'🟢' if direction == 'LONG' else '🔴'} <b>{direction}</b> | Entry: <b>{_fmt_fill(entry)}</b>\n"
        f"🛑 SL: <b>{_fmt_fill(sl)}</b> | 🎯 TP1: <b>{_fmt_fill(tp1)}</b>\n"
        f"✅ SL + TP sudah terpasang"
    )
    print(f"✅ [LIMIT FILLED] {symbol} | {direction} | Entry:{entry} | Lot:{lot}")


def monitor_pending_limit_orders():
    """
    Cek status semua pending limit order setiap scan loop.
    - FILLED  → aktifkan posisi (pasang SL/TP, masuk active_positions)
    - CANCELED/EXPIRED → bersihkan dict
    - Timeout (> LIMIT_ORDER_TIMEOUT_MINUTES) → cancel order di Binance
    """
    if not pending_limit_orders:
        return

    from datetime import timedelta
    now = datetime.now(timezone.utc)

    for symbol in list(pending_limit_orders.keys()):
        pend = pending_limit_orders.get(symbol)
        if pend is None:
            continue

        order_id  = pend.get("order_id")
        placed_at = pend.get("placed_at")
        direction = pend.get("direction", "")

        # ── Cek timeout ──────────────────────────────────────────────────────
        elapsed_min = (now - placed_at).total_seconds() / 60 if placed_at else 999
        if elapsed_min >= LIMIT_ORDER_TIMEOUT_MINUTES:
            print(f"  ⏰ [{symbol}] Limit order timeout ({elapsed_min:.0f} menit) — cancel order {order_id}")
            # Cancel order di Binance
            if order_id:
                try:
                    api_delete("/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
                    print(f"  🗑  [{symbol}] Limit order {order_id} berhasil di-cancel")
                except Exception as _ce:
                    print(f"  ⚠️  [{symbol}] Gagal cancel limit order: {_ce}")
            del pending_limit_orders[symbol]
            dir_str = "LONG" if direction == "LONG" else "SHORT"
            send_telegram_raw(
                f"⏰ <b>LIMIT ORDER EXPIRED — {symbol}</b>\n"
                f"{'─'*34}\n"
                f"Order {dir_str} tidak terisi dalam <b>{LIMIT_ORDER_TIMEOUT_MINUTES} menit</b>.\n"
                f"Order di-cancel otomatis. Bot akan cari sinyal baru."
            )
            continue

        # ── Cek status order di Binance ──────────────────────────────────────
        if not order_id:
            del pending_limit_orders[symbol]
            continue
        try:
            ord_data = api_get("/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, signed=True)
            status   = ord_data.get("status", "")
        except Exception as _qe:
            print(f"  ⚠️  [{symbol}] Gagal query limit order status: {_qe}")
            continue

        if status == "FILLED":
            print(f"  ✅ [{symbol}] Limit order FILLED — aktifkan posisi")
            del pending_limit_orders[symbol]
            try:
                _activate_position_from_limit(symbol, pend)
            except Exception as _ae:
                print(f"  ❌ [{symbol}] Error aktivasi posisi dari limit fill: {_ae}")
                send_telegram_raw(f"❌ <b>ERROR AKTIVASI LIMIT FILL — {symbol}</b>\n<code>{_ae}</code>")

        elif status in ("CANCELED", "EXPIRED", "REJECTED"):
            print(f"  🗑  [{symbol}] Limit order {status} — hapus dari pending")
            del pending_limit_orders[symbol]
            send_telegram_raw(
                f"🗑 <b>LIMIT ORDER {status} — {symbol}</b>\n"
                f"Order tidak aktif lagi. Bot akan cari sinyal baru."
            )
        else:
            # NEW / PARTIALLY_FILLED — masih menunggu
            remaining_min = LIMIT_ORDER_TIMEOUT_MINUTES - elapsed_min
            print(f"  ⏳ [{symbol}] Limit order {status} | elapsed={elapsed_min:.0f}m | sisa={remaining_min:.0f}m")




def get_open_orders(symbol: str) -> list:
    try:
        return api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
    except Exception:
        return []


def notify_tp1_hit(symbol: str, pos: dict, price: float):
    dir_em   = "🟢" if pos["direction"] == "LONG" else "🔴"
    tp1_pct  = int(TP1_PARTIAL * 100)
    tp2_pct  = 100 - tp1_pct
    msg = (
        f"🎯 <b>TP1 HIT — {symbol}</b>\n"
        f"{'─'*34}\n"
        f"{dir_em} Arah     : <b>{pos['direction']}</b>\n"
        f"💰 Entry   : <b>{pos['entry']}</b>\n"
        f"🎯 TP1     : <b>{pos['tp1']}</b>\n"
        f"📈 Harga   : <b>{price}</b>\n"
        f"✅ Sebagian posisi CLOSED ({tp1_pct}%)\n"
        f"🔄 SL akan dipindah ke Break Even\n"
        f"{'─'*34}\n"
        f"⚡ Sisa posisi ({tp2_pct}%) masih berjalan → TP2: <b>{pos['tp2']}</b>"
    )
    send_telegram_raw(msg)


def notify_sl_hit(symbol: str, pos: dict, price: float, sl_type: str = "Stop Loss"):
    entry    = pos["entry"]
    tp1_hit  = pos.get("tp1_hit", False)
    is_be    = sl_type == "Trailing Stop (Break Even)"
    pnl_pct  = ((price - entry) / entry * 100)
    if pos["direction"] == "SHORT":
        pnl_pct = -pnl_pct
    # BE = TP1 sudah profit → overall trade WIN meski sisa lot close di 0%
    is_win    = is_be or pnl_pct > 0
    dir_em    = "🟢" if pos["direction"] == "LONG" else "🔴"
    em        = "🔄" if is_be else "🛑"
    color     = "⚪" if is_be else "🔴"
    wins      = bot_state["wins"]
    losses    = bot_state["losses"]
    total     = wins + losses
    winrate   = (wins / total * 100) if total > 0 else 0
    result_em = "✅ WIN (BE)" if is_be else ("✅ WIN" if is_win else "❌ LOSS")
    is_real_sl = sl_type == "Stop Loss"
    cooldown_note = (
        f"⏳ <b>Cooldown {COOLDOWN_AFTER_SL_HOURS} jam aktif</b> — "
        f"bot tidak akan re-entry {symbol} selama {COOLDOWN_AFTER_SL_HOURS} jam."
    ) if is_real_sl else (
        f"✅ Trailing Stop (Break Even) — tidak ada cooldown paksa."
    )
    # Keterangan lot yang di-close sekarang
    tp1_pct_lot = int(pos.get("tp1_size", 0) / max(pos.get("lot", 1), 1e-9) * 100) if tp1_hit else 0
    remaining_pct = 100 - tp1_pct_lot if tp1_hit else 100
    lot_note = (
        f"📦 Lot close: sisa {remaining_pct}% (TP1 sudah close {tp1_pct_lot}% sebelumnya)"
    ) if tp1_hit else (
        f"📦 Lot close: 100% (full posisi)"
    )
    close_desc = (
        f"✅ Sisa posisi ditutup di Break Even — TP1 sudah ambil profit!"
        if is_be else
        f"❌ Posisi ditutup dengan loss"
    )
    msg = (
        f"{em} <b>{sl_type} HIT — {symbol}</b>\n"
        f"{'─'*34}\n"
        f"{dir_em} Arah    : <b>{pos['direction']}</b>\n"
        f"💰 Entry  : <b>{entry}</b>\n"
        f"🛑 SL     : <b>{pos['sl']}</b>\n"
        f"📉 Harga  : <b>{price}</b>\n"
        f"{color} PnL sisa: <b>{'+'if pnl_pct>=0 else ''}{pnl_pct:.2f}%</b>\n"
        f"{lot_note}\n"
        f"{'─'*34}\n"
        f"{close_desc}\n"
        f"{'─'*34}\n"
        f"📊 <b>Hasil: {result_em}</b>\n"
        f"  ✅ Win : <b>{wins}</b>  |  ❌ Loss: <b>{losses}</b>  |  🎯 WR: <b>{winrate:.1f}%</b>\n"
        f"{'─'*34}\n"
        f"{cooldown_note}"
    )
    send_telegram_raw(msg)


def notify_tp2_hit(symbol: str, pos: dict, price: float):
    dir_em  = "🟢" if pos["direction"] == "LONG" else "🔴"
    pnl_pct = ((price - pos["entry"]) / pos["entry"] * 100)
    if pos["direction"] == "SHORT":
        pnl_pct = -pnl_pct
    wins    = bot_state["wins"]
    losses  = bot_state["losses"]
    total   = wins + losses
    winrate = (wins / total * 100) if total > 0 else 0
    msg = (
        f"🏆 <b>TP2 HIT — {symbol}</b>\n"
        f"{'─'*34}\n"
        f"{dir_em} Arah    : <b>{pos['direction']}</b>\n"
        f"💰 Entry  : <b>{pos['entry']}</b>\n"
        f"🎯 TP2    : <b>{pos['tp2']}</b>\n"
        f"📈 Harga  : <b>{price}</b>\n"
        f"📊 PnL    : <b>+{pnl_pct:.2f}%</b>\n"
        f"{'─'*34}\n"
        f"✅ <b>Full posisi CLOSED — Trade Selesai!</b>\n"
        f"{'─'*34}\n"
        f"📊 <b>Hasil: ✅ WIN</b>\n"
        f"  ✅ Win : <b>{wins}</b>  |  ❌ Loss: <b>{losses}</b>  |  🎯 WR: <b>{winrate:.1f}%</b>"
    )
    send_telegram_raw(msg)


def notify_trailing_activated(symbol: str, pos: dict, new_sl: float):
    msg = (
        f"🔄 <b>TRAILING STOP AKTIF — {symbol}</b>\n"
        f"{'─'*34}\n"
        f"💰 Entry       : <b>{pos['entry']}</b>\n"
        f"🛑 SL Lama     : <b>{pos['sl']}</b>\n"
        f"✅ SL Baru     : <b>{new_sl}</b> (Break Even)\n"
        f"{'─'*34}\n"
        f"🔒 Posisi terlindungi dari loss"
    )
    send_telegram_raw(msg)


def manage_trailing():
    for symbol, pos in list(active_positions.items()):
        try:
            price = get_current_price(symbol)
            if price is None:
                continue

            entry     = pos["entry"]
            sl        = pos["sl"]
            tp1       = pos["tp1"]
            tp2       = pos["tp2"]
            direction = pos["direction"]

            # ── Re-fetch tick_size dari filters terbaru ──────────────────────
            # Jangan pakai pos["tick_size"] yang tersimpan saat entry — bisa stale.
            # tick_size yang salah → format_price salah → -1111 di algoOrder.
            _live_filters = get_lot_filters(symbol)
            tick_size = _live_filters.get("tickSize", pos.get("tick_size", 0.01))
            step_size = _live_filters.get("stepSize", pos.get("step_size", 0.001))

            sl_side   = "SELL" if pos["side"] == "BUY" else "BUY"

            still_open = has_position(symbol)
            if not still_open:
                # Jika sudah dihapus lebih dulu oleh sync_closed_positions, skip
                if symbol not in active_positions:
                    continue
                # Tentukan apakah SL atau TP yang kena
                # PENTING: price dari monitoring loop bisa stale (sudah bergerak setelah fill).
                # Cek urutan: TP2 dulu (profit tertinggi), lalu TP1 trail, lalu SL, lalu fallback.
                _is_sl = False
                if direction == "LONG":
                    if price >= tp2:
                        # Harga tembus TP2 → TP2 hit
                        notify_tp2_hit(symbol, pos, price)
                    elif pos.get("tp1_hit") and price >= entry:
                        # TP1 sudah kena sebelumnya + harga masih di atas entry
                        # → sisa lot kena trailing SL (break even), bukan SL murni
                        notify_sl_hit(symbol, pos, price, "Trailing Stop (Break Even)")
                    elif price <= sl or price < entry:
                        # Harga turun ke/bawah SL atau bawah entry → SL kena
                        _is_sl = True
                        sl_type = "Trailing Stop (Break Even)" if pos.get("trailing_active") else "Stop Loss"
                        notify_sl_hit(symbol, pos, price, sl_type)
                    else:
                        # Fallback: posisi close tanpa kondisi jelas → anggap SL
                        _is_sl = True
                        notify_sl_hit(symbol, pos, price, "Stop Loss")
                else:  # SHORT
                    if price <= tp2:
                        # Harga tembus TP2 (turun) → TP2 hit
                        notify_tp2_hit(symbol, pos, price)
                    elif pos.get("tp1_hit") and price <= entry:
                        # TP1 sudah kena sebelumnya + harga masih di bawah entry
                        # → sisa lot kena trailing SL (break even)
                        notify_sl_hit(symbol, pos, price, "Trailing Stop (Break Even)")
                    elif price >= sl or price > entry:
                        # Harga naik ke/atas SL atau atas entry → SL kena
                        _is_sl = True
                        sl_type = "Trailing Stop (Break Even)" if pos.get("trailing_active") else "Stop Loss"
                        notify_sl_hit(symbol, pos, price, sl_type)
                    else:
                        # Fallback: posisi close tanpa kondisi jelas → anggap SL
                        _is_sl = True
                        notify_sl_hit(symbol, pos, price, "Stop Loss")

                # Kalau TP1 sudah kena + posisi close di entry (BE) → dihitung WIN,
                # karena TP1 sudah ambil profit. Sisa lot memang 0% tapi overall trade untung.
                _is_be  = pos.get("tp1_hit", False) and not _is_sl
                _is_win = _is_be or \
                          (direction == "LONG" and price > entry) or \
                          (direction == "SHORT" and price < entry)
                # Kalau TP1 sudah kena sebelumnya, lot yang tersisa hanya tp2_size.
                # Pakai full lot hanya kalau TP1 belum pernah kena.
                _close_lot = pos.get("tp2_size") if pos.get("tp1_hit") else pos.get("lot", 0)
                if not _close_lot:
                    _close_lot = pos.get("lot", 0)
                if direction == "LONG":
                    _real_pnl = (price - entry) * _close_lot
                else:
                    _real_pnl = (entry - price) * _close_lot
                # BE close: anggap PnL sedikit positif agar update_performance hitung WIN bukan LOSS
                if _is_be and _real_pnl <= 0:
                    _real_pnl = 0.001
                update_performance(_real_pnl)
                del active_positions[symbol]
                # ── Set cooldown: 4 jam jika SL, tidak ada cooldown jika TP ──
                set_cooldown_post_close(
                    symbol,
                    reason="posisi close terdeteksi trailing monitor",
                    is_stoploss=_is_sl
                )
                if _is_sl:
                    record_pair_sl(symbol)   # ← catat SL untuk blacklist tracker
                continue

            if not pos.get("tp1_hit"):
                tp1_hit = (direction == "LONG" and price >= tp1) or \
                          (direction == "SHORT" and price <= tp1)

                # ── FIX BUG 3: cek juga via Binance open orders ──────────────
                # Kalau harga bounce cepat sebelum scan, price polling tidak cukup.
                # Kita cek apakah TP1 order sudah tidak ada lagi di Binance
                # (berarti sudah tereksekusi) meski harga saat ini sudah turun.
                #
                # GUARD: skip order-check dalam 60 detik pertama setelah entry.
                # Saat baru entry, TP order belum tentu muncul di Binance (API delay).
                # Tanpa guard ini → _no_tp_left=True + harga naik 0.2% → false TP1 hit.
                _open_time   = pos.get("open_time")
                _age_seconds = (datetime.now(timezone.utc) - _open_time).total_seconds() \
                               if _open_time else 999
                if not tp1_hit and _age_seconds >= 60:
                    try:
                        _open_ords = api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
                        _tp_ords   = [o for o in _open_ords if o.get("type") in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT")]
                        # Kalau tidak ada TP order DAN posisi masih ada DAN trailing belum aktif
                        # → kemungkinan TP1 sudah kena (order tereksekusi, hanya TP2 tersisa)
                        # Threshold dinaikkan ke 80% dari jarak entry→TP1 agar tidak false-trigger
                        # saat harga baru bergerak sedikit setelah entry.
                        _no_tp_left = len(_tp_ords) == 0
                        _tp1_dist   = abs(tp1 - entry)
                        _min_move   = _tp1_dist * 0.8  # harga harus sudah 80% menuju TP1
                        _was_in_profit = (direction == "LONG" and price >= entry + _min_move) or \
                                         (direction == "SHORT" and price <= entry - _min_move)
                        if _no_tp_left and _was_in_profit:
                            tp1_hit = True
                            print(f"  🔍 [{symbol}] tp1_hit via order-check (TP order hilang dari Binance)")
                    except Exception:
                        pass  # gagal cek → lanjut dengan price-based saja

                if tp1_hit:
                    active_positions[symbol]["tp1_hit"] = True
                    # Simpan original_sl sebelum dipindah ke BE — dipakai untuk RR trailing
                    if "original_sl" not in active_positions[symbol]:
                        active_positions[symbol]["original_sl"] = sl
                    notify_tp1_hit(symbol, pos, price)
                    new_sl = adjust_sl_price(entry, price, direction, tick_size, buffer_pct=0.005)
                    try:
                        cancel_open_orders(symbol)
                        time.sleep(0.3)
                        _remaining_lot = pos.get("tp2_size") or pos.get("lot", 0)
                        _pos_side = pos.get("position_side", "BOTH")
                        sl_placed = False
                        for _buf in (0.000, 0.005, 0.010, 0.015):
                            try:
                                fresh = get_current_price(symbol) or price
                                new_sl = adjust_sl_price(entry, fresh, direction, tick_size, buffer_pct=max(_buf, 0.005))
                                algo_post_sl(symbol, sl_side, new_sl, quantity=_remaining_lot, position_side=_pos_side)
                                sl_placed = True
                                break
                            except Exception as _e:
                                if _buf == 0.015:
                                    raise _e
                                time.sleep(0.3)
                        # ── FIX BUG 2: pasang TP2 setelah SL — jika SL gagal jangan pasang TP2 ──
                        _tp2_size = pos.get("tp2_size", 0)
                        _min_qty  = pos.get("min_qty", 0.001)
                        if sl_placed and _tp2_size >= _min_qty:
                            try:
                                _tp2_price = adjust_tp_price(tp2, price, direction, tick_size, buffer_pct=0.002)
                                algo_post_tp(symbol, sl_side, _tp2_price, _tp2_size, position_side=_pos_side)
                            except Exception as _tp2e:
                                print(f"  ⚠️  [{symbol}] TP2 gagal dipasang ulang setelah BE: {_tp2e}")
                                send_telegram_raw(f"⚠️ <b>TP2 gagal dipasang — {symbol}</b>\nCek manual di Binance!\nError: <code>{_tp2e}</code>")
                        active_positions[symbol]["sl"]              = new_sl
                        active_positions[symbol]["trailing_active"] = True
                        notify_trailing_activated(symbol, active_positions[symbol], new_sl)
                    except Exception as e:
                        print(f"⚠️ Gagal pindah SL ke BE {symbol}: {e}")

            elif pos.get("trailing_active"):
                # ── FIX BUG 1: gunakan original_sl untuk hitung RR, bukan sl terkini ──
                # Setelah BE, sl = entry → denominator hampir 0 → rr = INF → trailing geser tiap scan
                # Fix: simpan original_sl saat entry dan pakai itu sebagai basis RR
                original_sl = pos.get("original_sl", sl)
                original_sl_dist = abs(entry - original_sl)
                if original_sl_dist < 1e-9:
                    original_sl_dist = abs(entry - sl) + 1e-9  # fallback ke sl terkini jika original tidak ada
                rr = abs(price - entry) / original_sl_dist
                if rr >= TRAILING_TRIGGER:
                    if direction == "LONG":
                        candidate_raw = price * (1 - 0.005)
                    else:
                        candidate_raw = price * (1 + 0.005)
                    candidate_sl = adjust_sl_price(candidate_raw, price, direction, tick_size, buffer_pct=0.005)
                    should_update = (direction == "LONG"  and candidate_sl > sl) or \
                                   (direction == "SHORT" and candidate_sl < sl)
                    if should_update:
                        try:
                            cancel_open_orders(symbol)
                            time.sleep(0.3)
                            _remaining_lot = pos.get("tp2_size") or pos.get("lot", 0)
                            _pos_side2 = pos.get("position_side", "BOTH")
                            sl_ok = False
                            for _tbuf in (0.000, 0.005, 0.010):
                                try:
                                    _fresh2 = get_current_price(symbol) or price
                                    _csl    = adjust_sl_price(candidate_raw, _fresh2, direction, tick_size, buffer_pct=max(_tbuf, 0.005))
                                    algo_post_sl(symbol, sl_side, _csl, quantity=_remaining_lot, position_side=_pos_side2)
                                    candidate_sl = _csl
                                    sl_ok = True
                                    break
                                except Exception:
                                    if _tbuf == 0.010:
                                        raise
                                    time.sleep(0.3)
                            # ── FIX BUG 2: pasang TP2 kembali setelah cancel, hanya jika SL berhasil ──
                            _tp2_size = pos.get("tp2_size", 0)
                            _min_qty  = pos.get("min_qty", 0.001)
                            if sl_ok and _tp2_size >= _min_qty:
                                try:
                                    _tp2_price = adjust_tp_price(tp2, price, direction, tick_size, buffer_pct=0.002)
                                    algo_post_tp(symbol, sl_side, _tp2_price, _tp2_size, position_side=_pos_side2)
                                except Exception as _tp2ge:
                                    print(f"  ⚠️  [{symbol}] TP2 gagal dipasang ulang saat trailing geser: {_tp2ge}")
                                    send_telegram_raw(f"⚠️ <b>TP2 gagal dipasang — {symbol}</b>\nSetelah trailing geser. Cek manual!\nError: <code>{_tp2ge}</code>")
                            if sl_ok:
                                active_positions[symbol]["sl"] = candidate_sl
                                send_telegram_raw(
                                    f"🔄 <b>Trailing geser — {symbol}</b>\n"
                                    f"SL: {sl} → <b>{candidate_sl}</b> | Mark: {price}\n"
                                    f"RR: {rr:.2f}x dari SL awal"
                                )
                        except Exception as e:
                            print(f"⚠️ Gagal geser trailing {symbol}: {e}")

        except Exception as e:
            print(f"⚠️ manage_trailing error {symbol}: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 19b — HOURLY POSITION REPORT (IMPROVED)
# ═══════════════════════════════════════════════════════════════════════════

_last_hourly_report: datetime | None = None


def refresh_pnl_snapshots(current_balance: float):
    """
    Cek dan reset snapshot balance harian & bulanan jika perlu.
    Dipanggil setiap kali kita punya balance terbaru.
    """
    now        = datetime.now(timezone.utc)
    today_str  = now.strftime("%Y-%m-%d")
    month_str  = now.strftime("%Y-%m")

    if bot_state["balance_day_date"] != today_str:
        bot_state["balance_day_start"] = current_balance
        bot_state["balance_day_date"]  = today_str
        bot_state["day_wins"]          = 0
        bot_state["day_losses"]        = 0

    if bot_state["balance_month_key"] != month_str:
        bot_state["balance_month_start"] = current_balance
        bot_state["balance_month_key"]   = month_str
        bot_state["month_wins"]          = 0
        bot_state["month_losses"]        = 0


def get_pnl_summary_lines(current_balance: float) -> str:
    """
    Buat baris ringkasan PNL harian & bulanan.
    Format singkat untuk /pnl dan hourly report.
    """
    lines = []

    # ── Harian ──────────────────────────────────────────────────────────────
    day_start = bot_state.get("balance_day_start", 0)
    if day_start > 0:
        day_pnl     = current_balance - day_start
        day_pnl_pct = (day_pnl / day_start) * 100
        day_em      = "🟢" if day_pnl >= 0 else "🔴"
        lines.append(
            f"{day_em} Hari ini  : <b>{'+'if day_pnl>=0 else ''}{day_pnl:.2f} USDT "
            f"({'+'if day_pnl_pct>=0 else ''}{day_pnl_pct:.2f}%)</b>"
        )

    # ── Bulanan ──────────────────────────────────────────────────────────────
    month_start = bot_state.get("balance_month_start", 0)
    if month_start > 0:
        month_pnl     = current_balance - month_start
        month_pnl_pct = (month_pnl / month_start) * 100
        month_em      = "🟢" if month_pnl >= 0 else "🔴"
        month_key     = bot_state.get("balance_month_key", "")
        lines.append(
            f"{month_em} Bulan ini : <b>{'+'if month_pnl>=0 else ''}{month_pnl:.2f} USDT "
            f"({'+'if month_pnl_pct>=0 else ''}{month_pnl_pct:.2f}%)</b>"
        )

    return "\n".join(lines) if lines else ""



def get_mode_pnl_block() -> str:
    """
    Buat blok ringkasan PnL kumulatif terpisah per mode (LIVE / DEMO).
    Dipanggil di /pnl dan /status.
    """
    lines = []
    for mode, em in [("LIVE", "🔴"), ("DEMO", "🟢")]:
        data      = bot_state["mode_pnl"].get(mode, {})
        realized  = data.get("realized", 0.0)
        start_bal = data.get("start_bal", 0.0)
        wins      = data.get("wins", 0)
        losses    = data.get("losses", 0)
        total     = wins + losses
        wr        = f"{wins/total*100:.0f}%" if total > 0 else "–"
        pct       = f"({'+'if realized>=0 else ''}{realized/start_bal*100:.2f}%)" if start_bal > 0 else ""
        sign      = "+" if realized >= 0 else ""
        active    = " ◀ aktif" if mode == BOT_MODE else ""
        lines.append(
            f"{em} <b>{mode}</b>{active}\n"
            f"   PnL Kumulatif : <b>{sign}{realized:.2f} USDT {pct}</b>\n"
            f"   W/L           : {wins}W / {losses}L (WR {wr})"
        )
    return "\n".join(lines)


def cmd_resetpnl():
    """
    /resetpnl
    Reset PnL kumulatif mode yang sedang aktif (LIVE atau DEMO).
    Balance snapshot juga di-reset ke saldo saat ini.
    """
    try:
        bal = get_total_balance()
    except Exception:
        bal = 0.0
    bot_state["mode_pnl"][BOT_MODE] = {
        "realized":  0.0,
        "start_bal": bal,
        "wins":      0,
        "losses":    0,
    }
    em = "🔴" if BOT_MODE == "LIVE" else "🟢"
    send_telegram_raw(
        f"{em} <b>PnL {BOT_MODE} Di-reset</b>\n"
        f"{'─'*34}\n"
        f"💰 Balance saat ini : <b>{bal:.2f} USDT</b>\n"
        f"PnL kumulatif {BOT_MODE} mulai dihitung dari nol."
    )
    print(f"⚙️ PnL {BOT_MODE} di-reset. Balance snapshot: {bal:.2f} USDT")


def get_unrealized_pnl(symbol: str, pos: dict) -> float | None:
    try:
        price = get_current_price(symbol)
        if price is None:
            return None
        entry = pos["entry"]
        lot   = pos["lot"]
        if pos["direction"] == "LONG":
            return (price - entry) * lot
        else:
            return (entry - price) * lot
    except Exception:
        return None


def _build_position_line(symbol: str, pos: dict, now: datetime) -> tuple[str, float]:
    """
    Buat satu baris laporan posisi. Return (teks, pnl_float).
    pnl_float = None diganti 0 untuk total, tapi flag pnl_failed tetap dikelola caller.
    """
    direction = pos["direction"]
    entry     = pos["entry"]
    sl        = pos["sl"]
    tp1       = pos["tp1"]
    tp2       = pos["tp2"]
    lot       = pos["lot"]
    dir_em    = "🟢" if direction == "LONG" else "🔴"
    tp1_em    = "✅" if pos.get("tp1_hit") else "⏳"
    trail_em  = "🔄" if pos.get("trailing_active") else "  "
    pre_em    = "📥" if pos.get("pre_existing") else "🤖"

    # Durasi
    open_time = pos.get("open_time")
    if open_time:
        dur_secs = int((now - open_time).total_seconds())
        dur_h    = dur_secs // 3600
        dur_m    = (dur_secs % 3600) // 60
        dur_str  = f"{dur_h}j {dur_m}m"
    else:
        dur_str = "pre-existing"

    # Harga saat ini & PnL
    price = get_current_price(symbol)
    pnl   = None
    price_str = "N/A"
    pnl_str   = "❓ N/A"
    pnl_val   = 0.0

    if price is not None:
        price_str = f"{price:,.4f}"
        if pos["direction"] == "LONG":
            pnl = (price - entry) * lot
        else:
            pnl = (entry - price) * lot
        pnl_val = pnl
        pnl_pct = (pnl / (entry * lot + 1e-9)) * 100
        pnl_em  = "🟢" if pnl >= 0 else "🔴"
        pnl_str = f"{pnl_em} {'+'if pnl>=0 else ''}{pnl:.2f} USDT ({'+' if pnl_pct>=0 else ''}{pnl_pct:.1f}%)"

    # Jarak ke SL dan TP2
    dist_sl  = abs(price - sl)  / (price + 1e-9) * 100 if price else 0
    dist_tp2 = abs(tp2 - price) / (price + 1e-9) * 100 if price else 0

    line = (
        f"{dir_em}{pre_em} <b>{symbol}</b> {direction} | Lot: {lot}\n"
        f"  💰 Entry   : {entry}  →  🔖 Harga: {price_str}\n"
        f"  🛑 SL      : {sl}  ({dist_sl:.1f}% away)  {trail_em}\n"
        f"  🎯 TP1     : {tp1} {tp1_em}\n"
        f"  🎯 TP2     : {tp2}  ({dist_tp2:.1f}% away)\n"
        f"  📊 PnL     : {pnl_str}\n"
        f"  ⏱ Durasi  : {dur_str}"
    )
    return line, pnl_val if pnl is not None else None


def send_hourly_position_report():
    global _last_hourly_report

    now = datetime.now(timezone.utc)

    if _last_hourly_report is not None:
        elapsed = (now - _last_hourly_report).total_seconds()
        if elapsed < 3600:
            return

    _last_hourly_report = now
    now_str = now.strftime("%d %b %Y %H:%M UTC")

    if not active_positions:
        try:
            current_bal = get_total_balance()
            refresh_pnl_snapshots(current_bal)
            pnl_summary = get_pnl_summary_lines(current_bal)
            bal_line    = f"💼 Balance : <b>{current_bal:.2f} USDT</b>\n"
        except Exception:
            pnl_summary = ""
            bal_line    = ""

        mode_pnl_block = get_mode_pnl_block()
        cumulative_block = (
            f"{'─'*38}\n"
            f"📈 <b>Cumulative PNL</b>\n"
            f"{pnl_summary}\n"
            f"{'─'*38}\n"
            f"📊 <b>PnL per Mode</b>\n"
            f"{mode_pnl_block}\n"
            f"💡 /resetpnl — reset PnL mode aktif"
        ) if (pnl_summary or True) else ""

        msg = (
            f"📋 <b>HOURLY REPORT — {now_str}</b>\n"
            f"{'─'*38}\n"
            f"📭 Tidak ada posisi aktif.\n"
            f"{bal_line}"
            f"{'─'*38}\n"
            f"{cumulative_block}"
            f"⏰ Next report ~1 jam"
        )
        send_telegram_raw(msg)
        return

    total_pnl  = 0.0
    pnl_failed = []
    lines      = []

    for symbol, pos in active_positions.items():
        line, pnl_val = _build_position_line(symbol, pos, now)
        lines.append(line)
        if pnl_val is not None:
            total_pnl += pnl_val
        else:
            pnl_failed.append(symbol)

    total_em  = "🟢" if total_pnl >= 0 else "🔴"
    total_str = f"{total_em} {'+'if total_pnl>=0 else ''}{total_pnl:.2f} USDT"

    sep = "\n" + ("─"*34) + "\n"
    positions_block = sep.join(lines)

    try:
        current_bal = get_total_balance()
        bal_str     = f"{current_bal:.2f} USDT"
        refresh_pnl_snapshots(current_bal)
        pnl_summary = get_pnl_summary_lines(current_bal)
    except Exception:
        bal_str     = "N/A"
        pnl_summary = ""

    pre_count  = sum(1 for p in active_positions.values() if p.get("pre_existing"))
    bot_count  = len(active_positions) - pre_count
    pre_note   = f"\n  (📥 {pre_count} pre-existing | 🤖 {bot_count} dari bot)" if pre_count > 0 else ""

    paused_note = "\n⏸ <b>Bot dalam kondisi PAUSED</b>" if bot_paused else ""

    mode_pnl_block = get_mode_pnl_block()
    cumulative_block = (
        f"{'─'*38}\n"
        f"📈 <b>Cumulative PNL</b>\n"
        f"{pnl_summary}\n"
        f"{'─'*38}\n"
        f"📊 <b>PnL per Mode</b>\n"
        f"{mode_pnl_block}\n"
        f"💡 /resetpnl — reset PnL mode aktif"
    ) if (pnl_summary or True) else ""

    msg = (
        f"📋 <b>HOURLY REPORT — {now_str}</b>\n"
        f"{'─'*38}\n"
        f"📌 Posisi Aktif : <b>{len(active_positions)}</b> trade{pre_note}\n"
        f"💼 Balance      : <b>{bal_str}</b>\n"
        f"{'─'*38}\n"
        f"{positions_block}\n"
        f"{'─'*38}\n"
        f"💹 Total Unrealized PnL: <b>{total_str}</b>\n"
        f"{'─'*38}\n"
        f"{cumulative_block}"
        f"⏰ Next report ~1 jam"
        f"{paused_note}"
    )
    send_telegram_raw(msg)
    print(f"📋 Hourly report sent — {len(active_positions)} posisi | PnL: {total_str}")


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 20 — DRAWDOWN PROTECTION & STATS
# ═══════════════════════════════════════════════════════════════════════════

def check_drawdown():
    global bot_paused, _daily_limit_state
    current = get_total_balance()
    start   = bot_state["balance_start"]
    if start == 0:
        return
    dd = (start - current) / start
    if dd >= MAX_DRAWDOWN:
        # Jangan exit() — cukup pause bot agar bisa di-resume via /start atau /resumeorpause
        if _daily_limit_state.get("paused_by") == "DRAWDOWN":
            return  # sudah di-pause sebelumnya, skip notif ulang
        bot_paused = True
        _daily_limit_state["paused_by"] = "DRAWDOWN"
        msg = (
            f"🛑 <b>DRAWDOWN LIMIT TERCAPAI — Bot Di-PAUSE</b>\n"
            f"Drawdown: <b>{dd*100:.1f}%</b> (limit {MAX_DRAWDOWN*100:.0f}%)\n\n"
            f"⏸ Bot otomatis PAUSE — tidak ada trade baru.\n"
            f"Ketik /start atau /resumeorpause untuk melanjutkan secara manual."
        )
        print(f"⏸ BOT PAUSED (DRAWDOWN LIMIT REACHED: {dd*100:.1f}%)")
        send_telegram_raw(msg)


def update_performance(pnl):
    if pnl > 0:
        bot_state["wins"]        += 1
        bot_state["win_streak"]  += 1
        bot_state["lose_streak"]  = 0
        bot_state["day_wins"]    += 1
        bot_state["month_wins"]  += 1
    else:
        bot_state["losses"]      += 1
        bot_state["lose_streak"] += 1
        bot_state["win_streak"]   = 0
        bot_state["day_losses"]  += 1
        bot_state["month_losses"] += 1
    # ── Track PnL kumulatif per mode (LIVE / DEMO) ────────────────────────
    mode_data = bot_state["mode_pnl"].get(BOT_MODE)
    if mode_data is not None:
        mode_data["realized"] += pnl
        if pnl > 0:
            mode_data["wins"] += 1
        else:
            mode_data["losses"] += 1
    # ── Persist state setelah setiap trade close ──────────────────────────
    save_state()


def print_stats():
    total   = bot_state["wins"] + bot_state["losses"]
    winrate = (bot_state["wins"] / total * 100) if total > 0 else 0
    print(f"""
╔══════════════════════════════╗
║        📊 PERFORMANCE BOT AUTO TRADE    ║
╠══════════════════════════════╣
║ Winrate    : {winrate:.2f}%
║ Wins       : {bot_state['wins']}
║ Losses     : {bot_state['losses']}
║ Win Streak : {bot_state['win_streak']}
║ Lose Streak: {bot_state['lose_streak']}
╚══════════════════════════════╝
""")


def send_daily_summary():
    now       = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")

    if bot_state["last_summary_date"] == today_str:
        return
    if now.hour != DAILY_SUMMARY_HOUR_UTC:
        return

    try:
        current_balance = get_total_balance()
        refresh_pnl_snapshots(current_balance)
    except Exception:
        current_balance = 0

    # ── DAILY ──────────────────────────────────────────────────────────────
    day_wins   = bot_state.get("day_wins", 0)
    day_losses = bot_state.get("day_losses", 0)
    day_total  = day_wins + day_losses
    day_wr     = (day_wins / day_total * 100) if day_total > 0 else 0
    day_start  = bot_state.get("balance_day_start", 0)
    day_pnl    = current_balance - day_start if day_start > 0 else 0
    day_pnl_pct= (day_pnl / day_start * 100) if day_start > 0 else 0
    day_gross_win_pct  = day_pnl_pct if day_pnl >= 0 else 0.0
    day_gross_loss_pct = day_pnl_pct if day_pnl < 0 else 0.0
    day_net_pct= day_pnl_pct
    day_wr_bar_n = min(10, int(day_wr / 10))
    day_wr_bar   = "█" * day_wr_bar_n + "░" * (10 - day_wr_bar_n)
    day_label  = now.strftime("%Y-%m-%d")

    # ── MONTHLY ────────────────────────────────────────────────────────────
    mon_wins   = bot_state.get("month_wins", 0)
    mon_losses = bot_state.get("month_losses", 0)
    mon_total  = mon_wins + mon_losses
    mon_wr     = (mon_wins / mon_total * 100) if mon_total > 0 else 0
    mon_start  = bot_state.get("balance_month_start", 0)
    mon_pnl    = current_balance - mon_start if mon_start > 0 else 0
    mon_pnl_pct= (mon_pnl / mon_start * 100) if mon_start > 0 else 0
    mon_gross_win_pct  = mon_pnl_pct if mon_pnl >= 0 else 0.0
    mon_gross_loss_pct = mon_pnl_pct if mon_pnl < 0 else 0.0
    mon_net_pct= mon_pnl_pct
    mon_wr_bar_n = min(10, int(mon_wr / 10))
    mon_wr_bar   = "█" * mon_wr_bar_n + "░" * (10 - mon_wr_bar_n)
    mon_label  = now.strftime("%Y-%m")

    def _pct(v): return f"{'+'if v>=0 else ''}{v:.2f}%"
    def _em(v):  return "📈" if v >= 0 else "📉"

    msg = (
        f"📊 <b>PERFORMANCE SUMMARY</b>\n"
        f"{'─'*34}\n"
        f"📅 <b>DAILY ({day_label})</b>\n"
        f"  [{day_wr_bar}] WR: <b>{day_wr:.1f}%</b> ({day_wins}.0/{day_total}.0 poin)\n"
        f"  ✅ {day_wins} WIN ❌{day_losses} LOSS | Total:{day_total}\n"
        f"  {_em(day_gross_win_pct)} {_pct(day_gross_win_pct)}  "
        f"{'📉' if day_gross_loss_pct < 0 else '  '} {_pct(day_gross_loss_pct)}  "
        f"Net: <b>{_pct(day_net_pct)}</b>\n"
        f"  💰 Net PnL: <b>{_pct(day_net_pct)}</b>\n"
        f"{'─'*34}\n"
        f"🗓 <b>MONTHLY ({mon_label})</b>\n"
        f"  [{mon_wr_bar}] WR: <b>{mon_wr:.1f}%</b> ({mon_wins}.0/{mon_total}.0 poin)\n"
        f"  ✅ {mon_wins} WIN ❌{mon_losses} LOSS | Total:{mon_total}\n"
        f"  {_em(mon_gross_win_pct)} {_pct(mon_gross_win_pct)}  "
        f"{'📉' if mon_gross_loss_pct < 0 else '  '} {_pct(mon_gross_loss_pct)}  "
        f"Net: <b>{_pct(mon_net_pct)}</b>\n"
        f"  💰 Net PnL: <b>{_pct(mon_net_pct)}</b>\n"
        f"{'─'*34}\n"
        f"💵 Balance: <b>{current_balance:.2f} USDT</b> | ⚡ Bot aktif ✅"
    )

    send_telegram_raw(msg)
    bot_state["last_summary_date"] = today_str
    bot_state["signals_today"]     = 0


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 20b — TELEGRAM COMMAND HANDLER
# ═══════════════════════════════════════════════════════════════════════════
#
#  Commands:
#   /pnl              — unrealized PnL semua posisi aktif
#   /status           — status bot + ringkasan posisi
#   /changemargin <SYMBOL> <ISOLATED|CROSSED>
#   /changelev <SYMBOL> <1-125>
#   /resumeorpause    — toggle pause/resume scanning & trading
#   /closeallposition — market close semua posisi aktif
# ═══════════════════════════════════════════════════════════════════════════

def tg_get_updates(offset: int) -> list:
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": offset, "timeout": 20},   # ← FIX: long-poll 20s, cegah fetch ganda
            timeout=25,                                   # ← FIX: harus lebih besar dari long-poll timeout
        )
        data = r.json()
        return data.get("result", [])
    except Exception:
        return []


def cmd_pnl():
    """
    Kirim unrealized PnL semua posisi aktif.
    Data diambil LANGSUNG dari Binance /fapi/v2/positionRisk — bukan dari
    active_positions saja — sehingga posisi manual atau posisi yang tidak
    ter-sync juga ikut tampil.
    Keterangan ikon:
      🤖 = dibuka/dikelola bot
      📱 = dibuka manual di aplikasi (tidak ada di active_positions)
    """
    now_str = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")

    # Ambil semua posisi langsung dari Binance
    try:
        all_risk = api_get("/fapi/v2/positionRisk", signed=True)
        real_positions = [p for p in all_risk if float(p.get("positionAmt", 0)) != 0]
    except Exception as e:
        send_telegram_raw(
            f"💹 <b>/pnl — {now_str}</b>\n"
            f"{'─'*34}\n"
            f"❌ Gagal ambil data dari Binance: <code>{e}</code>"
        )
        return

    if not real_positions:
        send_telegram_raw(
            f"💹 <b>/pnl — {now_str}</b>\n"
            f"{'─'*34}\n"
            f"📭 Tidak ada posisi aktif di Binance."
        )
        return

    total_pnl = 0.0
    lines     = []

    for p in real_positions:
        symbol      = p["symbol"]
        amt         = float(p["positionAmt"])
        entry_price = float(p["entryPrice"])
        mark_price  = float(p["markPrice"])
        unrealized  = float(p["unRealizedProfit"])
        direction   = "LONG" if amt > 0 else "SHORT"
        dir_em      = "🟢" if direction == "LONG" else "🔴"
        src_em      = "🤖" if symbol in active_positions else "📱"

        total_pnl += unrealized
        pnl_em    = "🟢" if unrealized >= 0 else "🔴"
        pnl_str   = f"{pnl_em} {'+'if unrealized>=0 else ''}{unrealized:.2f} USDT"

        lines.append(
            f"{dir_em}{src_em} <b>{symbol}</b> {direction}\n"
            f"  Entry: {entry_price:.4f} | Mark: {mark_price:.4f}\n"
            f"  PnL  : {pnl_str}"
        )

    total_em  = "🟢" if total_pnl >= 0 else "🔴"
    total_str = f"{total_em} {'+'if total_pnl>=0 else ''}{total_pnl:.2f} USDT"
    bot_count    = sum(1 for p in real_positions if p["symbol"] in active_positions)
    manual_count = len(real_positions) - bot_count

    # ── Cumulative PNL harian & bulanan ─────────────────────────────────────
    try:
        current_bal = get_total_balance()
        refresh_pnl_snapshots(current_bal)
        pnl_summary = get_pnl_summary_lines(current_bal)
        bal_line    = f"💼 Balance : <b>{current_bal:.2f} USDT</b>\n"
    except Exception:
        pnl_summary = ""
        bal_line    = ""

    mode_pnl_block = get_mode_pnl_block()
    cumulative_block = (
        f"\n{'─'*34}\n"
        f"📈 <b>Cumulative PNL</b>\n"
        f"{pnl_summary}\n"
        f"{'─'*34}\n"
        f"📊 <b>PnL per Mode</b>\n"
        f"{mode_pnl_block}\n"
        f"💡 /resetpnl — reset PnL mode aktif"
    ) if (pnl_summary or True) else ""

    msg = (
        f"💹 <b>/pnl — {now_str}</b>\n"
        f"{'─'*34}\n"
        + ("\n" + "─"*34 + "\n").join(lines) +
        f"\n{'─'*34}\n"
        f"💹 Unrealized: <b>{total_str}</b>\n"
        f"{bal_line}"
        f"📊 Posisi: <b>{len(real_positions)}</b> (🤖{bot_count} | 📱{manual_count})"
        f"{cumulative_block}"
    )
    send_telegram_raw(msg)


def cmd_status():
    """Kirim status lengkap bot."""
    now     = datetime.now(timezone.utc)
    now_str = now.strftime("%d %b %Y %H:%M UTC")
    total   = bot_state["wins"] + bot_state["losses"]
    winrate = (bot_state["wins"] / total * 100) if total > 0 else 0

    try:
        bal = get_total_balance()
        bal_str = f"{bal:.2f} USDT"
    except Exception:
        bal_str = "N/A"

    uptime_str = "N/A"
    if bot_state.get("start_time"):
        up_secs = int((now - bot_state["start_time"]).total_seconds())
        up_h = up_secs // 3600
        up_m = (up_secs % 3600) // 60
        uptime_str = f"{up_h}j {up_m}m"

    state_em  = "⏸ PAUSED" if bot_paused else "▶️ RUNNING"
    mode_em   = "🔴 LIVE" if BOT_MODE == "LIVE" else "🟢 DEMO (testnet)"
    pre_count = sum(1 for p in active_positions.values() if p.get("pre_existing"))

    pos_lines = []
    for symbol, pos in active_positions.items():
        dir_em = "🟢" if pos["direction"] == "LONG" else "🔴"
        pre_em = "📥" if pos.get("pre_existing") else "🤖"
        price  = get_current_price(symbol)
        price_str = f"{price:,.4f}" if price else "N/A"
        pos_lines.append(f"  {dir_em}{pre_em} {symbol} {pos['direction']} | Entry:{pos['entry']} | Now:{price_str}")

    pos_block = "\n".join(pos_lines) if pos_lines else "  📭 Tidak ada posisi aktif"

    # ── Score threshold info ─────────────────────────────────────────────────
    if MIN_SCORE_CUSTOM > 0:
        score_line = (
            f"🧮 Score Gate : FULL ≥<b>{MIN_SCORE_CUSTOM}</b> | "
            f"RELAXED ≥<b>{MIN_SCORE_RELAXED_CUSTOM}</b> (custom)"
        )
    else:
        score_line = (
            f"🧮 Score Gate : FULL ≥<b>{MIN_SCORE}</b> | "
            f"RELAXED ≥<b>{MIN_SCORE_RELAXED}</b> (default)"
        )

    # ── BTC correlation filter info ──────────────────────────────────────────
    btc_corr_line = (
        f"🔗 BTC+Dom Filter: <b>{'✅ ON' if BTC_CORR_FILTER_ON else '⭕ OFF'}</b>"
    )

    # ── Scan mode filter info ────────────────────────────────────────────────
    active_mode_labels = [m["label"] for m in get_active_modes()]
    mode_filter_em = {"ALL": "🔄", "SCALPING": "⚡", "INTRADAY": "📈"}.get(_ACTIVE_MODE_FILTER, "🔄")
    scan_mode_line = (
        f"📡 Scan Mode  : {mode_filter_em} <b>{_ACTIVE_MODE_FILTER}</b> "
        f"({', '.join(active_mode_labels)})"
    )
    dir_filter_em = {"ALL": "🔄", "LONG": "🟢", "SHORT": "🔴"}.get(_DIRECTION_FILTER, "🔄")
    dir_filter_line = (
        f"🧭 Arah Filter: {dir_filter_em} <b>{_DIRECTION_FILTER}</b>"
    )
    # BTC situational info for /status
    _sit_icons = {
        "BTC_NEAR_DEMAND_BOUNCE":  "⚠️ BTC Near Demand — SHORT alt DIBLOK",
        "BTC_NEAR_DEMAND_FALLING": "⚡ BTC Near Demand — SHORT berisiko",
        "BTC_NEAR_SUPPLY_REJECT":  "⚠️ BTC Near Supply — LONG alt DIBLOK",
        "BTC_NEAR_SUPPLY_RISING":  "⚡ BTC Near Supply — LONG berisiko",
        "BTC_TRENDING_BULLISH":    "🟢 BTC Strong Up — bonus LONG",
        "BTC_TRENDING_BEARISH":    "🔴 BTC Strong Down — bonus SHORT",
        "BTC_EXTENDED":            "➡️ BTC Ranging — sinyal lebih selektif",
        "BTC_NEUTRAL":             "🔄 BTC Neutral",
    }
    try:
        _sit_df_ltf = fetch_btc_ltf_data(BTC_LTF_TF, limit=60)
        _sit_df_htf = fetch_ohlcv("BTC/USDT", "1h", limit=100)
        _sit_btcd   = fetch_btcd_ohlcv(tf=BTCDOM_LTF_TF, limit=80)
        _sit_ltf_dir = get_btc_ltf_direction(_sit_df_ltf)
        _sit_btcd_dir = get_btcd_ltf_direction(_sit_btcd)
        _sit_result  = analyze_btc_situation(
            df_btc_ltf    = _sit_df_ltf,
            df_btc_htf    = _sit_df_htf,
            btc_bias_htf  = get_btc_bias(),
            btcd_ltf_dir  = _sit_btcd_dir,
            btcd_trend_htf= get_btcd_bias(),
        )
        _sit_label = _sit_icons.get(_sit_result["situation"], "🔄 BTC Neutral")
        btc_situation_line = f"🪙 BTC Situation: <b>{_sit_label}</b>"
    except Exception:
        btc_situation_line = "🪙 BTC Situation: <i>data tidak tersedia</i>"

    sl_filter_line = (
        f"🛑 SL Filter  : <b>max {MAX_SL_DISTANCE_PCT*100:.0f}%</b> dari entry "
        f"({'aktif' if MAX_SL_DISTANCE_PCT > 0 else 'OFF'})"
    )

    # ── Super Scalper Mode badge ─────────────────────────────────────────────
    super_scalper_line = ""
    if _SUPER_SCALPER_MODE:
        super_scalper_line = (
            f"\n⚡⚡ <b>SUPER SCALPER MODE: ON</b> ⚡⚡\n"
            f"  Pairs: {len(_SUPER_SCALPER_PAIRS)} | Score≥45 | RR≥1.8 | SL≤1.2% | TP1=0.6% | SL CD=1j"
        )

    msg = (
        f"🤖 <b>BOT STATUS — {now_str}</b>\n"
        f"{'─'*38}\n"
        f"⚡ State      : <b>{state_em}</b>\n"
        f"🌐 Mode       : <b>{mode_em}</b>\n"
        f"⏱ Uptime     : {uptime_str}\n"
        f"💼 Balance    : <b>{bal_str}</b>\n"
        f"📡 Data Src   : {_active_data_source}\n"
        f"{'─'*38}\n"
        f"📊 Performance\n"
        f"  ✅ Win      : {bot_state['wins']}\n"
        f"  ❌ Loss     : {bot_state['losses']}\n"
        f"  🎯 Winrate  : {winrate:.1f}%\n"
        f"  🔔 Sinyal   : {bot_state['signals_today']} hari ini\n"
        f"{'─'*38}\n"
        f"📌 Posisi Aktif ({len(active_positions)}, {pre_count} pre-existing):\n"
        f"{pos_block}\n"
        f"{'─'*38}\n"
        f"⚙️ Config:\n"
        f"  Max SL {MAX_SL_LOSS_PCT*100:.1f}%/trade | Max {MAX_OPEN_TRADES} trades | Lev auto-tier\n"
        f"  {score_line}\n"
        f"  {btc_corr_line}\n"
        f"  {scan_mode_line}\n"
        f"  {dir_filter_line}\n"
        f"  {btc_situation_line}\n"
        f"  {sl_filter_line}"
        f"{super_scalper_line}"
    )
    send_telegram_raw(msg)


def cmd_changemargin(parts: list):
    """
    /changemargin <SYMBOL> <ISOLATED|CROSSED>
    Ganti margin type untuk sebuah symbol.
    """
    if len(parts) < 3:
        send_telegram_raw(
            "⚠️ Format salah.\n"
            "Gunakan: <code>/changemargin BTCUSDT ISOLATED</code>\n"
            "atau    : <code>/changemargin BTCUSDT CROSSED</code>"
        )
        return

    symbol      = parts[1].upper()
    margin_type = parts[2].upper()

    if margin_type not in ("ISOLATED", "CROSSED"):
        send_telegram_raw("⚠️ Margin type harus <b>ISOLATED</b> atau <b>CROSSED</b>.")
        return

    try:
        api_post("/fapi/v1/marginType", {"symbol": symbol, "marginType": margin_type})
        send_telegram_raw(
            f"✅ <b>Margin type berhasil diubah</b>\n"
            f"{'─'*30}\n"
            f"📌 Symbol : <b>{symbol}</b>\n"
            f"🔧 Type   : <b>{margin_type}</b>"
        )
    except Exception as e:
        err = str(e)
        if "No need to change" in err or "-4046" in err:
            send_telegram_raw(f"ℹ️ {symbol} sudah dalam mode <b>{margin_type}</b>, tidak perlu diubah.")
        else:
            send_telegram_raw(f"❌ Gagal ubah margin type {symbol}:\n<code>{err}</code>")


def cmd_changelev(parts: list):
    """
    /changelev <SYMBOL> <1-125>
    Ganti leverage untuk sebuah symbol.
    """
    if len(parts) < 3:
        send_telegram_raw(
            "⚠️ Format salah.\n"
            "Gunakan: <code>/changelev BTCUSDT 20</code>"
        )
        return

    symbol = parts[1].upper()
    try:
        lev = int(parts[2])
        if not (1 <= lev <= 125):
            raise ValueError("out of range")
    except ValueError:
        send_telegram_raw("⚠️ Leverage harus angka antara <b>1–125</b>.")
        return

    try:
        result = api_post("/fapi/v1/leverage", {"symbol": symbol, "leverage": lev})
        max_notional = result.get("maxNotionalValue", "N/A")
        send_telegram_raw(
            f"✅ <b>Leverage berhasil diubah</b>\n"
            f"{'─'*30}\n"
            f"📌 Symbol    : <b>{symbol}</b>\n"
            f"⚡ Leverage  : <b>{lev}x</b>\n"
            f"📊 Max Notional: {max_notional}"
        )
    except Exception as e:
        send_telegram_raw(f"❌ Gagal ubah leverage {symbol}:\n<code>{str(e)}</code>")


def cmd_resumeorpause():
    """Toggle pause/resume bot scanning & trading."""
    global bot_paused
    bot_paused = not bot_paused

    if bot_paused:
        msg = (
            f"⏸ <b>Bot di-PAUSE</b>\n"
            f"{'─'*30}\n"
            f"Scanning & trading dihentikan sementara.\n"
            f"Posisi aktif tetap dimonitor.\n"
            f"Ketik /resumeorpause untuk melanjutkan."
        )
    else:
        _daily_limit_state["paused_by"] = None  # reset agar limit/drawdown bisa re-trigger
        msg = (
            f"▶️ <b>Bot di-RESUME</b>\n"
            f"{'─'*30}\n"
            f"Scanning & trading aktif kembali.\n"
            f"Ketik /resumeorpause untuk pause."
        )
    send_telegram_raw(msg)
    print(f"{'⏸ PAUSED' if bot_paused else '▶️ RESUMED'}")
    save_state()


def cmd_start():
    """
    /start — Resume bot dari kondisi PAUSED saat startup.
    Bisa dipakai kapan saja untuk pastikan bot dalam kondisi RUNNING.
    """
    global bot_paused
    if not bot_paused:
        send_telegram_raw(
            f"▶️ <b>Bot sudah berjalan!</b>\n"
            f"Gunakan /resumeorpause untuk pause, atau /status untuk cek kondisi."
        )
        return

    bot_paused = False
    _daily_limit_state["paused_by"] = None  # reset agar drawdown/limit bisa re-trigger jika perlu

    # ── Sync posisi BARU setelah user konfirmasi via /start ───────────────────
    # Ini sengaja ditunda dari startup agar tidak false-close posisi LIVE
    # karena bot sempat konek ke endpoint yang salah saat masih paused.
    # Sekarang user sudah pastikan mode benar → aman untuk sync.
    send_telegram_raw("🔄 <b>Menghubungkan ke Binance...</b> sync posisi aktif...")
    try:
        active_positions.clear()          # bersihkan snapshot lama (mungkin dari mode lain)
        sync_existing_positions()         # ambil posisi real dari Binance sesuai BOT_MODE saat ini
        pos_count = len(active_positions)
        pre_count = sum(1 for p in active_positions.values() if p.get("pre_existing"))
        if pos_count > 0:
            pos_note = f"\n📌 Posisi terdeteksi: <b>{pos_count}</b> ({pre_count} pre-existing, tetap dimonitor)"
        else:
            pos_note = "\n📭 Tidak ada posisi aktif saat ini."
    except Exception as e:
        pos_note = f"\n⚠️ Sync posisi gagal: {e}"
        print(f"⚠️ sync_existing_positions gagal saat /start: {e}")

    # ── Auto cleanup orphan conditional orders (tanpa bilang ke user — cukup log) ──
    # Setelah sync posisi, langsung bersihkan conditional orders yang tidak punya
    # posisi aktif. Ini menangani kasus: posisi close tapi SL/TP orphan tertinggal.
    # Dijalankan otomatis — user tidak perlu ketik /cleanuporders manual.
    try:
        print("  🧹 Auto-cleanup orphan orders saat startup...")
        cleanup_stale_orders(dry_run=False, silent_if_clean=True)
    except Exception as _cleanup_err:
        print(f"  ⚠️  Auto-cleanup startup gagal (non-fatal): {_cleanup_err}")

    try:
        bal = get_total_balance()
        bal_str = f"{bal:.2f} USDT"
    except Exception:
        bal_str = "N/A"

    mode_em = "🔴 LIVE" if BOT_MODE == "LIVE" else "🟢 DEMO (testnet)"
    send_telegram_raw(
        f"🚀 <b>Bot STARTED!</b>\n"
        f"{'─'*34}\n"
        f"▶️ Scanning & trading aktif.\n"
        f"💼 Balance   : <b>{bal_str}</b>\n"
        f"🌐 Mode      : <b>{mode_em}</b>\n"
        f"⚡ Max SL/trade: <b>{MAX_SL_LOSS_PCT*100:.1f}%</b> dari balance | Max: <b>{MAX_OPEN_TRADES}</b> trades\n"
        f"{'─'*34}\n"
        f"Gunakan /status untuk cek kondisi bot."
        f"{pos_note}"
    )
    print("▶️ Bot STARTED via /start command")
    save_state()


def cmd_closeallposition():
    """
    Tutup SEMUA posisi aktif di Binance dengan market order.
    Sumber: query langsung dari Binance positionRisk — BUKAN hanya active_positions memory.
    Ini memastikan posisi pre-existing / dibuka manual juga ikut tertutup.
    """
    send_telegram_raw("🔄 <b>Mengecek posisi di Binance...</b>")

    # ── Ambil semua posisi real dari Binance (bukan dari memory bot) ──────────
    try:
        all_risk = api_get("/fapi/v2/positionRisk", signed=True)
    except Exception as e:
        send_telegram_raw(f"❌ Gagal ambil posisi dari Binance: {e}")
        return

    # Filter hanya yang benar-benar open (positionAmt != 0)
    open_positions = [p for p in all_risk if float(p.get("positionAmt", 0)) != 0]

    if not open_positions:
        send_telegram_raw("📭 Tidak ada posisi aktif untuk ditutup.")
        return

    send_telegram_raw(
        f"🔄 <b>Menutup semua posisi...</b>\n"
        f"Total: {len(open_positions)} posisi (dari Binance langsung)"
    )

    closed = []
    failed = []

    for p in open_positions:
        symbol   = p["symbol"]
        pos_amt  = float(p["positionAmt"])
        # positionAmt positif = LONG, negatif = SHORT
        direction  = "LONG" if pos_amt > 0 else "SHORT"
        close_side = "SELL" if direction == "LONG" else "BUY"
        abs_qty    = abs(pos_amt)

        try:
            # Cancel semua open orders untuk symbol ini dulu
            cancel_open_orders(symbol)
            time.sleep(0.2)

            # Pakai mkt_step_size dan quantityPrecision agar qty tidak ditolak Binance
            filters       = get_lot_filters(symbol)
            mkt_step_size = filters.get("mkt_stepSize", filters.get("stepSize", 0.001))
            cl_qty_prec   = filters.get("quantityPrecision", None)
            close_qty     = round_lot_to_step(abs_qty, mkt_step_size)
            if cl_qty_prec is not None:
                close_qty = round(math.floor(close_qty * (10 ** cl_qty_prec)) / (10 ** cl_qty_prec), cl_qty_prec)
            close_qty_str = format_qty(close_qty, mkt_step_size, cl_qty_prec)

            # Deteksi hedge mode dari positionSide field
            pos_side_field = p.get("positionSide", "BOTH")
            close_params = {
                "symbol":   symbol,
                "side":     close_side,
                "type":     "MARKET",
                "quantity": close_qty_str,
            }
            if pos_side_field in ("LONG", "SHORT"):
                # Hedge mode — gunakan positionSide, TIDAK pakai reduceOnly
                close_params["positionSide"] = pos_side_field
            else:
                # One-way mode — pakai reduceOnly
                close_params["reduceOnly"] = "true"

            print(f"  📤 CLOSE PARAMS [{symbol}]: {close_params}")
            resp = api_post("/fapi/v1/order", close_params)
            print(f"  ✅ CLOSE RESP [{symbol}]: orderId={resp.get('orderId')} status={resp.get('status')} executedQty={resp.get('executedQty')}")
            send_telegram_raw(
                f"📤 <b>Order close terkirim — {symbol}</b>\n"
                f"orderId: <code>{resp.get('orderId')}</code>\n"
                f"status: <code>{resp.get('status')}</code>\n"
                f"executedQty: <code>{resp.get('executedQty')}</code>\n"
                f"side: <code>{close_params.get('side')}</code> | qty: <code>{close_params.get('quantity')}</code>\n"
                f"positionSide: <code>{close_params.get('positionSide', 'BOTH')}</code> | reduceOnly: <code>{close_params.get('reduceOnly', 'false')}</code>"
            )

            # Estimasi PnL
            entry_price = float(p.get("entryPrice", 0))
            cur_price   = get_current_price(symbol)
            pnl_str     = "N/A"
            is_win      = False
            if cur_price and entry_price:
                pnl    = (cur_price - entry_price) * abs_qty if direction == "LONG" else (entry_price - cur_price) * abs_qty
                pnl_em = "🟢" if pnl >= 0 else "🔴"
                pnl_str = f"{pnl_em} {'+'if pnl>=0 else ''}{pnl:.2f} USDT"
                is_win  = pnl >= 0

            _perf_pnl = pnl if (cur_price and entry_price) else (1 if is_win else -1)
            update_performance(_perf_pnl)
            closed.append((symbol, direction, pnl_str))

            # Hapus dari memory bot jika ada
            if symbol in active_positions:
                del active_positions[symbol]
            set_cooldown_post_close(symbol, reason="manual /closeallposition")

            print(f"🔴 CLOSED {symbol} {direction} qty={close_qty_str} | PnL: {pnl_str}")

        except Exception as e:
            failed.append((symbol, str(e)))
            print(f"❌ Gagal close {symbol}: {e}")

    # ── Kirim laporan hasil ───────────────────────────────────────────────────
    lines = []
    for sym, dir_, pnl_s in closed:
        lines.append(f"  ✅ {sym} {dir_} | PnL: {pnl_s}")
    for sym, err in failed:
        lines.append(f"  ❌ {sym} GAGAL: {err}")

    wins  = bot_state["wins"]
    losses = bot_state["losses"]
    total = wins + losses
    wr    = (wins / total * 100) if total > 0 else 0
    msg = (
        f"🔴 <b>Close All Positions — Selesai</b>\n"
        f"{'─'*34}\n"
        + "\n".join(lines) +
        f"\n{'─'*34}\n"
        f"✅ {len(closed)} ditutup | ❌ {len(failed)} gagal\n"
        f"{'─'*34}\n"
        f"📊 Total: ✅ Win: <b>{wins}</b> | ❌ Loss: <b>{losses}</b> | 🎯 WR: <b>{wr:.1f}%</b>"
    )
    send_telegram_raw(msg)


def cmd_setmarginratio(parts: list):
    """
    /setmarginratio <persen>
    Set MAX_SL_LOSS_PCT — batas kerugian MAKSIMAL per trade jika SL kena,
    dihitung sebagai % dari total balance.

    Lot dihitung MUNDUR dari batas ini:
      lot = (balance × MAX_SL_LOSS_PCT) / sl_distance
    → SL dekat: lot lebih besar. SL jauh: lot lebih kecil.
    → Kerugian aktual saat SL kena TIDAK AKAN MELEBIHI angka ini.

    Contoh: /setmarginratio 1   → jika SL kena, max rugi 1% dari balance
            /setmarginratio 2   → jika SL kena, max rugi 2% dari balance
    Range  : 0.1% – 20%
    """
    global MAX_SL_LOSS_PCT, MARGIN_RATIO
    if len(parts) < 2:
        try:
            bal = get_total_balance()
            max_loss_usdt = bal * MAX_SL_LOSS_PCT
            bal_line = (
                f"💰 Max loss/trade saat ini: <b>{max_loss_usdt:.2f} USDT</b> "
                f"(dari balance {bal:.2f})"
            )
        except Exception:
            bal_line = ""
        send_telegram_raw(
            "⚠️ Format salah.\n"
            "Gunakan: <code>/setmarginratio 1</code>\n"
            "Contoh nilai: 0.5, 1, 2 (dalam persen max SL loss per trade)\n"
            f"Max SL loss saat ini: <b>{MAX_SL_LOSS_PCT * 100:.2f}%</b> dari balance\n"
            f"{bal_line}\n"
            "ℹ️ Lot dihitung otomatis agar loss MAKSIMAL saat SL kena = angka ini.\n"
            "ℹ️ SL dekat → lot lebih besar | SL jauh → lot lebih kecil."
        )
        return

    try:
        new_pct = float(parts[1].replace(",", "."))
    except ValueError:
        send_telegram_raw("⚠️ Masukkan angka yang valid. Contoh: <code>/setmarginratio 1</code>")
        return

    if not (0.1 <= new_pct <= 20.0):
        send_telegram_raw(
            "⚠️ Max SL loss harus antara <b>0.1%</b> sampai <b>20%</b>.\n"
            "Disarankan: 0.5–3% untuk manajemen risiko yang sehat."
        )
        return

    old_pct          = MAX_SL_LOSS_PCT * 100
    MAX_SL_LOSS_PCT  = new_pct / 100.0
    MARGIN_RATIO     = MAX_SL_LOSS_PCT   # alias

    if new_pct <= 1.0:
        risk_note = "🟢 Konservatif — risiko kecil per trade"
    elif new_pct <= 3.0:
        risk_note = "🟡 Moderat — standar risk management"
    elif new_pct <= 7.0:
        risk_note = "🟠 Agresif — hati-hati drawdown"
    else:
        risk_note = "🔴 Sangat agresif — risiko tinggi!"

    try:
        bal           = get_total_balance()
        max_loss_usdt = bal * MAX_SL_LOSS_PCT
        bal_line      = (
            f"💰 Max loss/trade: <b>{max_loss_usdt:.2f} USDT</b> "
            f"(dari balance {bal:.2f})"
        )
    except Exception:
        bal_line = ""

    send_telegram_raw(
        f"✅ <b>Max SL Loss Per Trade Diubah</b>\n"
        f"{'─'*34}\n"
        f"📉 Sebelum : <b>{old_pct:.2f}%</b>\n"
        f"📈 Sekarang: <b>{new_pct:.2f}%</b> dari total balance\n"
        f"{'─'*34}\n"
        f"{bal_line}\n"
        f"{risk_note}\n"
        f"{'─'*34}\n"
        f"ℹ️ Lot dihitung mundur dari batas ini:\n"
        f"   lot = (balance × {new_pct:.1f}%) ÷ jarak_SL\n"
        f"ℹ️ Loss aktual saat SL kena ≤ {new_pct:.2f}% balance.\n"
        f"ℹ️ Berlaku untuk trade baru. Posisi aktif tidak berubah."
    )
    print(f"⚙️ MAX_SL_LOSS_PCT diubah: {old_pct:.2f}% → {new_pct:.2f}%")
    save_state()


def cmd_maxopentrade(parts: list):
    """
    /maxopentrade <jumlah>
    Ubah MAX_OPEN_TRADES secara live tanpa restart bot.
    Contoh: /maxopentrade 3
    Range  : 1 – 20
    """
    global MAX_OPEN_TRADES
    if len(parts) < 2:
        send_telegram_raw(
            "⚠️ Format salah.\n"
            "Gunakan: <code>/maxopentrade 3</code>\n"
            f"Max open trades saat ini: <b>{MAX_OPEN_TRADES}</b>\n"
            f"Posisi aktif sekarang   : <b>{len(active_positions)}</b>"
        )
        return

    try:
        new_max = int(parts[1])
    except ValueError:
        send_telegram_raw("⚠️ Masukkan angka bulat. Contoh: <code>/maxopentrade 3</code>")
        return

    if not (1 <= new_max <= 20):
        send_telegram_raw("⚠️ Jumlah posisi harus antara <b>1</b> sampai <b>20</b>.")
        return

    global _user_set_max_trades
    old_max      = MAX_OPEN_TRADES
    MAX_OPEN_TRADES = new_max
    _user_set_max_trades = True   # user sudah set manual → jadi hard cap
    active_count = len(active_positions)

    # Peringatan jika new_max lebih kecil dari posisi aktif saat ini
    warn = ""
    if new_max < active_count:
        warn = (
            f"\n⚠️ <b>Perhatian:</b> Posisi aktif ({active_count}) melebihi limit baru ({new_max}).\n"
            f"Bot tidak akan buka posisi baru sampai posisi aktif berkurang."
        )

    # Saran berdasarkan modal (dari dynamic tier)
    try:
        bal = get_total_balance()
        bal_info = f"💰 Balance saat ini: <b>${bal:.2f}</b>\n"
    except Exception:
        bal_info = ""

    send_telegram_raw(
        f"✅ <b>Max Open Trades Diubah</b>\n"
        f"{'─'*34}\n"
        f"📉 Sebelum : <b>{old_max}</b> posisi\n"
        f"📈 Sekarang: <b>{new_max}</b> posisi\n"
        f"📌 Aktif   : <b>{active_count}</b> posisi\n"
        f"{'─'*34}\n"
        f"{bal_info}"
        f"ℹ️ Dynamic tier tetap aktif sebagai batas atas otomatis.{warn}"
    )
    print(f"⚙️ MAX_OPEN_TRADES diubah: {old_max} → {new_max}")
    save_state()





def cmd_setfixedlev(parts: list):
    """
    /setfixedlev <leverage>
    Set leverage tetap untuk semua trade. Contoh: /setfixedlev 10
    Override tier dinamis berdasarkan harga.
    Gunakan /resetmm untuk kembali ke leverage tier otomatis.
    """
    global FIXED_LEVERAGE
    if len(parts) < 2:
        status = f"<b>{FIXED_LEVERAGE}x (fixed)</b>" if FIXED_LEVERAGE > 0 else "<b>OFF</b> (auto-tier by price)"
        send_telegram_raw(
            "⚠️ Format salah.\n"
            "Gunakan: <code>/setfixedlev 10</code>\n"
            "Contoh: /setfixedlev 10  → semua trade pakai 10x leverage\n"
            f"Status saat ini: {status}\n"
            "Gunakan /resetmm untuk kembali ke leverage auto-tier."
        )
        return

    try:
        val = int(parts[1])
        if not (1 <= val <= 125):
            raise ValueError("out of range")
    except ValueError:
        send_telegram_raw("⚠️ Leverage harus angka bulat antara <b>1–125</b>.")
        return

    old = FIXED_LEVERAGE
    FIXED_LEVERAGE = val

    old_str = f"{old}x (fixed)" if old > 0 else "auto-tier by price"
    margin_note = f"📊 Max SL/trade: <b>{MAX_SL_LOSS_PCT*100:.1f}% dari total balance</b>"

    send_telegram_raw(
        f"✅ <b>Fixed Leverage Set</b>\n"
        f"{'─'*34}\n"
        f"⚡ Sebelum  : <b>{old_str}</b>\n"
        f"⚡ Sekarang : <b>{val}x (semua trade)</b>\n"
        f"{margin_note}\n"
        f"{'─'*34}\n"
        f"ℹ️ Berlaku untuk trade baru.\n"
        f"Gunakan /resetmm untuk kembali ke leverage auto-tier."
    )
    print(f"⚙️ FIXED_LEVERAGE: {old} → {val}")
    save_state()


def cmd_resetmm():
    """
    /resetmm
    Reset leverage fixed → kembali ke auto-tier by price.
    MAX_SL_LOSS_PCT tidak direset oleh command ini —
    gunakan /setmarginratio untuk mengubahnya.
    """
    global FIXED_LEVERAGE

    was_lev = FIXED_LEVERAGE
    FIXED_LEVERAGE = 0

    lev_was = f"{was_lev}x fixed" if was_lev > 0 else "sudah auto-tier"

    send_telegram_raw(
        f"🔄 <b>Leverage Reset ke Auto-Tier</b>\n"
        f"{'─'*34}\n"
        f"⚡ Leverage: {lev_was} → <b>auto-tier by price</b>\n"
        f"📊 Max SL : <b>{MAX_SL_LOSS_PCT*100:.1f}% dari balance per trade</b> (tidak berubah)\n"
        f"{'─'*34}\n"
        f"ℹ️ Bot kembali ke leverage otomatis berdasarkan harga pair.\n"
        f"Gunakan /setmarginratio untuk ubah max SL loss % per trade."
    )
    print(f"⚙️ MM reset: lev {was_lev}→0 (kembali dinamis)")
    save_state()


def cmd_togglebtcfilter():
    """
    /togglebtcfilter
    Toggle filter korelasi BTC + BTC.D (gabungan) ON/OFF.

    Logika kombinasi saat ON:
      BTC ↑ + BTC.D ↓  → LONG altcoin  ✅  (altcoin season)
      BTC ↑ + BTC.D →  → LONG altcoin  ✅  (alt ikut BTC)
      BTC ↑ + BTC.D ↑  → SKIP          ⛔  (BTC rally murni, ambiguous)
      BTC ↓ + BTC.D ↑  → SHORT altcoin ✅  (alt paling terpukul)
      BTC ↓ + BTC.D →  → SHORT altcoin ✅  (alt ikut BTC turun)
      BTC ↓ + BTC.D ↓  → SKIP          ⛔  (ambiguous)
      BTC → + BTC.D ↓  → LONG altcoin  ✅  (BTC sideways, dom turun)
      BTC → + BTC.D ↑  → SHORT altcoin ✅  (flow ke BTC, dom naik)
      BTC → + BTC.D →  → SKIP          ⛔  (tidak ada arah)
    BTCUSDT: hanya pakai arah BTC price (tidak perlu cek dom).
    """
    global BTC_CORR_FILTER_ON
    BTC_CORR_FILTER_ON = not BTC_CORR_FILTER_ON
    state_em = "✅ ON" if BTC_CORR_FILTER_ON else "⭕ OFF"

    if BTC_CORR_FILTER_ON:
        detail = (
            f"\n{'─'*34}\n"
            f"📌 <b>Tabel Kombinasi (altcoin):</b>\n"
            f"  BTC↑ + Dom↓ → <b>LONG alt</b>   ✅\n"
            f"  BTC↑ + Dom→ → <b>LONG alt</b>   ✅\n"
            f"  BTC↑ + Dom↑ → <b>SKIP</b>        ⛔\n"
            f"  BTC↓ + Dom↑ → <b>SHORT alt</b>  ✅\n"
            f"  BTC↓ + Dom→ → <b>SHORT alt</b>  ✅\n"
            f"  BTC↓ + Dom↓ → <b>SKIP</b>        ⛔\n"
            f"  BTC→ + Dom↓ → <b>LONG alt</b>   ✅\n"
            f"  BTC→ + Dom↑ → <b>SHORT alt</b>  ✅\n"
            f"  BTC→ + Dom→ → <b>SKIP</b>        ⛔\n"
            f"  BTCUSDT: hanya cek arah BTC price\n"
            f"\n⚠️ Perlu <code>BTCDOMUSDT</code> di Binance Futures.\n"
            f"Jika BTC.D tidak tersedia → dom dianggap RANGING."
        )
    else:
        detail = "\n\nℹ️ Semua sinyal lolos tanpa cek korelasi BTC + BTC.D."

    send_telegram_raw(
        f"🔗 <b>BTC + BTC.D Correlation Filter</b>\n"
        f"{'─'*34}\n"
        f"Status : <b>{state_em}</b>\n"
        f"BTC TF : <b>{BTC_BIAS_TF}</b>  |  BTC.D TF: <b>{BTCD_TF}</b>"
        f"{detail}"
    )
    print(f"⚙️ BTC_CORR_FILTER_ON → {BTC_CORR_FILTER_ON}")
    save_state()


def cmd_setscoreupto(parts: list):
    """
    /setscoreupto <score>
    Set batas minimum score sinyal yang akan dieksekusi bot.

    Contoh:
      /setscoreupto 60   → semua tier (FULL & RELAXED) wajib score ≥ 60 pts
      /setscoreupto 45   → semua tier wajib score ≥ 45 pts
      /setscoreupto 0    → reset ke default (45 FULL / 28 RELAXED)

    PENTING: RELAXED tier (LOW_TF / LTF_30M) kena gate yang SAMA — tidak ada diskon 60%.
    Range yang diizinkan: 1 – 100 pts.
    Gunakan /setscoreupto 0 atau /resetmm untuk kembali ke default.
    """
    global MIN_SCORE_CUSTOM, MIN_SCORE_RELAXED_CUSTOM

    if len(parts) < 2:
        if MIN_SCORE_CUSTOM > 0:
            status_full    = f"<b>{MIN_SCORE_CUSTOM} pts (custom)</b>"
            status_relaxed = f"<b>{MIN_SCORE_RELAXED_CUSTOM} pts (custom, 60%)</b>"
        else:
            status_full    = f"<b>{MIN_SCORE} pts (default)</b>"
            status_relaxed = f"<b>{MIN_SCORE_RELAXED} pts (default)</b>"
        send_telegram_raw(
            "⚠️ Format: <code>/setscoreupto &lt;score&gt;</code>\n"
            "Contoh  : <code>/setscoreupto 70</code>\n"
            f"{'─'*34}\n"
            f"📊 Threshold FULL tier    : {status_full}\n"
            f"📊 Threshold RELAXED tier : {status_relaxed}\n"
            f"{'─'*34}\n"
            "Range valid : <b>1 – 100</b> (0 = reset ke default)\n"
            "Gunakan <code>/setscoreupto 0</code> untuk reset ke default."
        )
        return

    try:
        val = int(parts[1])
    except ValueError:
        send_telegram_raw("⚠️ Masukkan angka bulat. Contoh: <code>/setscoreupto 70</code>")
        return

    # Reset ke default
    if val == 0:
        old_full    = MIN_SCORE_CUSTOM if MIN_SCORE_CUSTOM > 0 else MIN_SCORE
        old_relaxed = MIN_SCORE_RELAXED_CUSTOM if MIN_SCORE_CUSTOM > 0 else MIN_SCORE_RELAXED
        MIN_SCORE_CUSTOM         = 0
        MIN_SCORE_RELAXED_CUSTOM = 0
        send_telegram_raw(
            f"🔄 <b>Score Filter Reset ke Default</b>\n"
            f"{'─'*34}\n"
            f"📊 FULL tier    : {old_full} pts → <b>{MIN_SCORE} pts (default)</b>\n"
            f"📊 RELAXED tier : {old_relaxed} pts → <b>{MIN_SCORE_RELAXED} pts (default)</b>\n"
            f"{'─'*34}\n"
            f"ℹ️ Bot kembali ke threshold scoring default."
        )
        print(f"⚙️ MIN_SCORE_CUSTOM reset → default ({MIN_SCORE}/{MIN_SCORE_RELAXED})")
        return

    if not (1 <= val <= 100):
        send_telegram_raw("⚠️ Score harus antara <b>1 – 100</b>.\nGunakan <code>/setscoreupto 0</code> untuk reset ke default.")
        return

    old_full    = MIN_SCORE_CUSTOM if MIN_SCORE_CUSTOM > 0 else MIN_SCORE
    old_relaxed = MIN_SCORE_RELAXED_CUSTOM if MIN_SCORE_CUSTOM > 0 else MIN_SCORE_RELAXED

    relaxed_val = val   # RELAXED tier pakai gate yang SAMA — tidak ada diskon

    MIN_SCORE_CUSTOM         = val
    MIN_SCORE_RELAXED_CUSTOM = relaxed_val

    # Warna peringatan berdasarkan nilai
    if val < 30:
        warn = "\n⚠️ Score sangat rendah — sinyal lebih sering tapi winrate bisa turun!"
    elif val < 40:
        warn = "\n📢 Score cukup rendah — lebih banyak sinyal, perhatikan drawdown."
    elif val > 70:
        warn = "\n🎯 Score tinggi (Sniper mode) — sinyal sangat selektif, jarang tapi lebih akurat."
    else:
        warn = ""

    send_telegram_raw(
        f"✅ <b>Score Filter Diubah</b>\n"
        f"{'─'*34}\n"
        f"📊 FULL tier    : {old_full} → <b>{val} pts</b>\n"
        f"📊 RELAXED tier : {old_relaxed} → <b>{relaxed_val} pts</b> (sama, tidak ada diskon)\n"
        f"{'─'*34}\n"
        f"ℹ️ Berlaku untuk sinyal baru. Semua tier wajib ≥ {val} pts.\n"
        f"Gunakan <code>/setscoreupto 0</code> untuk reset ke default."
        f"{warn}"
    )
    print(f"⚙️ MIN_SCORE_CUSTOM: {old_full}→{val} | RELAXED: {old_relaxed}→{relaxed_val}")
    save_state()


def cmd_changeliveordemo():
    """
    /changeliveordemo
    Toggle antara mode LIVE (mainnet Binance) dan DEMO (testnet).
    LIVE  = uang asli — order masuk ke akun Binance mainnet.
    DEMO  = testnet   — order masuk ke testnet, tidak ada uang asli.
    """
    global BOT_MODE
    if BOT_MODE == "DEMO":
        BOT_MODE = "LIVE"
        url_now  = BASE_URL_LIVE
        warn = (
            "\n⚠️ <b>PERINGATAN:</b> Mode LIVE aktif!\n"
            "Semua order berikutnya masuk ke <b>akun Binance ASLI</b>.\n"
            "Pastikan API key yang kamu pakai adalah key mainnet, bukan testnet!"
        )
        em = "🔴"
    else:
        BOT_MODE = "DEMO"
        url_now  = BASE_URL_DEMO
        warn = "\nℹ️ Bot kembali ke mode testnet — tidak ada uang asli yang digunakan."
        em = "🟢"

    # ── Reset balance_start ke saldo mode yang baru ──────────────────────────
    # PENTING: wajib reset agar check_drawdown() tidak false-trigger.
    # Saldo DEMO (testnet) berbeda jauh dengan LIVE (mainnet).
    # Jika tidak di-reset, drawdown 20% langsung terpenuhi saat switch mode.
    try:
        new_bal = init_balance()
        bal_note = f"\n💰 Balance ({BOT_MODE}): <b>{new_bal:.2f} USDT</b>\n⚠️ Drawdown baseline di-reset ke saldo ini."
    except Exception as e:
        bot_state["balance_start"] = 0   # reset manual agar tidak false-trigger
        bal_note = f"\n⚠️ Gagal fetch balance ({e}) — drawdown baseline di-reset."
        print(f"⚠️ Gagal reset balance saat ganti mode: {e}")

    # ── Reset daily limit state ke saldo mode baru ────────────────────────────
    # PENTING: balance_open HARUS di-reset saat ganti mode.
    # Jika tidak → PnL harian dihitung dari balance mode lama (DEMO vs LIVE beda jauh)
    # → limit langsung terpicu false-positive saat switch.
    try:
        now              = datetime.now(timezone.utc)
        today            = now.strftime("%Y-%m-%d")
        month_str        = now.strftime("%Y-%m")
        new_balance_open = get_total_balance()

        # ── Reset daily limit baseline ─────────────────────────────────────────
        _daily_limit_state["date"]         = today
        _daily_limit_state["balance_open"] = new_balance_open
        _daily_limit_state["paused_by"]    = None
        _daily_limit_state["auto_started"] = False

        # ── Reset Cumulative PNL snapshot (Hari ini & Bulan ini) ───────────────
        # PENTING: saldo DEMO vs LIVE beda jauh → jika tidak di-reset,
        # Cumulative PNL langsung terhitung minus/plus ekstrem saat switch mode.
        bot_state["balance_day_start"]   = new_balance_open
        bot_state["balance_day_date"]    = today
        bot_state["balance_month_start"] = new_balance_open
        bot_state["balance_month_key"]   = month_str

        daily_reset_note = (
            f"\n📅 Daily limit & Cumulative PNL baseline di-reset:\n"
            f"   Balance open ({BOT_MODE}): <b>{new_balance_open:.2f} USDT</b>\n"
            f"   PnL harian & bulanan dihitung ulang dari 0 untuk mode ini."
        )
        print(f"📅 Daily limit + Cumulative PNL di-reset saat switch mode → {BOT_MODE} | balance_open={new_balance_open:.2f}")
    except Exception as e:
        _daily_limit_state["balance_open"] = 0.0
        _daily_limit_state["paused_by"]    = None
        daily_reset_note = f"\n⚠️ Gagal reset daily baseline ({e})."
        print(f"⚠️ Gagal reset daily limit state saat ganti mode: {e}")

    send_telegram_raw(
        f"{em} <b>Mode Bot Diubah</b>\n"
        f"{'─'*34}\n"
        f"⚡ Mode Sekarang : <b>{BOT_MODE}</b>\n"
        f"🌐 Endpoint      : <code>{url_now}</code>\n"
        f"{'─'*34}"
        f"{warn}"
        f"{bal_note}"
        f"{daily_reset_note}"
    )
    print(f"⚙️ BOT_MODE diubah → {BOT_MODE} | URL: {url_now}")
    save_state()

    # Kalau switch ke LIVE → deteksi IP + ingatkan whitelist di Binance
    if BOT_MODE == "LIVE":
        def _notify_ip_for_live():
            print("\n🌐 Switch ke LIVE — mendeteksi IP publik...")
            public_ip = get_public_ip()
            now_str = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
            if public_ip:
                print(f"✅ IP terdeteksi: {public_ip}")
                msg = (
                    f"🌐 <b>IP Server — Cek Whitelist LIVE ({now_str})</b>\n"
                    f"{'─'*38}\n"
                    f"📡 IP Publik   : <code>{public_ip}</code>\n"
                    f"{'─'*38}\n"
                    f"⚠️ <b>Pastikan IP ini sudah di-whitelist</b> di API key mainnet Binance:\n"
                    f"  1. Buka <b>Binance → API Management</b>\n"
                    f"  2. Edit API key LIVE kamu\n"
                    f"  3. Tambahkan <code>{public_ip}</code> ke IP Restriction\n"
                    f"  4. Simpan perubahan\n"
                    f"{'─'*38}\n"
                    f"🔴 Bot sekarang aktif di mode <b>LIVE</b> — order masuk ke akun asli!"
                )
            else:
                print("❌ Gagal deteksi IP publik saat switch ke LIVE.")
                msg = (
                    f"⚠️ <b>Gagal Deteksi IP — Switch ke LIVE ({now_str})</b>\n"
                    f"{'─'*38}\n"
                    f"Tidak bisa mendeteksi IP publik server ini.\n"
                    f"🔧 Cek manual IP kamu dan whitelist di Binance API Management\n"
                    f"sebelum bot mulai order di mode LIVE!\n"
                    f"{'─'*38}\n"
                    f"🔴 Bot aktif di mode <b>LIVE</b> — pastikan IP sudah benar!"
                )
            send_telegram_raw(msg)
        threading.Thread(target=_notify_ip_for_live, daemon=True).start()


# ── Backtest State ────────────────────────────────────────────────────────────
_backtest_running = False


def cmd_backtest(parts: list):
    """
    /backtest <SYMBOL> <TIMEFRAME> [DAYS]
    Jalankan backtest SMC signal engine pada historical data pair tertentu.

    Contoh:
      /backtest BTCUSDT 1h 30      → backtest BTC 1H selama 30 hari
      /backtest ETHUSDT 15m 14     → backtest ETH 15m selama 14 hari
      /backtest SOLUSDT 4h         → backtest default 30 hari

    Cara kerja:
    - Ambil data historis via Binance klines (max 1500 candles)
    - Simulasikan logic analyze_pair window per window
    - Hitung winrate, avg RR, total sinyal dari historical data
    - Output ke Telegram
    """
    global _backtest_running

    if _backtest_running:
        send_telegram_raw("⏳ Backtest sedang berjalan... tunggu selesai dulu.")
        return

    # Parse args
    if len(parts) < 3:
        send_telegram_raw(
            "⚠️ Format: <code>/backtest &lt;SYMBOL&gt; &lt;TF&gt; [DAYS]</code>\n"
            "Contoh  : <code>/backtest BTCUSDT 1h 30</code>\n"
            "TF valid: 15m, 30m, 1h, 4h\n"
            "DAYS    : 7–90 (default 30)"
        )
        return

    symbol = parts[1].upper()
    tf     = parts[2].lower()
    days   = 30
    if len(parts) >= 4:
        try:
            days = int(parts[3])
            days = max(7, min(90, days))
        except ValueError:
            pass

    valid_tfs = {"15m", "30m", "1h", "4h", "1d"}
    if tf not in valid_tfs:
        send_telegram_raw(f"⚠️ Timeframe tidak valid. Gunakan: {', '.join(sorted(valid_tfs))}")
        return

    # Tentukan HTF passend berdasarkan entry TF
    htf_map = {"15m": "4h", "30m": "4h", "1h": "1d", "4h": "1d", "1d": "1d"}
    htf_tf  = htf_map.get(tf, "4h")

    send_telegram_raw(
        f"🔬 <b>Backtest Dimulai</b>\n"
        f"{'─'*34}\n"
        f"📌 Pair    : <b>{symbol}</b>\n"
        f"⏱ TF Entry: <b>{tf}</b>  |  HTF: <b>{htf_tf}</b>\n"
        f"📅 Periode : <b>{days} hari</b>\n"
        f"{'─'*34}\n"
        f"⏳ Mengambil data & mensimulasi... harap tunggu."
    )

    def _run_backtest():
        global _backtest_running
        _backtest_running = True
        try:
            candles_per_day = {"15m": 96, "30m": 48, "1h": 24, "4h": 6, "1d": 1}
            n_entry  = min(1500, days * candles_per_day.get(tf, 24) + 200)
            n_htf    = min(1500, days * candles_per_day.get(htf_tf, 6) + 200)

            df_entry = fetch_ohlcv_realdata(symbol, tf,     limit=n_entry)
            df_htf   = fetch_ohlcv_realdata(symbol, htf_tf, limit=n_htf)

            if df_entry is None or df_htf is None or len(df_entry) < 100:
                send_telegram_raw(f"❌ Backtest gagal: tidak bisa ambil data {symbol} @ {tf}")
                return

            window_size = 100
            scan_start  = max(window_size, len(df_entry) - days * candles_per_day.get(tf, 24))
            scan_start  = max(window_size, scan_start)

            signals_found = 0
            wins          = 0
            losses        = 0
            total_rr      = 0.0
            skipped_score = 0
            skipped_rr    = 0

            for i in range(scan_start, len(df_entry) - 1):
                slice_entry = df_entry.iloc[:i + 1].copy().reset_index(drop=True)
                ts_now    = slice_entry["timestamp"].iloc[-1]
                slice_htf = df_htf[df_htf["timestamp"] <= ts_now].copy().reset_index(drop=True)

                if len(slice_htf) < 60:
                    continue

                # HTF Trend (PA method)
                htf_bias = detect_htf_trend(slice_htf)
                if htf_bias == "RANGING":
                    continue

                trade_direction = htf_bias
                price = float(slice_entry["close"].iloc[-1])

                # Price Action Pattern
                _, pa_pts = detect_price_action_pattern(slice_entry, trade_direction)

                # Supply & Demand
                sd_zones = find_supply_demand_zones(slice_entry, trade_direction)
                in_sd, best_sd = price_in_sd_zone(price, sd_zones)
                sd_touches = best_sd["touches"] if best_sd else 0

                # Support & Resistance
                sr_levels = find_sr_levels(slice_entry)
                sr_near, sr_touches, _ = price_near_sr(price, sr_levels, trade_direction)

                vol_rat = volume_ratio(slice_entry)

                score, _ = compute_score(
                    htf_aligned  = True,
                    pa_score     = pa_pts,
                    sd_in_zone   = in_sd,
                    sd_touches   = sd_touches,
                    sr_near      = sr_near,
                    sr_touches   = sr_touches,
                    vol_rat      = vol_rat,
                    session      = "London",   # netral untuk backtest
                    macro_pts    = 0,
                )

                if score < MIN_SCORE:
                    skipped_score += 1
                    continue

                # S/R level price untuk SL
                sr_price_val = None
                if sr_near and sr_levels:
                    proximity_sorted = sorted(sr_levels, key=lambda x: abs(x["price"] - price))
                    sr_price_val = proximity_sorted[0]["price"] if proximity_sorted else None

                entry, sl, tp1, tp2, rr1, rr2 = calculate_rr(
                    df        = slice_entry,
                    direction = trade_direction,
                    sd_zone   = best_sd if in_sd else None,
                    sr_level  = sr_price_val,
                )

                if entry <= 0 or sl <= 0 or rr1 < RR_MIN:
                    skipped_rr += 1
                    continue

                signals_found += 1
                total_rr += rr1

                # Simulasi outcome: 1 candle ke depan
                next_c    = df_entry.iloc[i + 1]
                next_high = float(next_c["high"])
                next_low  = float(next_c["low"])

                if trade_direction == "BULLISH":
                    if next_high >= tp1:
                        wins += 1
                    elif next_low <= sl:
                        losses += 1
                else:
                    if next_low <= tp1:
                        wins += 1
                    elif next_high >= sl:
                        losses += 1

            closed  = wins + losses
            winrate = (wins / closed * 100) if closed > 0 else 0.0
            avg_rr  = (total_rr / signals_found) if signals_found > 0 else 0.0
            sim_pnl = (wins * avg_rr) - losses

            wr_bar_n = min(10, int(winrate / 10))
            wr_bar   = "█" * wr_bar_n + "░" * (10 - wr_bar_n)
            wr_em    = "🟢" if winrate >= 55 else ("🟡" if winrate >= 45 else "🔴")

            now_str = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
            send_telegram_raw(
                f"📊 <b>HASIL BACKTEST — {symbol} {tf.upper()}</b>\n"
                f"{'─'*38}\n"
                f"🔍 Method     : <b>PA + Supply&Demand + S/R</b>\n"
                f"📅 Periode    : <b>{days} hari</b>  |  {now_str}\n"
                f"⏱ Entry TF   : <b>{tf}</b>  |  HTF: <b>{htf_tf}</b>\n"
                f"{'─'*38}\n"
                f"🔔 Total Sinyal   : <b>{signals_found}</b>\n"
                f"✅ Win            : <b>{wins}</b>\n"
                f"❌ Loss           : <b>{losses}</b>\n"
                f"❓ Belum close    : <b>{signals_found - closed}</b>\n"
                f"{'─'*38}\n"
                f"{wr_em} Winrate      : <b>{winrate:.1f}%</b>\n"
                f"  [{wr_bar}]\n"
                f"📐 Avg RR         : <b>1:{avg_rr:.2f}</b>\n"
                f"💹 Sim. PnL       : <b>{'+'if sim_pnl>=0 else ''}{sim_pnl:.1f}R</b>\n"
                f"{'─'*38}\n"
                f"⏭ Skip (score<{MIN_SCORE}): {skipped_score}\n"
                f"⏭ Skip (RR<{RR_MIN})  : {skipped_rr}\n"
                f"{'─'*38}\n"
                f"ℹ️ Simulasi 1-candle forward. Hasil bukan jaminan profit."
            )
            print(f"✅ Backtest selesai: {symbol} {tf} | {signals_found} sinyal | WR:{winrate:.1f}% | RR:{avg_rr:.2f}")

        except Exception as e:
            send_telegram_raw(f"❌ Backtest error: <code>{e}</code>")
            print(f"❌ Backtest error: {e}")
        finally:
            _backtest_running = False

    # Jalankan di thread terpisah agar tidak blocking main loop
    bt_thread = threading.Thread(target=_run_backtest, daemon=True)
    bt_thread.start()


def cmd_scalpingonly():
    """
    /scalpingonly
    Aktifkan hanya mode LOW_TF + LTF_30M (scalping sejati, entry 15m/30m).
    Mode INTRADAY dan SCALPING (4h→1h) dinonaktifkan.
    """
    global _ACTIVE_MODE_FILTER
    _ACTIVE_MODE_FILTER = "SCALPING"
    active_labels = [m["label"] for m in get_active_modes()]
    send_telegram_raw(
        f"⚡ <b>Mode: SCALPING ONLY</b>\n"
        f"{'─'*34}\n"
        f"✅ Mode aktif: <b>{', '.join(active_labels)}</b>\n"
        f"❌ INTRADAY + SCALPING (4h→1h) dinonaktifkan\n"
        f"{'─'*34}\n"
        f"Bot fokus sinyal cepat entry 15m/30m.\n"
        f"Gunakan /allmode untuk kembali ke semua mode."
    )
    print(f"⚙️ Mode filter: SCALPING ONLY ({active_labels})")
    save_state()


def cmd_intradayonly():
    """
    /intradayonly
    Aktifkan mode INTRADAY + SCALPING (4h→1h).
    Mode LOW_TF (15m) dan LTF_30M (30m) dinonaktifkan.
    """
    global _ACTIVE_MODE_FILTER
    _ACTIVE_MODE_FILTER = "INTRADAY"
    active_labels = [m["label"] for m in get_active_modes()]
    send_telegram_raw(
        f"📈 <b>Mode: INTRADAY ONLY</b>\n"
        f"{'─'*34}\n"
        f"✅ Mode aktif: <b>{', '.join(active_labels)}</b>\n"
        f"❌ LOW_TF / LTF_30M (15m/30m) dinonaktifkan\n"
        f"{'─'*34}\n"
        f"Bot fokus sinyal intraday (1d→4h→1h).\n"
        f"Gunakan /allmode untuk kembali ke semua mode."
    )
    print(f"⚙️ Mode filter: INTRADAY ONLY ({active_labels})")
    save_state()


def cmd_longonly():
    """
    /longonly
    Hanya proses sinyal BULLISH (LONG). Sinyal SHORT/BEARISH dilewati.
    Reset dengan /resetdirection.
    """
    global _DIRECTION_FILTER
    _DIRECTION_FILTER = "LONG"
    send_telegram_raw(
        f"🟢 <b>Direction Filter: LONG ONLY</b>\n"
        f"{'─'*34}\n"
        f"✅ Hanya sinyal <b>LONG (BULLISH)</b> yang akan diproses\n"
        f"❌ Sinyal SHORT dilewati\n"
        f"{'─'*34}\n"
        f"Gunakan /resetdirection untuk kembali ke semua arah."
    )
    print(f"⚙️ Direction filter: LONG ONLY")
    save_state()


def cmd_shortonly():
    """
    /shortonly
    Hanya proses sinyal BEARISH (SHORT). Sinyal LONG/BULLISH dilewati.
    Reset dengan /resetdirection.
    """
    global _DIRECTION_FILTER
    _DIRECTION_FILTER = "SHORT"
    send_telegram_raw(
        f"🔴 <b>Direction Filter: SHORT ONLY</b>\n"
        f"{'─'*34}\n"
        f"✅ Hanya sinyal <b>SHORT (BEARISH)</b> yang akan diproses\n"
        f"❌ Sinyal LONG dilewati\n"
        f"{'─'*34}\n"
        f"Gunakan /resetdirection untuk kembali ke semua arah."
    )
    print(f"⚙️ Direction filter: SHORT ONLY")
    save_state()


def cmd_resetdirection():
    """
    /resetdirection
    Reset direction filter — bot kembali proses semua arah (LONG + SHORT).
    """
    global _DIRECTION_FILTER
    _DIRECTION_FILTER = "ALL"
    send_telegram_raw(
        f"🔄 <b>Direction Filter: ALL (Default)</b>\n"
        f"{'─'*34}\n"
        f"✅ Bot kembali memproses sinyal <b>LONG & SHORT</b>\n"
        f"{'─'*34}\n"
        f"Filter arah dinonaktifkan."
    )
    print(f"⚙️ Direction filter: ALL (reset)")
    save_state()


def cmd_allmode():
    """
    /allmode
    Aktifkan kembali semua mode scan (default).
    """
    global _ACTIVE_MODE_FILTER
    _ACTIVE_MODE_FILTER = "ALL"
    active_labels = [m["label"] for m in get_active_modes()]
    send_telegram_raw(
        f"🔄 <b>Mode: ALL (Default)</b>\n"
        f"{'─'*34}\n"
        f"✅ Semua mode aktif: <b>{', '.join(active_labels)}</b>\n"
        f"{'─'*34}\n"
        f"Bot scan semua timeframe (INTRADAY + SCALPING + LOW_TF + LTF_30M)."
    )
    print(f"⚙️ Mode filter: ALL ({active_labels})")
    save_state()


def cmd_superscalpermode():
    """
    /superscalpermode
    Toggle Super Scalper Mode ON / OFF.

    Ketika ON — konfigurasi SUPER AGRESIF untuk momentum scalping:
      • Hanya pair HIGH-LIQUIDITY (25 coin terlikuid, volume besar)
      • Hanya mode LOW_TF (15m) + LTF_30M (30m)
      • Score threshold turun ke 35 (sinyal lebih banyak & sering)
      • RR minimum turun ke 1.2 (exit cepat, profit kecil tapi sering)
      • BTC correlation filter di-OFF (jangan block sinyal momentum)
      • Cooldown setelah SL dipersingkat ke 1 jam
      • TP1 fixed 0.8% dari entry (langsung ambil profit kecil)
      • Setelah TP1 hit → sisa lot tetap jalan ke TP2

    Semua setting lama di-restore otomatis saat /superscalpermode OFF.
    """
    global _SUPER_SCALPER_MODE, _PRE_SUPERSCALPER_SNAPSHOT
    global MIN_SCORE_CUSTOM, MIN_SCORE_RELAXED_CUSTOM
    global BTC_CORR_FILTER_ON, COOLDOWN_AFTER_SL_HOURS
    global TP1_PROFIT_PCT, _ACTIVE_MODE_FILTER

    if not _SUPER_SCALPER_MODE:
        # ── Aktifkan Super Scalper Mode ───────────────────────────────────────
        # Snapshot settings saat ini sebelum diubah
        _PRE_SUPERSCALPER_SNAPSHOT = {
            "MIN_SCORE_CUSTOM":          MIN_SCORE_CUSTOM,
            "MIN_SCORE_RELAXED_CUSTOM":  MIN_SCORE_RELAXED_CUSTOM,
            "BTC_CORR_FILTER_ON":        BTC_CORR_FILTER_ON,
            "COOLDOWN_AFTER_SL_HOURS":   COOLDOWN_AFTER_SL_HOURS,
            "TP1_PROFIT_PCT":            TP1_PROFIT_PCT,
            "_ACTIVE_MODE_FILTER":       _ACTIVE_MODE_FILTER,
        }

        # Terapkan config super scalper
        _SUPER_SCALPER_MODE       = True
        MIN_SCORE_CUSTOM          = 45    # 45 bukan 35 — di 15m/30m noise tinggi, jangan terlalu rendah
        MIN_SCORE_RELAXED_CUSTOM  = 45    # samakan RELAXED agar tidak ada gap
        BTC_CORR_FILTER_ON        = False # jangan block sinyal momentum
        COOLDOWN_AFTER_SL_HOURS   = 1    # cooldown SL hanya 1 jam
        TP1_PROFIT_PCT            = 0.6  # TP1 0.6% — realistis untuk 15m/30m dengan SL 1.2%
        _ACTIVE_MODE_FILTER       = "SCALPING"  # dipaksa di get_active_modes() via _SUPER_SCALPER_MODE

        pair_count = len(_SUPER_SCALPER_PAIRS)
        send_telegram_raw(
            f"⚡⚡ <b>SUPER SCALPER MODE — ON</b> ⚡⚡\n"
            f"{'═'*36}\n"
            f"🎯 <b>Target: Profit TP → Profit TP → berulang</b>\n"
            f"{'─'*36}\n"
            f"🪙 Pair aktif     : <b>{pair_count} coin high-liquidity</b>\n"
            f"⏱ Mode scan      : <b>LOW_TF (15m) + LTF_30M (30m)</b>\n"
            f"📊 Min score      : <b>45 pts</b> (dari {_PRE_SUPERSCALPER_SNAPSHOT['MIN_SCORE_CUSTOM']})\n"
            f"📐 Min RR         : <b>1.8</b> (SL ketat, TP realistis)\n"
            f"🛑 Max SL         : <b>1.2% (15m) / 1.5% (30m)</b>\n"
            f"🎯 TP1 fixed      : <b>0.6%</b> dari entry (ambil cepat)\n"
            f"🔥 BTC filter     : <b>OFF</b> (jangan block momentum)\n"
            f"⏳ Cooldown SL    : <b>1 jam</b> (dari {int(_PRE_SUPERSCALPER_SNAPSHOT['COOLDOWN_AFTER_SL_HOURS'])}j)\n"
            f"{'─'*36}\n"
            f"🏆 <b>20 Pair (Binance Futures OI + Volume terbesar):</b>\n"
            f"BTC ETH SOL XRP BNB | DOGE ADA AVAX LINK LTC\n"
            f"DOT UNI AAVE SUI PEPE | TRX XLM INJ ENA SHIB\n"
            f"{'─'*36}\n"
            f"⚠️ Mode AGRESIF — SL ketat, pantau posisi!\n"
            f"💡 Rekomen: /maxdailyloss 3 sebelum aktif\n"
            f"Gunakan /superscalpermode lagi untuk OFF + restore setting."
        )
        print("⚡ SUPER SCALPER MODE: ON | pairs=20 | score=45 | RR=1.8 | SL≤1.2% | TP1=0.6% | cd_SL=1h")

    else:
        # ── Matikan Super Scalper Mode & restore setting lama ─────────────────
        _SUPER_SCALPER_MODE = False

        snap = _PRE_SUPERSCALPER_SNAPSHOT
        if snap:
            MIN_SCORE_CUSTOM          = snap.get("MIN_SCORE_CUSTOM",         MIN_SCORE_CUSTOM)
            MIN_SCORE_RELAXED_CUSTOM  = snap.get("MIN_SCORE_RELAXED_CUSTOM", MIN_SCORE_RELAXED_CUSTOM)
            BTC_CORR_FILTER_ON        = snap.get("BTC_CORR_FILTER_ON",       BTC_CORR_FILTER_ON)
            COOLDOWN_AFTER_SL_HOURS   = snap.get("COOLDOWN_AFTER_SL_HOURS",  COOLDOWN_AFTER_SL_HOURS)
            TP1_PROFIT_PCT            = snap.get("TP1_PROFIT_PCT",           TP1_PROFIT_PCT)
            _ACTIVE_MODE_FILTER       = snap.get("_ACTIVE_MODE_FILTER",      _ACTIVE_MODE_FILTER)
            _PRE_SUPERSCALPER_SNAPSHOT = {}
            restore_note = "✅ Setting lama berhasil di-restore."
        else:
            restore_note = "ℹ️ Tidak ada snapshot — setting tetap seperti saat ini."

        send_telegram_raw(
            f"⏹ <b>SUPER SCALPER MODE — OFF</b>\n"
            f"{'─'*34}\n"
            f"{restore_note}\n"
            f"{'─'*34}\n"
            f"📊 Min score      : <b>{MIN_SCORE_CUSTOM} pts</b>\n"
            f"🔥 BTC filter     : <b>{'ON' if BTC_CORR_FILTER_ON else 'OFF'}</b>\n"
            f"⏳ Cooldown SL    : <b>{COOLDOWN_AFTER_SL_HOURS} jam</b>\n"
            f"🎯 TP1            : <b>{'otomatis (struktur)' if TP1_PROFIT_PCT == 0.0 else f'{TP1_PROFIT_PCT:.1f}%'}</b>\n"
            f"🪙 Pair aktif     : <b>FULL LIST ({len(PAIR_LIST)} pairs)</b>\n"
            f"{'─'*34}\n"
            f"Bot kembali ke mode normal. Gunakan /allmode jika perlu."
        )
        print("⏹ SUPER SCALPER MODE: OFF | setting lama di-restore")

    save_state()


def cmd_settp1profit(parts: list):
    """
    /settp1profit <persen>
    Set target profit TP1 sebagai persentase fixed dari entry price.

    Contoh:
      /settp1profit 1.5  → TP1 = entry +1.5% (LONG) atau -1.5% (SHORT)
      /settp1profit 0.8  → TP1 = entry +0.8% / -0.8%
      /settp1profit 0    → reset ke TP1 otomatis (dari struktur market)

    Berlaku untuk semua trade BARU setelah command ini dikirim.
    Posisi yang sudah terbuka TIDAK terpengaruh.
    """
    global TP1_PROFIT_PCT
    if len(parts) < 2:
        send_telegram_raw(
            "⚠️ <b>Format salah.</b>\n"
            "Gunakan: <code>/settp1profit &lt;persen&gt;</code>\n"
            "Contoh : <code>/settp1profit 1.5</code> → TP1 di +1.5% dari entry\n"
            "Reset  : <code>/settp1profit 0</code>   → kembali ke TP1 otomatis"
        )
        return
    try:
        pct = float(parts[1])
    except ValueError:
        send_telegram_raw("❌ Nilai tidak valid. Masukkan angka, contoh: <code>/settp1profit 1.5</code>")
        return
    if pct < 0:
        send_telegram_raw("❌ Persentase tidak boleh negatif. Masukkan angka ≥ 0.")
        return
    if pct > 20:
        send_telegram_raw("⚠️ Terlalu besar (> 20%). Pastikan nilai benar sebelum set.")
        return

    TP1_PROFIT_PCT = pct
    if pct == 0.0:
        send_telegram_raw(
            "🔄 <b>TP1 direset ke mode otomatis</b>\n"
            "TP1 akan dihitung dari struktur market (swing, S&D, S/R).\n"
            "Berlaku untuk trade baru."
        )
        print("⚙️ TP1_PROFIT_PCT direset ke 0.0 (otomatis)")
        save_state()
    else:
        send_telegram_raw(
            f"🎯 <b>TP1 Fixed Profit: {pct:.2f}%</b>\n"
            f"{'─'*34}\n"
            f"Setiap trade baru:\n"
            f"  LONG  → TP1 = entry × (1 + {pct:.2f}%)\n"
            f"  SHORT → TP1 = entry × (1 - {pct:.2f}%)\n"
            f"{'─'*34}\n"
            f"Posisi yang sudah terbuka <b>tidak berubah</b>.\n"
            f"Gunakan /resettp1 untuk kembali ke TP1 otomatis."
        )
        print(f"⚙️ TP1_PROFIT_PCT = {pct:.2f}%")
        save_state()


def cmd_resettp1():
    """
    /resettp1
    Reset TP1 kembali ke mode otomatis (dari struktur market).
    Alias dari /settp1profit 0.
    """
    global TP1_PROFIT_PCT
    TP1_PROFIT_PCT = 0.0
    send_telegram_raw(
        "🔄 <b>TP1 direset ke mode otomatis</b>\n"
        "TP1 akan dihitung dari struktur market (swing high/low, S&D zone, S/R level).\n"
        "Berlaku untuk trade baru."
    )
    print("⚙️ TP1_PROFIT_PCT direset ke 0.0 (otomatis)")
    save_state()


def cmd_settp1partial(parts: list):
    """
    /settp1partial <persen>
    Set berapa persen lot yang di-close saat TP1 tercapai.
    Sisa lot akan lanjut ke TP2 (close semua).

    Contoh:
      /settp1partial 25  → TP1 close 25% lot, TP2 close 75% sisa
      /settp1partial 50  → TP1 close 50% lot, TP2 close 50% sisa (default)
      /settp1partial 75  → TP1 close 75% lot, TP2 close 25% sisa
    """
    global TP1_PARTIAL
    if len(parts) < 2:
        send_telegram_raw(
            "⚠️ <b>Format salah.</b>\n"
            "Gunakan: <code>/settp1partial &lt;persen&gt;</code>\n"
            "Contoh : <code>/settp1partial 25</code> → TP1 close 25% lot\n"
            f"Saat ini: <b>{int(TP1_PARTIAL * 100)}%</b> lot di-close di TP1"
        )
        return
    try:
        pct = float(parts[1])
    except ValueError:
        send_telegram_raw("❌ Nilai tidak valid. Masukkan angka 1–99, contoh: <code>/settp1partial 25</code>")
        return
    if pct <= 0 or pct >= 100:
        send_telegram_raw("❌ Nilai harus antara 1–99 (persen).")
        return

    TP1_PARTIAL = pct / 100
    tp2_pct = 100 - pct
    send_telegram_raw(
        f"✅ <b>TP1 Partial diubah: {pct:.0f}%</b>\n"
        f"{'─'*34}\n"
        f"🎯 TP1 → close <b>{pct:.0f}%</b> lot\n"
        f"🎯 TP2 → close <b>{tp2_pct:.0f}%</b> sisa lot (semua)\n"
        f"{'─'*34}\n"
        f"Berlaku untuk trade <b>baru</b>. Posisi aktif tidak berubah."
    )
    print(f"⚙️ TP1_PARTIAL = {TP1_PARTIAL} ({pct:.0f}% lot di-close di TP1)")
    save_state()


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 20c — DAILY LOSS / WIN LIMIT
# ═══════════════════════════════════════════════════════════════════════════

def _get_daily_pnl_pct() -> tuple[float, float, float]:
    """
    Hitung daily PnL % dari balance awal hari ini (UTC 00:00).
    Return: (pnl_usdt, pnl_pct, balance_open)
      pnl_usdt    = PnL hari ini dalam USDT
      pnl_pct     = PnL hari ini dalam persen dari balance_open
      balance_open= Balance awal hari ini
    """
    try:
        current = get_total_balance()
    except Exception:
        return 0.0, 0.0, 0.0

    balance_open = _daily_limit_state.get("balance_open", 0.0)
    if balance_open <= 0:
        return 0.0, 0.0, 0.0

    pnl_usdt = current - balance_open
    pnl_pct  = (pnl_usdt / balance_open) * 100.0
    return pnl_usdt, pnl_pct, balance_open


def _reset_daily_limit_for_new_day():
    """
    Reset state daily limit untuk hari baru (UTC 00:00).
    Dipanggil otomatis di awal setiap hari baru.
    Auto-start bot hanya jika hari sebelumnya kena limit (DAILY_LOSS atau DAILY_WIN).
    """
    global bot_paused, _daily_limit_state

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _daily_limit_state["date"] == today:
        return   # sudah di-reset untuk hari ini

    paused_by_prev = _daily_limit_state.get("paused_by")

    # Snapshot balance awal hari baru
    try:
        new_balance_open = get_total_balance()
    except Exception:
        new_balance_open = _daily_limit_state.get("balance_open", 0.0)

    _daily_limit_state["date"]         = today
    _daily_limit_state["balance_open"] = new_balance_open
    _daily_limit_state["paused_by"]    = None
    _daily_limit_state["auto_started"] = False

    print(f"📅 Daily limit reset — {today} | Balance open: {new_balance_open:.2f} USDT")

    # ── Auto-start hanya jika kemarin kena limit (BUKAN pause manual) ────────
    if paused_by_prev in ("DAILY_LOSS", "DAILY_WIN") and bot_paused:
        bot_paused = False
        _daily_limit_state["auto_started"] = True
        limit_label = "Daily Loss" if paused_by_prev == "DAILY_LOSS" else "Daily Win"
        print(f"▶️ Bot auto-start: hari baru, limit {limit_label} kemarin sudah direset")
        send_telegram_raw(
            f"☀️ <b>Hari Baru — Bot Auto-Start</b>\n"
            f"{'─'*34}\n"
            f"📅 Tanggal     : <b>{today}</b>\n"
            f"💰 Balance baru: <b>{new_balance_open:.2f} USDT</b>\n"
            f"🔄 Kemarin kena <b>{limit_label} Limit</b> → otomatis di-resume.\n"
            f"{'─'*34}\n"
            f"{'🛑 Max Daily Loss: ' + str(DAILY_LOSS_LIMIT_PCT) + '%' if DAILY_LOSS_LIMIT_PCT > 0 else ''}\n"
            f"{'🎯 Max Daily Win : ' + str(DAILY_WIN_LIMIT_PCT) + '%' if DAILY_WIN_LIMIT_PCT > 0 else ''}\n"
            f"Bot scanning & trading aktif kembali!"
        )
    else:
        # Hari baru, bot tidak auto-start (pause manual atau limit tidak aktif)
        send_telegram_raw(
            f"☀️ <b>Hari Baru — Daily Limit Reset</b>\n"
            f"{'─'*34}\n"
            f"📅 Tanggal     : <b>{today}</b>\n"
            f"💰 Balance open: <b>{new_balance_open:.2f} USDT</b>\n"
            f"{'🛑 Max Daily Loss: ' + str(DAILY_LOSS_LIMIT_PCT) + '%' if DAILY_LOSS_LIMIT_PCT > 0 else '⭕ Daily Loss : OFF'}\n"
            f"{'🎯 Max Daily Win : ' + str(DAILY_WIN_LIMIT_PCT) + '%' if DAILY_WIN_LIMIT_PCT > 0 else '⭕ Daily Win  : OFF'}\n"
            f"{'─'*34}\n"
            f"{'▶️ Bot aktif.' if not bot_paused else '⏸ Bot masih PAUSE — kirim /start untuk mulai.'}"
        )


def check_daily_limits():
    """
    Cek apakah daily loss atau daily win limit sudah tercapai.
    Jika ya → pause bot otomatis dan kirim notifikasi.
    Dipanggil di main loop setiap iterasi.
    """
    global bot_paused, _daily_limit_state

    # Cek & reset hari baru dulu
    _reset_daily_limit_for_new_day()

    # Jika tidak ada limit yang aktif → tidak perlu cek
    if DAILY_LOSS_LIMIT_PCT <= 0 and DAILY_WIN_LIMIT_PCT <= 0:
        return

    # Jika bot sudah di-pause oleh limit hari ini → tidak perlu cek lagi
    if _daily_limit_state.get("paused_by") in ("DAILY_LOSS", "DAILY_WIN"):
        return

    pnl_usdt, pnl_pct, balance_open = _get_daily_pnl_pct()

    # ── Cek Daily Loss ────────────────────────────────────────────────────────
    if DAILY_LOSS_LIMIT_PCT > 0 and pnl_pct <= -DAILY_LOSS_LIMIT_PCT:
        bot_paused = True
        _daily_limit_state["paused_by"] = "DAILY_LOSS"
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        msg = (
            f"🛑 <b>DAILY LOSS LIMIT TERCAPAI — Bot Di-PAUSE</b>\n"
            f"{'─'*38}\n"
            f"📅 Tanggal       : <b>{today}</b>\n"
            f"💰 Balance Open  : <b>{balance_open:.2f} USDT</b>\n"
            f"📉 PnL Hari Ini  : <b>{pnl_usdt:+.2f} USDT ({pnl_pct:+.2f}%)</b>\n"
            f"🛑 Limit Loss    : <b>-{DAILY_LOSS_LIMIT_PCT:.1f}%</b>\n"
            f"{'─'*38}\n"
            f"⏸ <b>Bot otomatis PAUSE sampai besok (UTC 00:00).</b>\n"
            f"🔄 Bot akan auto-start besok pagi.\n"
            f"ℹ️ Gunakan /resumeorpause jika ingin resume manual hari ini."
        )
        print(f"🛑 DAILY LOSS LIMIT: {pnl_pct:.2f}% ≤ -{DAILY_LOSS_LIMIT_PCT}% → bot PAUSED")
        send_telegram_raw(msg)
        return

    # ── Cek Daily Win ─────────────────────────────────────────────────────────
    if DAILY_WIN_LIMIT_PCT > 0 and pnl_pct >= DAILY_WIN_LIMIT_PCT:
        bot_paused = True
        _daily_limit_state["paused_by"] = "DAILY_WIN"
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        msg = (
            f"🎯 <b>DAILY WIN LIMIT TERCAPAI — Bot Di-PAUSE</b>\n"
            f"{'─'*38}\n"
            f"📅 Tanggal       : <b>{today}</b>\n"
            f"💰 Balance Open  : <b>{balance_open:.2f} USDT</b>\n"
            f"📈 PnL Hari Ini  : <b>{pnl_usdt:+.2f} USDT ({pnl_pct:+.2f}%)</b>\n"
            f"🎯 Limit Win     : <b>+{DAILY_WIN_LIMIT_PCT:.1f}%</b>\n"
            f"{'─'*38}\n"
            f"⏸ <b>Bot otomatis PAUSE sampai besok (UTC 00:00).</b>\n"
            f"🔄 Bot akan auto-start besok pagi.\n"
            f"ℹ️ Gunakan /resumeorpause jika ingin resume manual hari ini."
        )
        print(f"🎯 DAILY WIN LIMIT: {pnl_pct:.2f}% ≥ +{DAILY_WIN_LIMIT_PCT}% → bot PAUSED")
        send_telegram_raw(msg)
        return


def cmd_maxdailyloss(parts: list):
    """
    /maxdailyloss <persen>
    Set batas maksimal loss harian sebagai % dari total portfolio.
    Jika PnL hari ini mencapai -X% → bot otomatis pause sampai besok UTC 00:00.

    Contoh:
      /maxdailyloss 5    → pause jika loss ≥ 5% portfolio hari ini
      /maxdailyloss 2.5  → pause jika loss ≥ 2.5% portfolio hari ini
      /maxdailyloss 0    → nonaktifkan limit (bot tidak pernah pause karena loss)

    Range: 0.1% – 50% (0 = off)
    """
    global DAILY_LOSS_LIMIT_PCT
    if len(parts) < 2:
        status_str = f"<b>{DAILY_LOSS_LIMIT_PCT:.1f}%</b>" if DAILY_LOSS_LIMIT_PCT > 0 else "<b>OFF</b> (tidak aktif)"
        try:
            _, pnl_pct, balance_open = _get_daily_pnl_pct()
            pnl_note = f"\n📉 Daily PnL saat ini: <b>{pnl_pct:+.2f}%</b> (dari {balance_open:.2f} USDT)"
        except Exception:
            pnl_note = ""
        send_telegram_raw(
            f"⚠️ <b>Format salah.</b>\n"
            f"Gunakan: <code>/maxdailyloss 5</code>\n"
            f"Contoh : <code>/maxdailyloss 5</code>  → pause jika loss ≥ 5%\n"
            f"         <code>/maxdailyloss 0</code>  → nonaktifkan limit\n"
            f"{'─'*34}\n"
            f"Status saat ini: {status_str}{pnl_note}"
        )
        return

    try:
        new_pct = float(parts[1].replace(",", "."))
    except ValueError:
        send_telegram_raw("❌ Masukkan angka yang valid. Contoh: <code>/maxdailyloss 5</code>")
        return

    if new_pct < 0:
        send_telegram_raw("❌ Nilai tidak boleh negatif. Gunakan 0 untuk nonaktifkan.")
        return
    if new_pct > 50:
        send_telegram_raw("⚠️ Nilai terlalu besar (max 50%). Cek lagi input kamu.")
        return

    old_pct = DAILY_LOSS_LIMIT_PCT
    DAILY_LOSS_LIMIT_PCT = new_pct

    # Reset paused_by jika sebelumnya kena DAILY_LOSS dan sekarang limit diubah/dimatikan
    if _daily_limit_state.get("paused_by") == "DAILY_LOSS" and new_pct == 0.0:
        _daily_limit_state["paused_by"] = None

    # Snapshot balance open hari ini jika belum ada
    if _daily_limit_state.get("balance_open", 0.0) == 0.0 or _daily_limit_state.get("date") != datetime.now(timezone.utc).strftime("%Y-%m-%d"):
        _reset_daily_limit_for_new_day()

    try:
        _, pnl_pct, balance_open = _get_daily_pnl_pct()
        pnl_note = f"\n📉 Daily PnL saat ini  : <b>{pnl_pct:+.2f}%</b> (dari {balance_open:.2f} USDT)"
    except Exception:
        pnl_note = ""

    if new_pct == 0.0:
        send_telegram_raw(
            f"⭕ <b>Max Daily Loss — DINONAKTIFKAN</b>\n"
            f"{'─'*34}\n"
            f"Bot tidak akan auto-pause karena loss harian.{pnl_note}"
        )
    else:
        old_str = f"{old_pct:.1f}%" if old_pct > 0 else "OFF"
        send_telegram_raw(
            f"🛑 <b>Max Daily Loss Diset: {new_pct:.1f}%</b>\n"
            f"{'─'*34}\n"
            f"📉 Sebelum : <b>{old_str}</b>\n"
            f"📉 Sekarang: <b>{new_pct:.1f}%</b> dari portfolio\n"
            f"{'─'*34}\n"
            f"Jika PnL harian ≤ <b>-{new_pct:.1f}%</b> → bot auto-pause.\n"
            f"Bot akan auto-start besok UTC 00:00.{pnl_note}\n"
            f"{'─'*34}\n"
            f"ℹ️ Gunakan /maxdailyloss 0 untuk nonaktifkan."
        )
    print(f"⚙️ DAILY_LOSS_LIMIT_PCT: {old_pct:.1f}% → {new_pct:.1f}%")
    save_state()


def cmd_maxdailywin(parts: list):
    """
    /maxdailywin <persen>
    Set batas maksimal win harian sebagai % dari total portfolio.
    Jika PnL hari ini mencapai +X% → bot otomatis pause sampai besok UTC 00:00.

    Contoh:
      /maxdailywin 10   → pause jika profit ≥ 10% portfolio hari ini
      /maxdailywin 3.5  → pause jika profit ≥ 3.5% portfolio hari ini
      /maxdailywin 0    → nonaktifkan limit (bot tidak pernah pause karena win)

    Range: 0.1% – 100% (0 = off)
    """
    global DAILY_WIN_LIMIT_PCT
    if len(parts) < 2:
        status_str = f"<b>{DAILY_WIN_LIMIT_PCT:.1f}%</b>" if DAILY_WIN_LIMIT_PCT > 0 else "<b>OFF</b> (tidak aktif)"
        try:
            _, pnl_pct, balance_open = _get_daily_pnl_pct()
            pnl_note = f"\n📈 Daily PnL saat ini: <b>{pnl_pct:+.2f}%</b> (dari {balance_open:.2f} USDT)"
        except Exception:
            pnl_note = ""
        send_telegram_raw(
            f"⚠️ <b>Format salah.</b>\n"
            f"Gunakan: <code>/maxdailywin 10</code>\n"
            f"Contoh : <code>/maxdailywin 10</code> → pause jika profit ≥ 10%\n"
            f"         <code>/maxdailywin 0</code>  → nonaktifkan limit\n"
            f"{'─'*34}\n"
            f"Status saat ini: {status_str}{pnl_note}"
        )
        return

    try:
        new_pct = float(parts[1].replace(",", "."))
    except ValueError:
        send_telegram_raw("❌ Masukkan angka yang valid. Contoh: <code>/maxdailywin 10</code>")
        return

    if new_pct < 0:
        send_telegram_raw("❌ Nilai tidak boleh negatif. Gunakan 0 untuk nonaktifkan.")
        return
    if new_pct > 100:
        send_telegram_raw("⚠️ Nilai terlalu besar (max 100%). Cek lagi input kamu.")
        return

    old_pct = DAILY_WIN_LIMIT_PCT
    DAILY_WIN_LIMIT_PCT = new_pct

    # Reset paused_by jika sebelumnya kena DAILY_WIN dan sekarang limit diubah/dimatikan
    if _daily_limit_state.get("paused_by") == "DAILY_WIN" and new_pct == 0.0:
        _daily_limit_state["paused_by"] = None

    # Snapshot balance open hari ini jika belum ada
    if _daily_limit_state.get("balance_open", 0.0) == 0.0 or _daily_limit_state.get("date") != datetime.now(timezone.utc).strftime("%Y-%m-%d"):
        _reset_daily_limit_for_new_day()

    try:
        _, pnl_pct, balance_open = _get_daily_pnl_pct()
        pnl_note = f"\n📈 Daily PnL saat ini  : <b>{pnl_pct:+.2f}%</b> (dari {balance_open:.2f} USDT)"
    except Exception:
        pnl_note = ""

    if new_pct == 0.0:
        send_telegram_raw(
            f"⭕ <b>Max Daily Win — DINONAKTIFKAN</b>\n"
            f"{'─'*34}\n"
            f"Bot tidak akan auto-pause karena win harian.{pnl_note}"
        )
    else:
        old_str = f"{old_pct:.1f}%" if old_pct > 0 else "OFF"
        send_telegram_raw(
            f"🎯 <b>Max Daily Win Diset: {new_pct:.1f}%</b>\n"
            f"{'─'*34}\n"
            f"📈 Sebelum : <b>{old_str}</b>\n"
            f"📈 Sekarang: <b>{new_pct:.1f}%</b> dari portfolio\n"
            f"{'─'*34}\n"
            f"Jika PnL harian ≥ <b>+{new_pct:.1f}%</b> → bot auto-pause.\n"
            f"Bot akan auto-start besok UTC 00:00.{pnl_note}\n"
            f"{'─'*34}\n"
            f"ℹ️ Gunakan /maxdailywin 0 untuk nonaktifkan."
        )
    print(f"⚙️ DAILY_WIN_LIMIT_PCT: {old_pct:.1f}% → {new_pct:.1f}%")
    save_state()


def cmd_resetdailylimit():
    """
    /resetdailylimit
    Reset manual daily PnL baseline ke balance saat ini.
    Berguna setelah switch LIVE ↔ DEMO, atau saat PnL harian
    terhitung salah karena perubahan saldo mendadak.

    Efek:
      - balance_open di-set ke balance saat ini → PnL harian = 0 mulai sekarang
      - paused_by di-reset → limit tidak aktif lagi (jika sebelumnya kena limit)
      - Limit persen (DAILY_LOSS / DAILY_WIN) TIDAK diubah

    Gunakan juga saat:
      - Switch dari DEMO ke LIVE (PnL tiba-tiba minus banyak karena beda saldo)
      - Mau mulai tracking harian dari titik saldo baru
    """
    global bot_paused, _daily_limit_state

    try:
        new_balance_open = get_total_balance()
    except Exception as e:
        send_telegram_raw(f"❌ Gagal fetch balance: <code>{e}</code>\nDaily limit tidak di-reset.")
        return

    now       = datetime.now(timezone.utc)
    today     = now.strftime("%Y-%m-%d")
    month_str = now.strftime("%Y-%m")
    old_balance_open = _daily_limit_state.get("balance_open", 0.0)
    old_paused_by    = _daily_limit_state.get("paused_by")

    # ── Reset daily limit baseline ────────────────────────────────────────────
    _daily_limit_state["date"]         = today
    _daily_limit_state["balance_open"] = new_balance_open
    _daily_limit_state["paused_by"]    = None
    _daily_limit_state["auto_started"] = False

    # ── Reset Cumulative PNL snapshot (Hari ini & Bulan ini) ──────────────────
    # PENTING: balance_day_start & balance_month_start wajib di-reset ke saldo
    # saat ini agar Cumulative PNL tidak terhitung dari balance mode/hari lama.
    bot_state["balance_day_start"]   = new_balance_open
    bot_state["balance_day_date"]    = today
    bot_state["balance_month_start"] = new_balance_open
    bot_state["balance_month_key"]   = month_str

    # Jika bot di-pause oleh daily limit → otomatis resume
    resumed_note = ""
    if old_paused_by in ("DAILY_LOSS", "DAILY_WIN") and bot_paused:
        bot_paused = False
        label = "Daily Loss" if old_paused_by == "DAILY_LOSS" else "Daily Win"
        resumed_note = f"\n▶️ Bot otomatis di-<b>RESUME</b> (sebelumnya pause karena {label})."

    loss_str = f"🛑 Max Daily Loss: <b>{DAILY_LOSS_LIMIT_PCT:.1f}%</b>" if DAILY_LOSS_LIMIT_PCT > 0 else "⭕ Max Daily Loss: <b>OFF</b>"
    win_str  = f"🎯 Max Daily Win : <b>{DAILY_WIN_LIMIT_PCT:.1f}%</b>"  if DAILY_WIN_LIMIT_PCT  > 0 else "⭕ Max Daily Win : <b>OFF</b>"

    send_telegram_raw(
        f"🔄 <b>Daily Limit Baseline Di-Reset</b>\n"
        f"{'─'*34}\n"
        f"📅 Tanggal        : <b>{today}</b>\n"
        f"💰 Balance Lama   : <b>{old_balance_open:.2f} USDT</b>\n"
        f"💰 Balance Baru   : <b>{new_balance_open:.2f} USDT</b>\n"
        f"{'─'*34}\n"
        f"📊 PnL harian & bulanan dihitung ulang dari <b>0%</b> mulai sekarang.\n"
        f"{loss_str}\n"
        f"{win_str}"
        f"{resumed_note}\n"
        f"{'─'*34}\n"
        f"ℹ️ Limit % tidak berubah. Gunakan /maxdailyloss atau /maxdailywin untuk ubah."
    )
    print(f"🔄 Daily limit di-reset manual | balance_open: {old_balance_open:.2f} → {new_balance_open:.2f} USDT | mode: {BOT_MODE}")
    print(f"🔄 Cumulative PNL snapshot di-reset → day_start={new_balance_open:.2f} | month_start={new_balance_open:.2f}")
    save_state()


def handle_command(text: str):
    """Dispatch perintah Telegram ke handler yang sesuai."""
    text  = text.strip()
    parts = text.split()
    cmd   = parts[0].lower().split("@")[0]  # hapus @botname jika ada

    if cmd == "/start":
        cmd_start()
    elif cmd == "/pnl":
        cmd_pnl()
    elif cmd == "/status":
        cmd_status()
    elif cmd == "/changemargin":
        cmd_changemargin(parts)
    elif cmd == "/changelev":
        cmd_changelev(parts)
    elif cmd == "/resumeorpause":
        cmd_resumeorpause()
    elif cmd in ("/closeallposition", "/closeallpositions"):
        cmd_closeallposition()
    elif cmd == "/setmarginratio":
        cmd_setmarginratio(parts)
    elif cmd == "/maxopentrade":
        cmd_maxopentrade(parts)
    elif cmd == "/changeliveordemo":
        cmd_changeliveordemo()
    elif cmd == "/setfixedlev":
        cmd_setfixedlev(parts)
    elif cmd == "/resetmm":
        cmd_resetmm()
    elif cmd == "/setscoreupto":
        cmd_setscoreupto(parts)
    elif cmd == "/togglebtcfilter":
        cmd_togglebtcfilter()
    elif cmd == "/resetpnl":
        cmd_resetpnl()
    elif cmd == "/backtest":
        cmd_backtest(parts)
    elif cmd == "/scalpingonly":
        cmd_scalpingonly()
    elif cmd == "/intradayonly":
        cmd_intradayonly()
    elif cmd == "/longonly":
        cmd_longonly()
    elif cmd == "/shortonly":
        cmd_shortonly()
    elif cmd == "/resetdirection":
        cmd_resetdirection()
    elif cmd == "/settp1profit":
        cmd_settp1profit(parts)
    elif cmd == "/resettp1":
        cmd_resettp1()
    elif cmd == "/settp1partial":
        cmd_settp1partial(parts)
    elif cmd == "/maxdailyloss":
        cmd_maxdailyloss(parts)
    elif cmd == "/maxdailywin":
        cmd_maxdailywin(parts)
    elif cmd == "/resetdailylimit":
        cmd_resetdailylimit()
    elif cmd == "/superscalpermode":
        cmd_superscalpermode()
    elif cmd in ("/cleanuporders", "/cleanuporder", "/cleanupordern", "/cleanup"):
        pending_info = f"\n⏳ Pending limit orders ({len(pending_limit_orders)}): {sorted(pending_limit_orders.keys()) or 'none'}" if pending_limit_orders else ""
        send_telegram_raw(f"🔄 <b>Cleanup orphan orders dimulai...</b>{pending_info}\nPending limit orders TIDAK akan disentuh.")
        try:
            cleanup_stale_orders(dry_run=False)
        except Exception as e:
            send_telegram_raw(f"❌ Cleanup error: <code>{e}</code>")
    elif cmd == "/help":
        sep = "─" * 34
        send_telegram_raw(
            f"🤖 <b>Daftar Command Bot</b>\n"
            f"{sep}\n"
            "/start\n"
            "  Mulai bot (dari kondisi standby saat startup)\n\n"
            "/pnl\n"
            "  Lihat unrealized PnL semua posisi aktif\n\n"
            "/status\n"
            "  Status lengkap bot + posisi aktif\n\n"
            "/changemargin &lt;SYMBOL&gt; &lt;ISOLATED|CROSSED&gt;\n"
            "  Ganti margin type, contoh:\n"
            "  <code>/changemargin BTCUSDT ISOLATED</code>\n\n"
            "/changelev &lt;SYMBOL&gt; &lt;1-125&gt;\n"
            "  Ganti leverage, contoh:\n"
            "  <code>/changelev BTCUSDT 20</code>\n\n"
            "/resumeorpause\n"
            "  Toggle pause/resume scanning & trading\n\n"
            "/closeallposition\n"
            "  Tutup semua posisi aktif sekarang\n\n"
            "/setmarginratio &lt;persen&gt;\n"
            "  Set max SL loss % per trade — kerugian maks jika SL kena.\n"
            "  Lot dihitung mundur: lot = (balance × %) ÷ jarak_SL\n"
            "  <code>/setmarginratio 1</code>  → jika SL kena, max rugi 1% dari balance\n\n"
            "/maxopentrade &lt;jumlah&gt;\n"
            "  Ubah max posisi yang boleh dibuka (1–20), contoh:\n"
            "  <code>/maxopentrade 3</code>\n\n"
            "/changeliveordemo\n"
            "  Toggle mode LIVE ↔ DEMO (testnet/mainnet)\n"
            "  ⚠️ LIVE = uang asli! Pastikan API key mainnet\n\n"
            "/setfixedlev &lt;leverage&gt;\n"
            "  Set leverage tetap untuk semua trade, contoh:\n"
            "  <code>/setfixedlev 10</code>  → semua trade pakai 10x\n\n"
            "/resetmm\n"
            "  Reset fixed leverage → kembali ke auto-tier by price\n\n"
            "/setscoreupto &lt;score&gt;\n"
            "  Set minimum score sinyal (1–100, 0=reset ke default), contoh:\n"
            "  <code>/setscoreupto 70</code>  → semua tier wajib ≥ 70 pts\n"
            "  <code>/setscoreupto 0</code>   → reset ke default (45/28)\n\n"
            "/togglebtcfilter\n"
            "  Toggle filter korelasi BTC + BTC.D ON/OFF (satu command)\n"
            "  ON: kombinasi arah BTC price + BTC.D menentukan sinyal lolos\n"
            "  Contoh: BTC↑ Dom↓ → LONG alt | BTC↓ Dom↑ → SHORT alt\n\n"
            "/resetpnl\n"
            "  Reset PnL kumulatif mode aktif (LIVE atau DEMO)\n\n"
            "/backtest &lt;SYMBOL&gt; &lt;TF&gt; [DAYS]\n"
            "  Backtest sinyal SMC di historical data, contoh:\n"
            "  <code>/backtest BTCUSDT 1h 30</code>\n"
            "  TF: 15m, 30m, 1h, 4h | DAYS: 7–90 (default 30)\n\n"
            "/scalpingonly\n"
            "  Aktifkan HANYA mode LOW_TF + LTF_30M (entry 15m/30m)\n"
            "  Scalping sejati — SL ketat, sinyal cepat\n\n"
            "/intradayonly\n"
            "  Aktifkan mode INTRADAY + SCALPING (4h→1h)\n"
            "  Sinyal intraday — SCALPING masuk kategori ini\n\n"
            "/allmode\n"
            "  Aktifkan kembali SEMUA mode scan (default)\n\n"
            "/longonly\n"
            "  Hanya proses sinyal LONG (BULLISH) — SHORT dilewati\n\n"
            "/shortonly\n"
            "  Hanya proses sinyal SHORT (BEARISH) — LONG dilewati\n\n"
            "/resetdirection\n"
            "  Reset direction filter → bot kembali proses LONG + SHORT\n\n"
            "/settp1profit &lt;persen&gt;\n"
            "  Set TP1 sebagai % fixed dari entry (berlaku trade baru), contoh:\n"
            "  <code>/settp1profit 1.5</code> → TP1 di +1.5% dari entry\n"
            "  <code>/settp1profit 0</code>   → reset ke TP1 otomatis\n\n"
            "/resettp1\n"
            "  Reset TP1 ke mode otomatis (struktur swing/S&amp;D/S/R)\n\n"
            "/settp1partial &lt;persen&gt;\n"
            "  Set % lot yang di-close di TP1 (sisanya lanjut ke TP2), contoh:\n"
            "  <code>/settp1partial 25</code> → TP1 close 25%, TP2 close 75%\n"
            "  <code>/settp1partial 50</code> → default (50%/50%)\n\n"
            "/maxdailyloss &lt;persen&gt;\n"
            "  Set batas max loss harian (% dari total portfolio), contoh:\n"
            "  <code>/maxdailyloss 5</code>   → pause jika loss ≥ 5% hari ini\n"
            "  <code>/maxdailyloss 0</code>   → nonaktifkan limit\n"
            "  Bot auto-pause saat limit tercapai, auto-start besok UTC 00:00\n\n"
            "/maxdailywin &lt;persen&gt;\n"
            "  Set batas max profit harian (% dari total portfolio), contoh:\n"
            "  <code>/maxdailywin 10</code>   → pause jika profit ≥ 10% hari ini\n"
            "  <code>/maxdailywin 0</code>    → nonaktifkan limit\n"
            "  Bot auto-pause saat limit tercapai, auto-start besok UTC 00:00\n\n"
            "/resetdailylimit\n"
            "  Reset manual baseline PnL harian ke balance saat ini.\n"
            "  PnL harian dihitung ulang dari 0% mulai sekarang.\n"
            "  Pakai setelah switch LIVE ↔ DEMO agar PnL tidak false-trigger.\n"
            "  Jika bot di-pause karena daily limit → otomatis di-resume.\n\n"
            "/cleanuporders\n"
            "  Cancel semua orphan orders (orders tanpa posisi aktif)\n"
            "  ✅ Pending limit orders TIDAK disentuh\n\n"
            "/superscalpermode\n"
            "  Toggle Super Scalper Mode ON/OFF\n"
            "  ON  → 20 pair high-liquidity (OI+Vol terbesar Binance Futures)\n"
            "        15m/30m only | Score≥45 | RR≥1.8 | SL max 1.2%\n"
            "        TP1=0.6% | BTC filter OFF | SL cooldown 1 jam\n"
            "  OFF → semua setting lama di-restore otomatis\n"
            "  ⚠️ Rekomendasi: aktifkan /maxdailyloss 3 sebelum ON\n\n"
            "📌 <b>Filter SL Otomatis:</b>\n"
            f"  Sinyal dengan SL &gt; {MAX_SL_DISTANCE_PCT*100:.0f}% dari entry otomatis di-SKIP\n"
            f"  (SL besar = kategori swing, tidak cocok futures)"
        )
    else:
        pass  # Abaikan perintah tidak dikenal


_command_lock = threading.Lock()   # cegah dua command diproses bersamaan


def telegram_polling_loop():
    """
    Background thread yang terus polling update Telegram.
    Hanya memproses message dari TELEGRAM_CHAT_ID (owner).

    Fix double-fire:
    - deleteWebhook saat startup — webhook + polling tidak bisa bersamaan (salah satu menyebabkan dobel)
    - Hanya proses update.message (bukan edited_message) — edit pesan tidak trigger command
    - Dedup via _processed_update_ids: set update_id yang sudah diproses, cegah double execute
      jika polling sempat dapat update yang sama dua kali (misal reconnect)
    - Update offset SEGERA setelah dapat update_id, sebelum proses command
    - _command_lock: mutex agar hanya 1 command diproses dalam satu waktu (cegah race condition)
    """
    global _tg_offset
    _processed_update_ids: set = set()   # dedup dalam 1 sesi bot
    print("🤖 Telegram command listener aktif...")

    # ── Hapus webhook aktif jika ada ─────────────────────────────────────────
    # Jika ada webhook aktif, Telegram split delivery → command bisa jalan dua kali.
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteWebhook",
            data={"drop_pending_updates": False},
            timeout=10,
        )
        wh_result = r.json()
        if wh_result.get("result"):
            print("✅ Webhook dihapus — polling mode aktif")
        else:
            print(f"ℹ️ deleteWebhook: {wh_result.get('description', 'OK')}")
    except Exception as _we:
        print(f"⚠️ deleteWebhook error (lanjut): {_we}")

    # ── Flush pending updates lama sebelum mulai proses command ─────────────
    # Saat bot restart, Telegram masih punya antrian update lama (command yang
    # dikirim sebelum bot mati). Kalau langsung diproses → bot bisa tiba-tiba
    # pause/resume/start sendiri.
    #
    # FIX: Hanya flush update yang timestamp-nya LEBIH LAMA dari waktu bot start.
    # Command yang dikirim SETELAH bot start (dalam 60 detik terakhir) tetap diproses.
    # Ini mencegah /start yang baru dikirim ikut ke-flush.
    _bot_start_unix = int(time.time())   # timestamp saat polling mulai
    try:
        _flush = tg_get_updates(0)
        flushed_count = 0
        if _flush:
            for u in _flush:
                msg_date = u.get("message", {}).get("date", 0)
                # Flush update yang dikirim SEBELUM bot start (bukan dalam 30 detik terakhir)
                if msg_date < _bot_start_unix - 30:
                    _processed_update_ids.add(u["update_id"])
                    flushed_count += 1
                    # Update offset hanya untuk update yang di-flush
                    if u["update_id"] + 1 > _tg_offset:
                        _tg_offset = u["update_id"] + 1
            if flushed_count:
                print(f"🧹 Flushed {flushed_count} pending Telegram updates (offset now {_tg_offset})")
            kept = len(_flush) - flushed_count
            if kept:
                print(f"📬 {kept} update baru dipertahankan untuk diproses (dikirim < 30 detik lalu)")
    except Exception as _fe:
        print(f"⚠️ Flush pending updates error: {_fe}")

    while True:
        try:
            updates = tg_get_updates(_tg_offset)
            for update in updates:
                uid = update["update_id"]
                # Selalu update offset lebih dulu — cegah re-fetch update yang sama
                if uid + 1 > _tg_offset:
                    _tg_offset = uid + 1

                # Skip jika update_id ini sudah pernah diproses (dedup)
                if uid in _processed_update_ids:
                    continue
                _processed_update_ids.add(uid)
                # Jaga ukuran set agar tidak membengkak (simpan 500 terakhir, hapus 100 terlama)
                if len(_processed_update_ids) > 500:
                    oldest = sorted(_processed_update_ids)[:100]
                    for old_id in oldest:
                        _processed_update_ids.discard(old_id)

                # Hanya proses message biasa — ABAIKAN edited_message
                # edited_message terjadi saat user edit pesan lama → tidak boleh re-trigger command
                msg = update.get("message")
                if not msg:
                    continue
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != TELEGRAM_CHAT_ID:
                    continue  # Abaikan pesan dari luar owner
                text = msg.get("text", "")
                if text.startswith("/"):
                    print(f"📨 Command diterima: {text} (update_id={uid})")
                    try:
                        with _command_lock:   # mutex: cegah dua command jalan bersamaan
                            handle_command(text)
                    except Exception as e:
                        print(f"⚠️ Command error: {e}")
        except Exception as e:
            print(f"⚠️ Polling error: {e}")

        time.sleep(0.5)   # ← FIX: tidak perlu besar, long-poll sudah blocking di Telegram server


# ═══════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 20 — HTF RANGING ADAPTIVE ENGINE (v5 NEW)
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class RangingContext:
    """Hasil evaluasi saat HTF sedang ranging."""
    allowed:                bool          = False
    direction:              Optional[str] = None
    ltf_score:              int           = 0
    reasons:                list          = field(default_factory=list)
    score_breakdown:        dict          = field(default_factory=dict)
    bos_confirmed:          bool          = False
    bos_strength:           str           = "NONE"
    displacement_confirmed: bool          = False
    volume_spike:           bool          = False
    in_ob:                  bool          = False
    in_fvg:                 bool          = False
    session_ok:             bool          = False
    range_position:         str           = "MID"
    ltf_momentum:           str           = "NONE"
    near_range_edge:        bool          = False


def _ranging_atr(df: pd.DataFrame, period: int = 14) -> float:
    trs = []
    for i in range(1, len(df)):
        h  = float(df.iloc[i]["high"])
        lo = float(df.iloc[i]["low"])
        pc = float(df.iloc[i - 1]["close"])
        trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
    return sum(trs[-period:]) / min(period, len(trs)) if trs else 0.0


def _is_range_wide_enough(df_htf: pd.DataFrame) -> bool:
    """Range HTF harus minimal RANGE_ATR_FACTOR × ATR agar layak di-trade."""
    atr        = _ranging_atr(df_htf)
    lookback   = min(50, len(df_htf))
    recent     = df_htf.iloc[-lookback:]
    range_span = float(recent["high"].max()) - float(recent["low"].min())
    return range_span >= atr * RANGE_ATR_FACTOR


def _assess_range_position(df_htf: pd.DataFrame) -> tuple:
    """
    Deteksi posisi harga di dalam range HTF.
    Returns: (position: str, pct_from_bottom: float)
    """
    lookback   = min(50, len(df_htf))
    recent     = df_htf.iloc[-lookback:]
    range_high = float(recent["high"].max())
    range_low  = float(recent["low"].min())
    price      = float(df_htf["close"].iloc[-1])
    full_range = range_high - range_low

    if full_range < 1e-9:
        return "MID", 0.5

    pct = (price - range_low) / full_range

    if pct >= (1 - RANGE_EDGE_BUFFER_PCT):  return "NEAR_HIGH", pct
    if pct <= RANGE_EDGE_BUFFER_PCT:        return "NEAR_LOW",  pct
    return "MID", pct


def _detect_ltf_bos(df: pd.DataFrame, direction: str, window: int = 5) -> tuple:
    """
    Deteksi BOS pada LTF entry TF.
    Returns: (confirmed: bool, strength: str, body_pct: float)
    """
    if len(df) < window * 2 + 3:
        return False, "NONE", 0.0

    lookback = df.iloc[-(window * 2 + 2):-2]
    recent   = df.iloc[-3:]

    if direction == "BULLISH":
        pivot = float(lookback["high"].max())
        broke = any(float(c["close"]) > pivot for _, c in recent.iterrows())
    else:
        pivot = float(lookback["low"].min())
        broke = any(float(c["close"]) < pivot for _, c in recent.iterrows())

    if not broke:
        return False, "NONE", 0.0

    bos_candle   = df.iloc[-2]
    o, h, lo, cl = (float(bos_candle[x]) for x in ["open", "high", "low", "close"])
    candle_range = h - lo
    body         = abs(cl - o)
    body_pct     = (body / candle_range * 100) if candle_range > 0 else 0.0

    is_directional = (cl > o) if direction == "BULLISH" else (cl < o)
    if not is_directional:
        return True, "WEAK", body_pct

    strength = "STRONG" if body_pct / 100 >= STRONG_BOS_BODY_PCT else "WEAK"
    return True, strength, body_pct


def _detect_ltf_momentum(df: pd.DataFrame, n: int = LTF_CONSECUTIVE_MIN) -> str:
    """N candle berturut-turut arah sama → momentum terkonfirmasi."""
    if len(df) < n + 1:
        return "NONE"
    recent = df.iloc[-n:]
    bull   = all(float(c["close"]) > float(c["open"]) for _, c in recent.iterrows())
    bear   = all(float(c["close"]) < float(c["open"]) for _, c in recent.iterrows())
    if bull: return "BULLISH"
    if bear: return "BEARISH"
    return "NONE"


def _detect_ranging_displacement(df: pd.DataFrame, direction: str) -> bool:
    """
    Displacement candle khusus mode ranging:
    - Body ≥ DISPLACEMENT_BODY_RATIO_MIN × range
    - Body ≥ 1.5× rata-rata body 10 candle terakhir
    - Arah sesuai direction
    """
    for candle in [df.iloc[-1], df.iloc[-2]]:
        o, h, lo, cl = (float(candle[x]) for x in ["open", "high", "low", "close"])
        candle_range = h - lo
        if candle_range < 1e-9:
            continue

        body       = abs(cl - o)
        body_ratio = body / candle_range

        directional = (cl > o) if direction == "BULLISH" else (cl < o)
        if not directional or body_ratio < DISPLACEMENT_BODY_RATIO_MIN:
            continue

        recent_bodies = [
            abs(float(df.iloc[i]["close"]) - float(df.iloc[i]["open"]))
            for i in range(-12, -2)
            if len(df) >= 12
        ]
        avg_body = sum(recent_bodies) / len(recent_bodies) if recent_bodies else body
        if body >= avg_body * 1.5:
            return True

    return False


def evaluate_htf_ranging(
    df_htf:    pd.DataFrame,
    df_entry:  pd.DataFrame,
    htf_event: Optional[str],
    pair:      str,
    label:     str,
) -> RangingContext:
    """
    Evaluasi apakah trade layak dilakukan saat HTF ranging.

    Menggantikan logika hard-skip:
        if htf_bias == "RANGING": return

    Dengan sistem scoring LTF 10-tahap. Hanya allow trade jika:
    - Range HTF cukup lebar (bukan tight chop)
    - Harga di tepi range atau ada CHoCH di mid
    - Ada LTF BOS terkonfirmasi
    - Ada minimal 1 confluence (displacement / OB / FVG / volume spike)
    - LTF composite score ≥ LTF_RANGING_SCORE_GATE (40)  # UPDATED
    """
    ctx = RangingContext()
    bd  = {}

    # ── 0. Gate: range HTF harus cukup lebar ─────────────────────────────────
    if not _is_range_wide_enough(df_htf):
        ctx.reasons.append("HTF range terlalu sempit — pure chop ❌")
        return ctx

    # ── 1. Range position ─────────────────────────────────────────────────────
    range_pos, pct_from_bot = _assess_range_position(df_htf)
    ctx.range_position = range_pos

    if range_pos == "NEAR_LOW":
        range_bias = "BULLISH"
        bd["range_position"] = 20
        ctx.reasons.append(f"Harga dekat LOW range ({pct_from_bot*100:.0f}% dari bawah) → Bullish bias ✅")
    elif range_pos == "NEAR_HIGH":
        range_bias = "BEARISH"
        bd["range_position"] = 20
        ctx.reasons.append(f"Harga dekat HIGH range ({pct_from_bot*100:.0f}% dari bawah) → Bearish bias ✅")
    else:
        # Mid-range: hanya izin kalau ada CHoCH, atau lanjut dengan score kecil  # UPDATED
        if htf_event == "CHoCH":
            range_bias = None  # resolusi dari LTF momentum
            bd["range_position"] = 5
            ctx.reasons.append("CHoCH di mid-range → butuh konfirmasi LTF ⚠️")
        else:
            # UPDATED: tidak langsung return, lanjut dengan penalty score kecil
            range_bias = None
            bd["range_position"] = -5
            ctx.reasons.append("⚠️ Harga di mid-range tanpa CHoCH — chop risk, penalti score")

    # ── 2. LTF BOS ────────────────────────────────────────────────────────────
    test_dirs = [range_bias] if range_bias else ["BULLISH", "BEARISH"]
    bos_confirmed, bos_strength, bos_body_pct = False, "NONE", 0.0
    resolved_direction = None

    for d in test_dirs:
        confirmed, strength, body_pct = _detect_ltf_bos(df_entry, d)
        if confirmed and strength in ("STRONG", "WEAK"):
            bos_confirmed, bos_strength, bos_body_pct = confirmed, strength, body_pct
            resolved_direction = d
            break

    ctx.bos_confirmed = bos_confirmed
    ctx.bos_strength  = bos_strength

    if not bos_confirmed:
        # UPDATED: tidak hard return, beri penalty dan lanjut
        bd["ltf_bos"] = -10
        ctx.reasons.append("⚠️ Tidak ada LTF BOS — penalti score, lanjut proses")
    else:
        bos_pts = 25 if bos_strength == "STRONG" else 12
        bd["ltf_bos"] = bos_pts
        ctx.reasons.append(
            f"LTF BOS {resolved_direction} ({bos_strength}) | Body: {bos_body_pct:.1f}% "
            f"{'✅' if bos_strength == 'STRONG' else '⚠️'}"
        )

    # ── 3. Consecutive momentum ───────────────────────────────────────────────
    # UPDATED: jika BOS tidak terkonfirmasi, gunakan range_bias atau deteksi dari candle
    if resolved_direction is None:
        resolved_direction = range_bias or ("BULLISH" if float(df_entry["close"].iloc[-1]) > float(df_entry["open"].iloc[-1]) else "BEARISH")

    momentum = _detect_ltf_momentum(df_entry)
    ctx.ltf_momentum = momentum

    if momentum == resolved_direction:
        bd["momentum"] = 10
        ctx.reasons.append(f"✅ {LTF_CONSECUTIVE_MIN} candle momentum {momentum} terkonfirmasi")
    elif momentum == "NONE":
        bd["momentum"] = 0
        ctx.reasons.append("⚠️ Momentum LTF campuran (tidak konsisten)")
    else:
        bd["momentum"] = -5  # UPDATED: dikurangi dari -15 → -5
        ctx.reasons.append(f"⚠️ Momentum LTF berlawanan dengan BOS — penalti ringan")

    # ── 4. Displacement candle ────────────────────────────────────────────────
    has_disp = _detect_ranging_displacement(df_entry, resolved_direction)
    ctx.displacement_confirmed = has_disp
    bd["displacement"] = 15 if has_disp else 0
    ctx.reasons.append(
        "✅ Displacement candle terkonfirmasi" if has_disp else "⚠️ Tidak ada displacement candle"
    )

    # ── 5. Volume spike ───────────────────────────────────────────────────────
    vol_rat       = volume_ratio(df_entry)
    has_vol_spike = vol_rat >= VOLUME_SPIKE_MULTIPLIER
    ctx.volume_spike = has_vol_spike

    if has_vol_spike:
        bd["volume_spike"] = 10
        ctx.reasons.append(f"✅ Volume spike: {vol_rat}× avg (≥{VOLUME_SPIKE_MULTIPLIER}×)")
    elif vol_rat >= 1.5:
        bd["volume_spike"] = 5
        ctx.reasons.append(f"⚡ Volume elevated: {vol_rat}× avg")
    else:
        bd["volume_spike"] = 0
        ctx.reasons.append(f"⚠️ Volume normal: {vol_rat}× avg")

    # ── 6. OB / FVG confluence ────────────────────────────────────────────────
    price  = float(df_entry["close"].iloc[-1])
    ob_list = find_order_blocks(df_entry, resolved_direction)
    fvg    = find_fvg(df_entry, resolved_direction)
    in_ob, _ = price_in_ob(price, ob_list)
    in_fvg   = price_in_fvg(price, fvg)

    ctx.in_ob  = in_ob
    ctx.in_fvg = in_fvg

    if in_ob and in_fvg:
        bd["ob_fvg"] = 15
        ctx.reasons.append("✅ OB + FVG confluence")
    elif in_ob:
        bd["ob_fvg"] = 10
        ctx.reasons.append("✅ Harga di dalam Order Block")
    elif in_fvg:
        bd["ob_fvg"] = 8
        ctx.reasons.append("✅ Harga di dalam Fair Value Gap")
    else:
        bd["ob_fvg"] = 0
        ctx.reasons.append("⚠️ Tidak ada OB/FVG (pure momentum play)")

    # ── 7. Session ────────────────────────────────────────────────────────────
    session    = get_session()
    sess_bonus = SESSION_BONUS_MAP.get(session, 0)
    ctx.session_ok = session in ("London", "New York")
    bd["session"] = sess_bonus
    ctx.reasons.append(
        f"{'✅' if ctx.session_ok else '⚠️'} Session: {session} (+{sess_bonus} pts)"
    )

    # ── 8. Edge penalty ───────────────────────────────────────────────────────
    edge_penalty = 0
    if range_pos == "NEAR_HIGH" and resolved_direction == "BULLISH":
        edge_penalty = -20
        ctx.near_range_edge = True
        ctx.reasons.append("❌ BULLISH dekat HIGH range — fade risk → penalti besar")
    elif range_pos == "NEAR_LOW" and resolved_direction == "BEARISH":
        edge_penalty = -20
        ctx.near_range_edge = True
        ctx.reasons.append("❌ BEARISH dekat LOW range — fade risk → penalti besar")
    bd["edge_penalty"] = edge_penalty

    # ── 9. Hitung total score ─────────────────────────────────────────────────
    total = max(0, min(100, sum(bd.values())))
    ctx.ltf_score       = total
    ctx.score_breakdown = bd
    ctx.direction       = resolved_direction

    # ── 10. Confluence check (penalty, bukan hard skip) ───────────────────────  # UPDATED
    has_confluence = has_disp or in_ob or in_fvg or has_vol_spike or bos_strength == "STRONG"  # UPDATED
    if not has_confluence:
        # UPDATED: tidak return, tambah penalty kecil lalu recalc
        bd["confluence_penalty"] = -5  # UPDATED: penalty ringan -5
        ctx.reasons.append("⚠️ Tidak ada confluence kuat — penalti score kecil")
        total = max(0, min(100, sum(bd.values())))  # recalc setelah penalty
        ctx.ltf_score       = total
        ctx.score_breakdown = bd
    else:
        bd["confluence_penalty"] = 0

    # ── 11. Gate final score ──────────────────────────────────────────────────  # UPDATED
    if total < LTF_RANGING_SCORE_GATE:
        # UPDATED: tidak return langsung, log saja lalu biarkan ctx.allowed = False
        ctx.reasons.append(
            f"⚠️ LTF score {total} < gate {LTF_RANGING_SCORE_GATE} → tidak disetujui"
        )
        # ctx.allowed tetap False (default), return tanpa sinyal
        return ctx

    ctx.allowed = True
    ctx.reasons.append(f"✅ HTF RANGING trade DISETUJUI | LTF score: {total}/100")
    return ctx


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 21 — PAIR ANALYSIS v5 (HTF RANGING ADAPTIVE)
# ═══════════════════════════════════════════════════════════════════════════

def analyze_pair(pair, mode, df_htf, df_ref, btc_bias, btcd_trend, session, signal_candidates,
                 btc_allow_long: bool = True, btc_allow_short: bool = True,
                 btc_h1_div: str = "NONE", btc_h4_div: str = "NONE",
                 btc_h1_stoch_state: str = "NEUTRAL"):
    """
    ═══════════════════════════════════════════════════════════════
    SMC FLOW — SCORING MURNI (v6) + BTC MULTI-TF FILTER
    ═══════════════════════════════════════════════════════════════
    Step 1 : HTF → tentukan bias arah
    Step 2 : LTF → cari liquidity target (sweep / reaksi)
    Step 3 : BOS / momentum terkonfirmasi
    Step 4 : Entry di pullback (OB / FVG)
    Step 5 : Hitung score akumulatif → kirim jika ≥ threshold

    Tidak ada hard-gate per komponen.
    Setiap komponen memberi poin positif atau negatif.
    Sinyal SELALU ada kandidat; yang dikirim = yang paling kuat.
    ═══════════════════════════════════════════════════════════════
    """
    label    = mode["label"]
    htf_tf   = mode["htf_tf"]
    entry_tf = mode["entry_tf"]
    tier     = mode.get("tier", "FULL")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1 — HTF BIAS
    # ─────────────────────────────────────────────────────────────────────────
    htf_bias, htf_event, _, _ = detect_structure(df_htf)

    # Tentukan trade_direction dari HTF
    # OPT: df_entry_pre akan di-reuse sebagai df_entry jika HTF ranging
    # Ini eliminasi 1 API call per pair saat HTF ranging (hemat ~30% request di volatile market)
    df_entry_pre = None   # diisi saat HTF ranging

    if htf_bias != "RANGING":
        trade_direction = htf_bias
        htf_aligned     = True
    else:
        # HTF ranging → cari arah dari LTF momentum / range position
        # Fetch entry TF untuk deteksi awal (akan di-reuse, tidak perlu fetch ulang)
        try:
            df_entry_pre = fetch_ohlcv(pair, entry_tf, limit=250)   # langsung 250 agar bisa reuse
            time.sleep(0.15)
        except Exception as e:
            print(f"  ❌ [{label}] {pair} — pre-fetch failed: {e}")
            return
        if df_entry_pre is None or len(df_entry_pre) == 0:
            print(f"  ⚠️  [{label}] {pair} — pre-fetch None/kosong, skip")
            return

        # Gunakan adaptive evaluator untuk tentukan arah & ranging_ctx
        ranging_ctx = evaluate_htf_ranging(
            df_htf    = df_htf,
            df_entry  = df_entry_pre,
            htf_event = htf_event,
            pair      = pair,
            label     = label,
        )
        if not ranging_ctx.allowed:
            print(
                f"  ⏭  [{label}] {pair} — HTF RANGING score {ranging_ctx.ltf_score} "
                f"< {LTF_RANGING_SCORE_GATE}"
            )
            return

        trade_direction = ranging_ctx.direction
        htf_aligned     = False   # HTF ranging → penalti di score
        print(
            f"  🔀 [{label}] {pair} — HTF RANGING → LTF approved "
            f"({trade_direction}) | LTF score {ranging_ctx.ltf_score}"
        )

    # RELAXED tier (LOW_TF): pakai logika asli, skip ranging tanpa CHoCH
    if tier == "RELAXED":
        if htf_bias == "RANGING" and htf_event != "CHoCH":
            print(f"  ⏭  [{label}] {pair} — 1H ranging tanpa CHoCH, skip")
            return
        if htf_bias == "RANGING":
            last            = df_htf.iloc[-1]
            trade_direction = "BULLISH" if float(last["close"]) > float(last["open"]) else "BEARISH"
            htf_aligned     = False
        else:
            htf_aligned = True

    ranging_ctx = locals().get("ranging_ctx")  # None kalau HTF tidak ranging

    # ── SIGNAL COOLDOWN CHECK ──────────────────────────────────────────────────
    # Blok sinyal arah yang sama selama SIGNAL_COOLDOWN_HOURS jam setelah sinyal terakhir.
    # Arah BERLAWANAN tetap boleh (LONG cooldown tidak blok SHORT).
    # Cooldown diangkat otomatis jika: SL kena, posisi expired, atau flip arah.
    on_cd, cd_remaining, cd_reason = is_on_cooldown(pair, trade_direction)
    if on_cd:
        dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  ⏳ [{label}] {pair} {dir_str} — {cd_reason}")
        return

    # ── DIRECTION FILTER CHECK (/longonly / /shortonly) ──────────────────────
    if _DIRECTION_FILTER != "ALL":
        _dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        if _dir_str != _DIRECTION_FILTER:
            print(f"  ⏭  [{label}] {pair} {_dir_str} — difilter (hanya {_DIRECTION_FILTER})")
            return

    # ── BTC MULTI-TF DIRECTION GATE ──────────────────────────────────────────
    # Gate utama berbasis BTC Daily → H4 → H1 analysis.
    # LONG diblok jika BTC Daily bukan BULLISH.
    # SHORT diblok jika BTC Daily bukan BEARISH.
    # Exception: BTCUSDT sendiri lolos (BTC tidak filter dirinya sendiri).
    _is_btc_pair = pair in ("BTCUSDT", "BTC/USDT")
    if not _is_btc_pair:
        if trade_direction == "BULLISH" and not btc_allow_long:
            _dir = "LONG" if trade_direction == "BULLISH" else "SHORT"
            print(f"  🚫 [{label}] {pair} {_dir} — BTC Daily tidak BULLISH, diblok BTC Multi-TF Gate")
            return
        if trade_direction == "BEARISH" and not btc_allow_short:
            _dir = "LONG" if trade_direction == "BULLISH" else "SHORT"
            print(f"  🚫 [{label}] {pair} {_dir} — BTC Daily tidak BEARISH, diblok BTC Multi-TF Gate")
            return
    # ─────────────────────────────────────────────────────────────────────────

    # ── MARKET REGIME HARDBLOCK — BTC EMA + BTCD EMA ─────────────────────────
    # Layer pertahanan paling awal, sebelum apapun diproses:
    #   BULL_REGIME (BTC EMA↑ + BTCD EMA↓) → SHORT alt DIBLOK total
    #   BEAR_REGIME (BTC EMA↓ + BTCD EMA↑) → LONG  alt DIBLOK total
    # Tidak ada pengecualian. Cache 3 menit — tidak overload API.
    _regime, _regime_reason, _regime_block_long, _regime_block_short = get_market_regime()
    if _regime_block_long and trade_direction == "BULLISH":
        print(f"  🚫 [{label}] {pair} LONG — REGIME BLOCK: {_regime_reason}")
        return
    if _regime_block_short and trade_direction == "BEARISH":
        print(f"  🚫 [{label}] {pair} SHORT — REGIME BLOCK: {_regime_reason}")
        return
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # Ref TF bonus
    # ─────────────────────────────────────────────────────────────────────────
    ref_bias    = get_ref_bias(df_ref)
    ref_aligned = (ref_bias == trade_direction)

    # ─────────────────────────────────────────────────────────────────────────
    # Fetch entry TF (250 candle) — reuse df_entry_pre jika sudah ada (HTF ranging)
    # ─────────────────────────────────────────────────────────────────────────
    if df_entry_pre is not None:
        df_entry = df_entry_pre   # OPT: reuse, skip API call
    else:
        try:
            df_entry = fetch_ohlcv(pair, entry_tf, limit=250)
            time.sleep(0.15)
        except Exception as e:
            print(f"  ❌ [{label}] {pair} fetch failed: {e}")
            return
    if df_entry is None or len(df_entry) == 0:
        print(f"  ⚠️  [{label}] {pair} — df_entry None/kosong, skip")
        return

    current_price = float(df_entry["close"].iloc[-1])

    if has_active_position(pair, trade_direction):
        dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  🚫 [{label}] {pair} — Posisi {dir_str} aktif, skip")
        return

    # Volatility gate (satu-satunya hard-gate yang tersisa — pasar harus bergerak)
    vol_ok, vol_reason = check_volatility(df_entry)
    if not vol_ok:
        print(f"  ⛔ [{label}] {pair} — {vol_reason}")
        return

    # ── ADX FILTER: skip jika pasar terlalu ranging (HTF mode saja) ─────────────
    # ADX < ADX_TRENDING_MIN = market choppy, sinyal directional tidak reliable.
    # Filter ini TIDAK berlaku untuk LOW_TF / LTF_30M (scalping bisa entry di awal breakout
    # sebelum ADX sempat naik). HTF = INTRADAY/SCALPING entry 1H lebih butuh trend confirmation.
    if label not in ("LOW_TF", "LTF_30M"):
        _adx_val = calculate_adx(df_htf, period=14)
        if _adx_val < ADX_TRENDING_MIN:
            dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
            print(
                f"  ⛔ [{label}] {pair} {dir_str} — ADX HTF={_adx_val:.1f} < {ADX_TRENDING_MIN} "
                f"(market choppy, skip)"
            )
            return
        elif _adx_val >= ADX_TRENDING_FULL:
            print(f"  📈 [{label}] {pair} — ADX HTF={_adx_val:.1f} (trending kuat ✅)")
        else:
            print(f"  ⚡ [{label}] {pair} — ADX HTF={_adx_val:.1f} (trending lemah, lanjut)")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2 — LIQUIDITY TARGET (Sweep / reaksi)
    # ─────────────────────────────────────────────────────────────────────────
    # Coba strong sweep dulu, lalu relaxed — hasilnya masuk scoring, bukan gate
    liq_swept, liq_type, liq_level, sweep_strength = detect_liquidity_sweep(
        df_entry, trade_direction, strict=True
    )
    if not liq_swept:
        liq_swept, liq_type, liq_level, sweep_strength = detect_liquidity_sweep(
            df_entry, trade_direction, strict=False
        )
    # Tidak ada sweep → liq_swept=False, score negatif, tapi proses tetap lanjut

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3 — BOS / MOMENTUM
    # ─────────────────────────────────────────────────────────────────────────
    entry_bias, entry_event, _, _ = detect_structure(df_entry)
    bos_confirmed  = (entry_bias == trade_direction and entry_event == "BOS")
    bos_str_label  = "STRONG"  # default; bisa di-grade lebih lanjut
    if bos_confirmed:
        # grade kekuatan BOS dari candle terakhir
        last_c     = df_entry.iloc[-1]
        o_c, h_c, l_c, cl_c = float(last_c["open"]), float(last_c["high"]), float(last_c["low"]), float(last_c["close"])
        crange     = h_c - l_c
        body_pct   = abs(cl_c - o_c) / crange if crange > 0 else 0
        bos_str_label = "STRONG" if body_pct >= STRONG_BOS_BODY_PCT else "WEAK"
    else:
        bos_str_label = "NONE"

    # Momentum: N candle berturut-turut arah sama
    ltf_momentum   = _detect_ltf_momentum(df_entry)
    momentum_align = (ltf_momentum == trade_direction)
    momentum_any   = (ltf_momentum != "NONE")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4 — ENTRY ZONE (OB / FVG / IMB / Gap pullback)
    # ─────────────────────────────────────────────────────────────────────────
    price        = float(df_entry["close"].iloc[-1])
    ob_list      = find_order_blocks(df_entry, trade_direction)
    in_ob, best_ob = price_in_ob(price, ob_list)
    fvg          = find_fvg(df_entry, trade_direction)
    in_fvg       = price_in_fvg(price, fvg)
    imb          = find_imb(df_entry, trade_direction)
    in_imb       = price_in_imb(price, imb)
    gap          = find_gap(df_entry, trade_direction)
    in_gap       = price_in_gap(price, gap)

    # ── CANDLE KONFIRMASI — penalty jika tidak ada, bukan hard-skip ─────────────
    # Cegah entry di candle yang masih bisa berbalik.
    conf_candle = check_confirmation_candle(df_entry, trade_direction)
    _conf_candle_penalty = 0
    if not conf_candle:
        dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  ⚠️  [{label}] {pair} {dir_str} — Candle konfirmasi lemah (penalty -10 pts)")
        _conf_candle_penalty = -10

    # Supporting
    vol_rat      = volume_ratio(df_entry)
    displacement = detect_displacement(df_entry, trade_direction)
    rsi          = calculate_rsi(df_entry)

    # RSI directional penalty (bukan hard block) — sesuai main(1).py
    # LONG: RSI > 70 → -10 poin | SHORT: RSI < 30 → -10 poin
    # Ini ditangani otomatis oleh rsi_score() yang sudah diupdate

    m_pts, macro_reason = macro_score(pair, trade_direction, btc_bias, btcd_trend)
    ema_pts, ema_reason, ema20, ema50, ema200 = get_ema_score(df_entry, trade_direction)

    # ── BONUS/PENALTY dari BTC Multi-TF Divergence & Stoch State ──────────────
    # Bonus maksimal +10 (divergence + stoch state ideal searah trade)
    # Penalty maksimal -10 (divergence berlawanan arah trade)
    _btc_mtf_bonus = 0
    _btc_mtf_notes = []

    if not _is_btc_pair:
        # Divergence H1 BTC searah trade = timing entry sempurna
        if trade_direction == "BULLISH" and btc_h1_div == "BULLISH_DIV":
            _btc_mtf_bonus += 6
            _btc_mtf_notes.append("BTC H1 Bullish Div (timing Long ideal) +6")
        elif trade_direction == "BEARISH" and btc_h1_div == "BEARISH_DIV":
            _btc_mtf_bonus += 6
            _btc_mtf_notes.append("BTC H1 Bearish Div (timing Short ideal) +6")
        # Divergence H1 BTC berlawanan = counter-signal
        elif trade_direction == "BULLISH" and btc_h1_div == "BEARISH_DIV":
            _btc_mtf_bonus -= 5
            _btc_mtf_notes.append("BTC H1 Bearish Div saat Long -5")
        elif trade_direction == "BEARISH" and btc_h1_div == "BULLISH_DIV":
            _btc_mtf_bonus -= 5
            _btc_mtf_notes.append("BTC H1 Bullish Div saat Short -5")

        # Divergence H4 BTC searah trade = konfirmasi lebih kuat
        if trade_direction == "BULLISH" and btc_h4_div == "BULLISH_DIV":
            _btc_mtf_bonus += 4
            _btc_mtf_notes.append("BTC H4 Bullish Div +4")
        elif trade_direction == "BEARISH" and btc_h4_div == "BEARISH_DIV":
            _btc_mtf_bonus += 4
            _btc_mtf_notes.append("BTC H4 Bearish Div +4")

        # Stoch RSI H1 BTC: Oversold saat Long = pullback selesai
        if trade_direction == "BULLISH" and btc_h1_stoch_state in ("OVERSOLD", "CROSSING_UP"):
            _btc_mtf_bonus += 3
            _btc_mtf_notes.append(f"BTC H1 Stoch {btc_h1_stoch_state} → timing Long bagus +3")
        elif trade_direction == "BEARISH" and btc_h1_stoch_state in ("OVERBOUGHT", "CROSSING_DOWN"):
            _btc_mtf_bonus += 3
            _btc_mtf_notes.append(f"BTC H1 Stoch {btc_h1_stoch_state} → timing Short bagus +3")

    if _btc_mtf_bonus != 0:
        m_pts = m_pts + _btc_mtf_bonus
        if _btc_mtf_notes:
            macro_reason = macro_reason + " | BTC MTF: " + ", ".join(_btc_mtf_notes)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4b — BTC/BTCD LTF CORRELATION FILTER
    # Deteksi kondisi BTC + BTC Dominance di low timeframe sebelum sinyal keluar.
    # Blok sinyal jika kondisi makro LTF sangat berlawanan.
    # ─────────────────────────────────────────────────────────────────────────
    df_btc_ltf    = fetch_btc_ltf_data(BTC_LTF_TF, limit=60)
    btc_ltf_dir   = get_btc_ltf_direction(df_btc_ltf)
    df_btcd       = fetch_btcd_ohlcv(tf=BTCDOM_LTF_TF, limit=80)
    btcd_ltf_dir  = get_btcd_ltf_direction(df_btcd)

    corr_score, corr_reason, corr_blocked = analyze_btcd_correlation(
        direction    = trade_direction,
        btc_ltf_dir  = btc_ltf_dir,
        btcd_ltf_dir = btcd_ltf_dir,
        btc_bias_htf = btc_bias,
        btcd_trend_htf = btcd_trend,
        pair         = pair,
    )

    # jika BTC_CORR_FILTER_ON = False, corr_blocked harus selalu False
    if not BTC_CORR_FILTER_ON:
        corr_blocked = False

    if corr_blocked:
        dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  🚫 [{label}] {pair} {dir_str} — BTCD CORR BLOK: {corr_reason}")
        return

    # corr_score dimasukkan ke macro_pts sebagai bonus/penalty tambahan
    m_pts = m_pts + corr_score
    if corr_score != 0:
        macro_reason = f"{macro_reason} | {corr_reason}"

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4c — BTC SITUATIONAL AWARENESS
    # ─────────────────────────────────────────────────────────────────────────
    _btc_htf_df = None
    try:
        _btc_htf_df = fetch_ohlcv("BTC/USDT", "1h", limit=100)
    except Exception:
        pass

    btc_situation = analyze_btc_situation(
        df_btc_ltf    = df_btc_ltf,
        df_btc_htf    = _btc_htf_df,
        btc_bias_htf  = btc_bias,
        btcd_ltf_dir  = btcd_ltf_dir,
        btcd_trend_htf= btcd_trend,
    )

    _sit = btc_situation["situation"]
    _sit_reason = btc_situation["reason"]

    if btc_situation["block_short"] and trade_direction == "BEARISH":
        print(f"  🚫 [{label}] {pair} SHORT — BTC SITUATIONAL BLOCK: {_sit_reason}")
        return

    if btc_situation["block_long"] and trade_direction == "BULLISH":
        print(f"  🚫 [{label}] {pair} LONG — BTC SITUATIONAL BLOCK: {_sit_reason}")
        return

    if btc_situation["warn_short"] and trade_direction == "BEARISH":
        print(f"  ⚠️  [{label}] {pair} SHORT — BTC SITUATIONAL WARN: {_sit_reason}")
    if btc_situation["warn_long"] and trade_direction == "BULLISH":
        print(f"  ⚠️  [{label}] {pair} LONG — BTC SITUATIONAL WARN: {_sit_reason}")

    _sit_adj = btc_situation["score_adj"]
    if _sit_adj != 0:
        m_pts = m_pts + _sit_adj
        macro_reason = f"{macro_reason} | {_sit_reason}"
        print(f"  🪙 [{label}] {pair} — BTC Situation: {_sit} ({_sit_adj:+d} pts)")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5 — SCORING AKUMULATIF (SMC Flow — IMB+Gap aware, sesuai main(1).py)
    # ─────────────────────────────────────────────────────────────────────────
    score, score_bd = compute_score(
        htf_aligned      = htf_aligned,
        bos_confirmed    = bos_confirmed,
        bos_strength     = bos_str_label,
        momentum_aligned = momentum_align,
        momentum_present = momentum_any,
        liq_swept        = liq_swept,
        sweep_strength   = sweep_strength,
        in_ob            = in_ob,
        in_fvg           = in_fvg,
        in_imb           = in_imb,
        in_gap           = in_gap,
        displacement     = displacement,
        vol_rat          = vol_rat,
        session          = session,
        rsi              = rsi,
        macro_pts        = m_pts,
        ema_pts          = ema_pts,
        ref_aligned      = ref_aligned,
        direction        = trade_direction,
    )

    # Terapkan penalty candle konfirmasi ke score total
    if _conf_candle_penalty < 0:
        score += _conf_candle_penalty

    # ── Zona SMC wajib — minimal satu zona (OB/FVG/IMB/Gap) ─────────────────
    if not in_ob and not in_fvg and not in_imb and not in_gap:
        print(f"  ⛔ [{label}] {pair} — Tidak di zona SMC (OB/FVG/IMB/Gap), sinyal diblok")
        return

    # Threshold berdasarkan tier
    # PENTING: MIN_SCORE_CUSTOM berlaku untuk SEMUA tier (FULL & RELAXED).
    # RELAXED tier TIDAK mendapat diskon — kalau custom aktif, gate-nya sama.
    if tier == "RELAXED":
        effective_gate = MIN_SCORE_RELAXED_CUSTOM if MIN_SCORE_CUSTOM > 0 else MIN_SCORE_RELAXED
    else:
        effective_gate = MIN_SCORE_CUSTOM if MIN_SCORE_CUSTOM > 0 else MIN_SCORE

    # ─────────────────────────────────────────────────────────────────────────
    # ATR — diperlukan untuk adaptive SL, TP2 fallback, dan buffer validasi
    # ─────────────────────────────────────────────────────────────────────────
    atr = calculate_atr(df_entry)

    # ─────────────────────────────────────────────────────────────────────────
    # RR check (tetap diperlukan sebagai quality gate — bukan logika, tapi risk)
    # ─────────────────────────────────────────────────────────────────────────
    entry, sl, tp1, tp2, rr1, rr2, tp_anchored = calculate_rr(
        df_entry, trade_direction,
        best_ob if in_ob else None,
        fvg     if in_fvg else None,
        mode_label = label,
    )

    grade = grade_signal(rr1)
    if grade is None:
        print(f"  ⛔ [{label}] {pair} — RR {rr1} < {RR_GRADE_B} (tidak layak)")
        return

    # ── RR gate per-mode (termasuk Super Scalper override) ────────────────────
    # grade_signal hanya cek floor global (RR_GRADE_B=1.5).
    # get_rr_min_for_mode() bisa return lebih tinggi (mis. 1.8 saat super scalper).
    _rr_gate = get_rr_min_for_mode(label)
    if rr1 < _rr_gate:
        dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  ⛔ [{label}] {pair} {dir_str} — RR {rr1:.2f} < {_rr_gate} (gate mode {label})")
        return

    # ── Jika TP tidak punya anchor struktural → penalty score, bukan langsung skip ──
    # FIX: dulu hard-skip (return), sekarang hanya kurangi score agar sinyal tetap bisa lolos
    # jika faktor lain cukup kuat. Ini mencegah buang setup valid hanya karena TP tidak tepat di swing.
    _tp_anchor_penalty = 0
    if not tp_anchored:
        _tp_anchor_penalty = -15
        dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  ⚠️  [{label}] {pair} {dir_str} — TP tidak ter-anchor ke swing/struktur (penalty -15 pts)")

    # ── Validasi arah TP vs entry — guard agar TP tidak "kebalik" atau terlalu dekat ──
    _sl_dist = abs(entry - sl)
    if trade_direction == "BULLISH":
        if tp1 <= entry:
            print(f"  ⛔ [{label}] {pair} LONG — TP1 ({tp1:.4f}) <= entry ({entry:.4f}), skip")
            return
        if tp2 <= tp1:
            # TP2 invalid tapi TP1 ok — downgrade tp2 ke fallback (tidak tolak sinyal)
            tp2 = tp1 + max(atr * 0.5, (tp1 - entry) * 0.3)
        # TP1 tidak boleh lebih dekat dari 80% jarak SL (mencegah TP1 trivial)
        if (tp1 - entry) < _sl_dist * 0.8:
            print(f"  ⛔ [{label}] {pair} LONG — TP1 terlalu dekat entry vs SL (RR rendah), skip")
            return
    else:  # BEARISH
        if tp1 >= entry:
            print(f"  ⛔ [{label}] {pair} SHORT — TP1 ({tp1:.4f}) >= entry ({entry:.4f}), skip")
            return
        if tp2 >= tp1:
            tp2 = tp1 - max(atr * 0.5, (entry - tp1) * 0.3)
        if (entry - tp1) < _sl_dist * 0.8:
            print(f"  ⛔ [{label}] {pair} SHORT — TP1 terlalu dekat entry vs SL (RR rendah), skip")
            return

    if SIGNAL_MODE == "SNIPER" and grade != "A":
        print(f"  ⛔ [{label}] {pair} — SNIPER: Grade {grade} rejected")
        return

    # ── ADAPTIVE SL: pastikan SL minimal 1× ATR dari entry ────────────────────
    # SL yang lebih sempit dari ATR hampir pasti kena noise sebelum harga jalan.
    # Setelah SL dilebarkan, RR di-recheck — jika jatuh di bawah RR_GRADE_B → skip.
    # Ambil max_sl_pct dari SL_TP_CAPS agar konsisten dengan konfigurasi per-mode.
    _sl_max_pct_for_mode = SL_TP_CAPS.get(label, (0.03, 0.06))[0]
    sl, rr1, _sl_ok = apply_adaptive_sl(
        entry       = entry,
        sl          = sl,
        direction   = trade_direction,
        atr         = atr,
        tp1         = tp1,
        tp2         = tp2,
        mode_label  = label,
        rr_min      = get_rr_min_for_mode(label),
        max_sl_pct  = _sl_max_pct_for_mode,
    )
    if not _sl_ok:
        dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(
            f"  ⛔ [{label}] {pair} {dir_str} — RR={rr1} < {RR_GRADE_B} "
            f"setelah SL dilebarkan ke 1×ATR (skip)"
        )
        return
    # Re-grade setelah adaptive SL (RR bisa berubah)
    grade = grade_signal(rr1)
    if grade is None:
        print(f"  ⛔ [{label}] {pair} — RR {rr1} < {RR_GRADE_B} setelah adaptive SL (tidak layak)")
        return

    # ── ENTRY CONFIRMATION: pastikan candle terakhir sudah bounce dari zona ──────
    # Sebelum limit order dipasang, harga harus sudah menunjukkan tanda reversal
    # dari zona OB/FVG (bounce candle) — bukan langsung entry saat harga menyentuh zona.
    # Ini mengurangi false entry saat harga hanya "menyentuh" zona lalu lanjut breakdown.
    #
    # Syarat (salah satu):
    #   BULLISH: candle terakhir bullish (close > open) DI DALAM atau DEKAT zona
    #   BEARISH: candle terakhir bearish (close < open) DI DALAM atau DEKAT zona
    #
    # Jika tidak ada bounce → penalty -8 pts (tidak hard-skip, karena candle mungkin
    # belum closed penuh saat scan berjalan). Ini BERBEDA dari conf_candle_penalty
    # yang sudah ada — ini lebih spesifik ke "bounce dari zona entry".
    _last_c    = df_entry.iloc[-1]
    _last_open = float(_last_c["open"])
    _last_cls  = float(_last_c["close"])
    _zone_bounce_penalty = 0

    if in_ob and best_ob:
        # Cek apakah candle terakhir mulai bounce dari zona OB
        if trade_direction == "BULLISH":
            _in_zone = _last_c["low"] <= best_ob["high"] * 1.002   # low candle menyentuh OB
            _bounced = _last_cls > _last_open   # candle bullish = tanda bounce
        else:
            _in_zone = _last_c["high"] >= best_ob["low"] * 0.998   # high candle menyentuh OB
            _bounced = _last_cls < _last_open   # candle bearish = tanda bounce
        if _in_zone and not _bounced:
            _zone_bounce_penalty = -8
            print(f"  ⚠️  [{label}] {pair} — Harga di OB tapi belum bounce (penalty -8 pts)")
    elif in_fvg and fvg:
        # Cek bounce dari zona FVG
        if trade_direction == "BULLISH":
            _in_zone = _last_c["low"] <= fvg.get("top", entry) * 1.002
            _bounced = _last_cls > _last_open
        else:
            _in_zone = _last_c["high"] >= fvg.get("bottom", entry) * 0.998
            _bounced = _last_cls < _last_open
        if _in_zone and not _bounced:
            _zone_bounce_penalty = -8
            print(f"  ⚠️  [{label}] {pair} — Harga di FVG tapi belum bounce (penalty -8 pts)")

    if _zone_bounce_penalty < 0:
        score += _zone_bounce_penalty

    # ── SL Distance Filter: hard global cap 6% dulu, lalu per-mode ─────────────
    # Hard cap 6% berlaku untuk SEMUA mode tanpa kecuali.
    # Ini mencegah sinyal dengan SL abnormal (>6%) lolos walau SL_TP_CAPS sudah di-set.
    _sl_dist_pct = abs(entry - sl) / max(entry, 1e-9)
    if _sl_dist_pct > 0.06:   # 6% hard maximum, semua mode
        dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(
            f"  ⛔ [{label}] {pair} {dir_str} — SL terlalu jauh: {_sl_dist_pct*100:.2f}% > 6% hard cap (skip)"
        )
        return

    # ── SL Distance Filter per-mode ───────────────────────────────────────────────
    # Ambil batas SL dari SL_TP_CAPS agar satu sumber kebenaran — tidak ada hardcode
    # yang tidak sinkron saat SL_TP_CAPS diubah (misal saat Super Scalper Mode ON).
    _sl_max_pct = SL_TP_CAPS.get(label, (0.03, 0.06))[0]

    if _sl_dist_pct > _sl_max_pct:
        print(
            f"  ⛔ [{label}] {pair} — SL terlalu jauh: {_sl_dist_pct*100:.2f}% > "
            f"{_sl_max_pct*100:.1f}% (skip, mode {label})"
        )
        return

    if score < effective_gate:
        print(
            f"  📉 [{label}] {pair} — Score {score} < {effective_gate} "
            f"| HTF:{score_bd['htf']} BOS:{score_bd['bos']} "
            f"Mom:{score_bd['momentum']} Liq:{score_bd['liquidity']} Zone:{score_bd['entry_zone']}"
        )
        return

    # Terapkan penalty TP non-anchor SETELAH gate (agar tidak double-punish sinyal borderline)
    # Sinyal yang sudah lolos gate, lalu kena penalty → re-check
    if _tp_anchor_penalty < 0:
        score += _tp_anchor_penalty
        if score < effective_gate:
            print(
                f"  📉 [{label}] {pair} — Score {score} < {effective_gate} setelah TP anchor penalty "
                f"(skip)"
            )
            return

    # ─────────────────────────────────────────────────────────────────────────
    # Build reasons
    # ─────────────────────────────────────────────────────────────────────────
    htf_label = f"HTF {htf_bias} ({htf_tf})" if htf_bias != "RANGING" \
        else f"HTF RANGING ({htf_tf}) → LTF {trade_direction}"
    reasons = [f"{htf_label} — {htf_event or 'trend confirmed'}"]

    if ranging_ctx is not None:
        reasons.append(f"🔀 Ranging adaptive: LTF score {ranging_ctx.ltf_score}/100")
        for r in ranging_ctx.reasons:
            reasons.append(f"  ↳ {r}")

    if ref_bias != "N/A":
        reasons.append(f"Ref TF ({mode.get('ref_tf')}) bias: {ref_bias} {'✅' if ref_aligned else '⚠️'}")
    reasons.append(f"EMA: {ema_reason}")

    if bos_confirmed:
        reasons.append(f"BOS {trade_direction} terkonfirmasi ({bos_str_label}) pada {entry_tf}")
    else:
        reasons.append(f"⚠️ Tidak ada BOS pada {entry_tf} (score dikurangi)")

    if momentum_align:
        reasons.append(f"Momentum {ltf_momentum} konsisten ({LTF_CONSECUTIVE_MIN} candle) ✅")
    elif momentum_any:
        reasons.append(f"⚠️ Momentum ada tapi tidak konsisten ({ltf_momentum})")

    if liq_swept:
        lvl_str = f" @ {fmt_price(liq_level)}" if liq_level else ""
        reasons.append(f"Liquidity: {liq_type}{lvl_str} [{sweep_strength}]")
    else:
        reasons.append("⚠️ Tidak ada liquidity sweep (score dikurangi)")

    if in_ob and best_ob:
        reasons.append(f"OB: ${fmt_price(best_ob['low'])}–${fmt_price(best_ob['high'])} (pullback ke OB midpoint)")
    if in_fvg and fvg:
        reasons.append(f"FVG: ${fmt_price(fvg['bottom'])}–${fmt_price(fvg['top'])} ({fvg['fill_pct']*100:.0f}% filled)")
    if in_imb and imb:
        reasons.append(f"IMB: ${fmt_price(imb['bottom'])}–${fmt_price(imb['top'])} (body imbalance {imb['fill_pct']*100:.0f}% filled)")
    if in_gap and gap:
        reasons.append(f"Gap: ${fmt_price(gap['bottom'])}–${fmt_price(gap['top'])} (price gap {gap['size_pct']:.2f}%)")
    # Confluence label
    _active_zones = sum([in_ob, in_fvg, in_imb, in_gap])
    if _active_zones >= 2:
        zone_names = " + ".join(z for z, ok in [("OB", in_ob), ("FVG", in_fvg), ("IMB", in_imb), ("Gap", in_gap)] if ok)
        reasons.append(f"{zone_names} confluence ✅")
    if _active_zones == 0:
        reasons.append("⚠️ Tidak di zona SMC — entry market")

    if displacement:
        reasons.append("Displacement candle terkonfirmasi ✅")
    if vol_rat >= 1.5:
        reasons.append(f"Volume spike: {vol_rat}× avg ✅")
    if session in ("London", "New York"):
        reasons.append(f"Prime session: {session} ✅")
    reasons.append(f"RSI: {rsi:.1f} → {rsi_score(rsi, trade_direction):+} pts")
    reasons.append(f"Macro: {macro_reason}")

    signal = {
        "pair":        pair,
        "direction":   "LONG" if trade_direction == "BULLISH" else "SHORT",
        "entry":       round(entry, 6),
        "stop_loss":   round(sl, 6),
        "take_profit": [round(tp1, 6), round(tp2, 6)],
        "RR":          rr1,
        "score":       score,
        "grade":       grade,
        "reason":      reasons,
        # ── Metadata untuk send_telegram_signal (dibutuhkan di execute_trade) ──
        "_score_bd":               score_bd,
        "_session":                session,
        "_pa_name":                "-",       # SMC mode tidak pakai PA pattern name
        "_btc_bias":               btc_bias,
        "_btcd_trend":             btcd_trend,
        "_macro_reason":           macro_reason,
        "zone_confluence_reasons": [],   # tidak pakai S/R confluence lagi
    }

    # ── Guard dedup sebelum masuk antrian ──────────────────────────────────
    _dir_chk = "LONG" if trade_direction == "BULLISH" else "SHORT"
    if _is_duplicate_signal(pair, _dir_chk, entry, sl):
        print(f"  ⏩ [{label}] {pair} {_dir_chk} — hash match, sudah pernah dikirim, skip")
        return

    signal_candidates.append({
        "pair":         pair,
        "direction":    trade_direction,
        "score":        score,
        "rr":           rr1,
        "signal":       signal,
        "mode":         mode,
        "score_bd":     score_bd,
        "session":      session,
        "rsi":          rsi,
        "btc_bias":     btc_bias,
        "btcd_trend":   btcd_trend,
        "macro_reason": macro_reason,
        "entry":        entry,
        "sl":           sl,
        "tp1":          tp1,
        "tp2":          tp2,
        "tier":         tier,
        "ref_bias":     ref_bias,
        "ema_reason":   ema_reason,
        "ema20":        ema20,
        "ema50":        ema50,
        "ema200":       ema200,
        "ranging_ctx":  ranging_ctx,
        "btc_ltf_dir":  btc_ltf_dir,
        "btcd_ltf_dir": btcd_ltf_dir,
        "corr_reason":  corr_reason,
    })

    tier_label = f"{tier}{'(RANGING)' if ranging_ctx else ''}"
    bos_tag    = f"BOS:{bos_str_label}" if bos_confirmed else "BOS:❌"
    liq_tag    = f"Liq:{'✅' if liq_swept else '❌'}"
    _zone_parts = [z for z, ok in [("OB", in_ob), ("FVG", in_fvg), ("IMB", in_imb), ("Gap", in_gap)] if ok]
    zone_tag    = "Zone:" + "+".join(_zone_parts) if _zone_parts else "Zone:❌"
    corr_em    = "✅" if corr_score >= 0 else "⚠️"
    print(
        f"  📋 [{label}/{tier_label}] {pair} — Queued | "
        f"Score:{score} Grade:{grade} RR:1:{rr1} | "
        f"{bos_tag} {liq_tag} {zone_tag} | "
        f"BTC_LTF:{btc_ltf_dir} BTCD_LTF:{btcd_ltf_dir} {corr_em}"
    )


def fetch_all_tf_for_pair(pair, htf_tfs, ref_tfs) -> dict:
    cache   = {}
    all_tfs = list(set(htf_tfs + ref_tfs))
    for tf in all_tfs:
        try:
            cache[tf] = fetch_ohlcv(pair, tf, limit=250)
            time.sleep(0.15)
        except Exception as e:
            print(f"  ⚠️  {pair} @ {tf}: {e}")
            cache[tf] = None
    return cache


# ═══════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 21b — RALLY EXHAUSTION GATE & AVERAGE MARKET RSI
# ═══════════════════════════════════════════════════════════════════════════

EXHAUSTION_STOCH_THRESHOLD = 78   # %K di atas ini = overbought zone
EXHAUSTION_STOCH_D_GAP     = 5    # %K - %D < 5 dan mengecil = momentum melemah
EXHAUSTION_ENABLED         = True # toggle on/off


def is_rally_exhausted() -> tuple:
    """
    Deteksi apakah rally BTC sudah exhaustion berdasarkan 3 konfirmasi:
    1. Stoch RSI %K > 78 (overbought zone)
    2. Gap %K - %D mengecil (momentum melemah)
    3. Candle BTC 1H terakhir punya upper wick rejection

    Return: (is_exhausted: bool, reason: str)
    """
    if not EXHAUSTION_ENABLED:
        return False, "Exhaustion gate disabled"
    try:
        df_btc = fetch_ohlcv("BTC/USDT", "1h", limit=100)
    except Exception as e:
        return False, f"BTC fetch error: {e}"
    if df_btc is None or len(df_btc) < 20:
        return False, "Data BTC tidak cukup"

    k_now, d_now = calc_stoch_rsi(df_btc)

    # Hitung nilai sebelumnya (potong 1 candle terakhir)
    k_prev, d_prev = calc_stoch_rsi(df_btc.iloc[:-1])

    gap_now  = k_now - d_now
    gap_prev = k_prev - d_prev

    if k_now < EXHAUSTION_STOCH_THRESHOLD:
        return False, f"Stoch %K={k_now:.1f} belum overbought"

    momentum_weak = gap_now < gap_prev and gap_now < EXHAUSTION_STOCH_D_GAP

    last = df_btc.iloc[-1]
    o, h, cl = float(last["open"]), float(last["high"]), float(last["close"])
    body       = abs(cl - o)
    upper_wick = h - max(o, cl)
    wick_ratio = upper_wick / body if body > 1e-9 else 0
    rejection_candle = wick_ratio > 1.2 and cl < o

    if momentum_weak and rejection_candle:
        return True, (
            f"Exhaustion: Stoch %K={k_now:.1f} OB | "
            f"gap {gap_prev:.1f}→{gap_now:.1f} melemah | "
            f"BTC upper wick rejection {wick_ratio:.1f}x"
        )
    if momentum_weak:
        return True, (
            f"Exhaustion: Stoch %K={k_now:.1f} OB | "
            f"gap melemah {gap_prev:.1f}→{gap_now:.1f}"
        )
    return False, f"Stoch %K={k_now:.1f}, gap={gap_now:.1f} — belum exhausted"


def get_average_market_rsi(signal_candidates: list) -> float:
    """
    Hitung rata-rata RSI dari semua pair hasil scan.
    Dipakai sebagai patokan kondisi market keseluruhan setelah scan selesai.

    > 70 → Market overbought → blok sinyal LONG
    < 30 → Market oversold  → blok sinyal SHORT

    Return: float (default 50.0 jika tidak ada data)
    """
    rsi_values = [c["rsi"] for c in signal_candidates if c.get("rsi") and 0 < c["rsi"] <= 100]
    if not rsi_values:
        return 50.0
    return round(sum(rsi_values) / len(rsi_values), 2)


# ██  SECTION 22b — STARTUP IP DETECTION & WHITELIST REMINDER
# ═══════════════════════════════════════════════════════════════════════════

def get_public_ip() -> str | None:
    """
    Ambil IP publik server/PC yang menjalankan bot.
    Coba beberapa layanan sebagai fallback.
    """
    services = [
        "https://api.ipify.org?format=text",
        "https://ifconfig.me/ip",
        "https://checkip.amazonaws.com",
        "https://icanhazip.com",
        "https://ident.me",
    ]
    for url in services:
        try:
            r = requests.get(url, timeout=5)
            ip = r.text.strip()
            if ip:
                return ip
        except Exception:
            continue
    return None


def send_startup_ip(max_retries: int = 5, wait_seconds: int = 30):
    """
    Saat bot start:
    1. Deteksi IP publik server ini
    2. Kirim ke Telegram supaya bisa di-whitelist di Binance API
    3. Countdown 30 detik — kasih waktu whitelist sebelum bot mulai trading
    4. Retry sampai 5x jika IP tidak terdeteksi
    """
    print("\n🌐 Mendeteksi IP publik server...")

    public_ip = None
    for attempt in range(1, max_retries + 1):
        public_ip = get_public_ip()
        if public_ip:
            print(f"✅ IP terdeteksi: {public_ip}")
            break
        print(f"  ⚠️  Percobaan {attempt}/{max_retries} gagal deteksi IP, coba lagi...")
        time.sleep(3)

    now_str = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")

    if public_ip:
        msg = (
            f"🌐 <b>IP Server Bot — {now_str}</b>\n"
            f"{'─'*38}\n"
            f"📡 IP Publik   : <code>{public_ip}</code>\n"
            f"{'─'*38}\n"
            f"⚠️ <b>Jika pakai mode LIVE:</b>\n"
            f"Tambahkan IP ini ke whitelist API key Binance kamu:\n"
            f"  1. Buka <b>Binance → API Management</b>\n"
            f"  2. Edit API key kamu\n"
            f"  3. Tambahkan <code>{public_ip}</code> ke IP Restriction\n"
            f"  4. Simpan perubahan\n"
            f"{'─'*38}\n"
            f"⏳ Bot siap dalam <b>{wait_seconds} detik</b>...\n"
            f"Mode sekarang: <b>{'🔴 LIVE' if BOT_MODE == 'LIVE' else '🟢 DEMO (testnet)'}</b>"
        )
        print(f"\n{'─'*60}")
        print(f"📡 IP PUBLIK SERVER: {public_ip}")
        print(f"{'─'*60}")
        print(f"⚠️  WHITELIST IP INI DI BINANCE API MANAGEMENT JIKA PAKAI MODE LIVE!")
        print(f"{'─'*60}")
    else:
        msg = (
            f"⚠️ <b>Gagal Deteksi IP Server — {now_str}</b>\n"
            f"{'─'*38}\n"
            f"Tidak bisa mendeteksi IP publik setelah {max_retries}x percobaan.\n"
            f"Kemungkinan tidak ada koneksi internet saat startup.\n"
            f"{'─'*38}\n"
            f"🔧 Cek manual IP kamu dan whitelist di Binance API jika mode LIVE.\n"
            f"⏳ Bot siap dalam <b>{wait_seconds} detik</b>...\n"
            f"Mode sekarang: <b>{'🔴 LIVE' if BOT_MODE == 'LIVE' else '🟢 DEMO (testnet)'}</b>"
        )
        print("❌ Gagal deteksi IP publik — bot tetap lanjut, cek internet kamu.")

    send_telegram_raw(msg)

    # Countdown — kasih waktu whitelist IP sebelum bot konek ke Binance
    print(f"\n⏳ Countdown {wait_seconds} detik sebelum bot mulai...")
    for remaining in range(wait_seconds, 0, -5):
        print(f"   {remaining}s ...")
        time.sleep(5)
    print("✅ Countdown selesai!\n")


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 22 — MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("🤖  BOT AUTO TRADE — BINANCE LIVE × PA + S&D + S/R ENGINE  V5")
    print("=" * 70)
    print(f"📊 Pairs       : {len(PAIR_LIST)}")
    print(f"🔢 Modes       : {len(MODES)}")
    for m in MODES:
        print(f"   • {m['label']:10s} HTF: {m['htf_tf']:4s} → Entry: {m['entry_tf']}")
    print(f"🧮 Score Gate  : ≥ {MIN_SCORE} pts")
    print(f"📐 RR Gate     : ≥ {RR_GRADE_B} (Grade B) | ≥ {RR_GRADE_A} (Grade A)")
    print(f"⏱  Cooldown    : {COOLDOWN_MINUTES} min per pair/mode/TF")
    print(f"⚡ Leverage    : Auto-tier (5x-20x by price) | Max Trades: {MAX_OPEN_TRADES} (dynamic, scale otomatis)")
    print()
    print("TELEGRAM COMMANDS: /start /pnl /status /changemargin /changelev /resumeorpause /closeallposition /changeliveordemo /setmarginratio /setfixedlev /resetmm /setscoreupto /togglebtcfilter /backtest /help")
    print("=" * 70)

    # Kirim IP publik ke Telegram + countdown 30 detik untuk whitelist
    send_startup_ip(max_retries=5, wait_seconds=30)

    # ── Load state dari disk (Railway Volume) ─────────────────────────────────
    # Semua setting (BOT_MODE, MARGIN_RATIO, dll) di-restore ke nilai terakhir
    # sebelum bot di-restart. Jika file tidak ada → pakai default di atas.
    load_state()

    # ── Auto-save background thread ───────────────────────────────────────────
    # Simpan state ke disk setiap 5 menit agar tidak hilang jika restart mendadak
    threading.Thread(target=_auto_save_loop, daemon=True).start()

    init_balance()
    load_exchange_info()
    if not _exchange_info_cache:
        print("⚠️  Exchange info kosong! Coba load ulang...")
        load_exchange_info()
    # sync_existing_positions() dipindah ke cmd_start() — jangan sync sebelum user konfirmasi mode
    # Mencegah false-close posisi LIVE karena bot sempat konek endpoint salah saat restart

    # Jalankan Telegram command listener di background thread
    # ← HARUS sebelum notif standby, supaya bot bisa langsung terima /start
    tg_thread = threading.Thread(target=telegram_polling_loop, daemon=True)
    tg_thread.start()
    time.sleep(1)   # beri waktu thread polling siap

    # ── Notif standby — minta user kirim /start ──────────────────────────────
    try:
        bal     = get_total_balance()
        bal_str = f"{bal:.2f} USDT"
    except Exception:
        bal_str = "N/A"

    mode_em = "🔴 LIVE" if BOT_MODE == "LIVE" else "🟢 DEMO (testnet)"

    send_telegram_raw(
        f"⏸ <b>BOT STANDBY — Menunggu /start</b>\n"
        f"{'─'*38}\n"
        f"💼 Balance   : <b>{bal_str}</b>\n"
        f"🌐 Mode      : <b>{mode_em}</b>\n"
        f"⚡ Max SL/trade: <b>{MAX_SL_LOSS_PCT*100:.1f}%</b> dari balance | Max: <b>{MAX_OPEN_TRADES}</b> trades\n"
        f"{'─'*38}\n"
        f"⚠️ <b>Posisi belum di-sync</b> — sync akan dilakukan saat /start dikirim.\n"
        f"Pastikan mode sudah benar sebelum /start:\n"
        f"  /changeliveordemo — ganti LIVE/DEMO (saat ini: <b>{mode_em}</b>)\n"
        f"  /setmarginratio — set max SL loss % per trade\n"
        f"  /maxopentrade — ubah max posisi\n"
        f"  /setfixedlev — leverage tetap untuk semua trade\n"
        f"  /resetmm — reset ke MM dinamis\n"
        f"  /setscoreupto — filter min score sinyal (1–100)\n"
        f"{'─'*38}\n"
        f"Ketik <b>/start</b> untuk konek ke Binance & mulai trading."
    )
    print("⏸ Bot STANDBY — menunggu /start dari Telegram...")

    htf_tfs_needed = list({m["htf_tf"] for m in _ALL_MODES})  # pakai semua TF; difilter per scan

    while True:
        try:
            now     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            session = get_session()
            print(f"\n{'─'*70}")
            print(f"⏰ Scan: {now} UTC | Session: {session} | Source: {_active_data_source}")

            # ── Cek daily loss/win limit & reset hari baru ────────────────────
            # Dilakukan di setiap iterasi, sebelum cek bot_paused
            check_daily_limits()

            if bot_paused:
                # ── PAUSED: tidak ada sync posisi sama sekali ─────────────────
                # SENGAJA tidak memanggil sync_closed_positions() / manage_trailing()
                # saat paused. Alasan:
                #   - Setelah restart Railway, BOT_MODE mungkin belum dikonfirmasi user
                #   - Jika mode=LIVE tapi koneksi ke testnet (atau sebaliknya),
                #     sync akan salah baca posisi → false stoploss / false close
                #   - User harus kirim /start dulu → baru sync posisi real dari Binance
                # Ini mencegah posisi LIVE dianggap close karena bot sempat konek ke DEMO.
                print("⏸ Bot PAUSED — menunggu /start (sync posisi ditunda)")
                time.sleep(SCAN_INTERVAL)
                continue

            check_drawdown()
            sync_closed_positions()          # ← deteksi posisi yang close dari luar
            monitor_pending_limit_orders()   # ← cek limit order: filled/timeout/cancel
            # cleanup_stale_orders() dihapus dari auto-loop — jalankan via /cleanuporders
            manage_trailing()
            send_hourly_position_report()

            # ── Early exit: skip scan pair jika posisi sudah penuh ────────────
            # Hitung pending limit orders sebagai "slot terpakai" agar tidak double entry
            _cur_open = count_open_positions() + len(pending_limit_orders)
            _dyn_max  = get_dynamic_max_trades()
            if _cur_open >= _dyn_max:
                print(f"  📛 Posisi penuh ({_cur_open}/{_dyn_max}, termasuk {len(pending_limit_orders)} pending limit) — skip scan pair")
                time.sleep(SCAN_INTERVAL)
                continue

            # ═══════════════════════════════════════════════════════════════
            # ██  LANGKAH 1 — ANALISA BTC MULTI-TF (PRIORITAS TERTINGGI)
            #
            # BTC dianalisa LEBIH DULU sebelum scan altcoin apapun.
            # Urutan: Daily → H4 → H1, pakai Stochastic RSI 5,3,3 + Divergence
            #
            # ATURAN UTAMA:
            #   • BTC Daily RANGING        → SKIP SEMUA POSISI (tidak ada setup)
            #   • BTC Daily BULLISH        → Long alt diizinkan
            #   • BTC Daily BEARISH        → Short alt diizinkan
            #   • BTCD naik + BTC bearish  → Short tetap OK (alt season bearish)
            #   • BTCD turun + BTC bullish → Long tetap OK (alt season bullish)
            # ═══════════════════════════════════════════════════════════════
            btc_mtf = analyze_btc_multitf()
            btc_bias = btc_mtf.bias   # kompatibel dengan kode lama

            print(f"\n{'─'*70}")
            print(f"🔍 ANALISA BTC MULTI-TIMEFRAME (Daily→H4→H1):")
            print(f"   Daily : {btc_mtf.daily_bias} | Stoch K={btc_mtf.daily_stoch_k:.1f} D={btc_mtf.daily_stoch_d:.1f} [{btc_mtf.daily_stoch_state}]")
            print(f"   H4    : {btc_mtf.h4_bias}    | Stoch K={btc_mtf.h4_stoch_k:.1f} D={btc_mtf.h4_stoch_d:.1f} [{btc_mtf.h4_stoch_state}] | Div: {btc_mtf.h4_divergence}")
            print(f"   H1    : {btc_mtf.h1_bias}    | Stoch K={btc_mtf.h1_stoch_k:.1f} D={btc_mtf.h1_stoch_d:.1f} [{btc_mtf.h1_stoch_state}] | Div: {btc_mtf.h1_divergence}")
            print(f"   Setup : {'✅ VALID' if btc_mtf.setup_valid else '🚫 RANGING — SKIP SEMUA POSISI'}")
            print(f"   Arah  : Long={'✅' if btc_mtf.allow_long else '❌'} | Short={'✅' if btc_mtf.allow_short else '❌'}")
            print(f"   Alasan: {btc_mtf.reason}")

            # ── GATE UTAMA: Jika BTC tidak punya setup → skip scan pair ─────────
            if not btc_mtf.setup_valid:
                print(f"\n🛑 BTC RANGING di Daily — tidak ada setup jelas.")
                print(f"   Semua scan altcoin DITUNDA sampai BTC punya arah trending.")
                # Kirim notif Telegram setiap N menit sekali (tidak setiap scan)
                _btc_ranging_notif_key = "btc_ranging_last_notif"
                _last_notif_ts = getattr(analyze_btc_multitf, "_ranging_notif_ts", 0)
                if time.time() - _last_notif_ts > 1800:   # 30 menit sekali
                    analyze_btc_multitf._ranging_notif_ts = time.time()
                    send_telegram_raw(
                        f"⏸ <b>BTC Ranging — Scan Ditunda</b>\n"
                        f"{'─'*38}\n"
                        f"📊 BTC Daily : <b>RANGING</b> (tidak ada setup trending)\n"
                        f"🔢 Stoch Daily K={btc_mtf.daily_stoch_k:.1f} D={btc_mtf.daily_stoch_d:.1f} [{btc_mtf.daily_stoch_state}]\n"
                        f"🔢 Stoch H4   K={btc_mtf.h4_stoch_k:.1f} D={btc_mtf.h4_stoch_d:.1f} [{btc_mtf.h4_stoch_state}]\n"
                        f"{'─'*38}\n"
                        f"⚠️ Strategi: Tidak ambil posisi saat BTC ranging.\n"
                        f"Bot akan otomatis resume saat BTC punya arah trending kembali."
                    )
                time.sleep(SCAN_INTERVAL)
                continue

            # ── BTC.D: fetch untuk filter korelasi ───────────────────────────────
            if BTC_CORR_FILTER_ON:
                btcd_trend = get_btcd_bias()
                print(f"🪙 BTC Bias Final: {btc_bias} | BTC.D ({BTCD_TF}): {btcd_trend} | Corr Filter: ON")
            else:
                btcd_trend = "FLAT"
                print(f"🪙 BTC Bias Final: {btc_bias} | BTC.D: OFF")

            # ── MARKET REGIME — log setiap scan (cache 3 menit, tidak overload API) ──
            _mr, _mr_reason, _mr_bl, _mr_bs = get_market_regime()
            _mr_icons = {
                "BULL_REGIME": "🟢 BULL REGIME",
                "BEAR_REGIME": "🔴 BEAR REGIME",
                "NEUTRAL":     "⚪ NEUTRAL",
            }
            _mr_label = _mr_icons.get(_mr, "⚪ NEUTRAL")
            _mr_block_str = ""
            if _mr_bl:  _mr_block_str = " | ⛔ LONG DIBLOK"
            if _mr_bs:  _mr_block_str = " | ⛔ SHORT DIBLOK"
            print(f"📊 Market Regime: {_mr_label}{_mr_block_str}")

            # ── Kumpulkan semua kandidat sinyal dulu, baru filter & kirim ────
            signal_candidates = []
            candidates_lock   = threading.Lock()
            _active_modes     = get_active_modes()   # ambil mode aktif (bisa difilter via command)

            # OPT: pakai TF dari mode aktif saja (bukan _ALL_MODES)
            # Hemat request saat /scalpingonly atau /intradayonly aktif
            htf_tfs_active = list({m["htf_tf"] for m in _active_modes})
            ref_tfs_needed = list({m["ref_tf"] for m in _active_modes if m.get("ref_tf")})

            # ── Rally Exhaustion Gate — cek sebelum scan dimulai ─────────────
            exhaustion_mode, exhaustion_reason = is_rally_exhausted()
            if exhaustion_mode:
                print(f"⛽ RALLY EXHAUSTION DETECTED — sinyal LONG akan diblok\n   {exhaustion_reason}")

            # OPT: limit candle per TF — HTF & ref cukup 150, hemat ~40% data transfer
            _TF_LIMIT_MAP = {tf: 150 for tf in htf_tfs_active}
            for tf in ref_tfs_needed:
                _TF_LIMIT_MAP[tf] = 150

            def process_pair(pair):
                tf_cache = {}
                all_tfs  = list(set(htf_tfs_active + ref_tfs_needed))
                for tf in all_tfs:
                    try:
                        limit = _TF_LIMIT_MAP.get(tf, 150)
                        tf_cache[tf] = fetch_ohlcv_realdata(pair, tf, limit=limit)
                        time.sleep(0.4)   # 0.15 → 0.4: kurangi risiko 418 saat banyak pair paralel
                    except Exception as e:
                        print(f"  ⚠️  {pair} @ {tf}: {e}")
                        tf_cache[tf] = None

                local_candidates = []
                for mode in _active_modes:
                    df_htf = tf_cache.get(mode["htf_tf"])
                    df_ref = tf_cache.get(mode["ref_tf"]) if mode.get("ref_tf") else None
                    if df_htf is None:
                        continue
                    try:
                        # ── Filter arah berdasarkan BTC multi-TF result ──────────
                        # Setiap pair dicek apakah arahnya diizinkan oleh BTC setup
                        analyze_pair(
                            pair             = pair,
                            mode             = mode,
                            df_htf           = df_htf,
                            df_ref           = df_ref,
                            btc_bias         = btc_bias,
                            btcd_trend       = btcd_trend,
                            session          = session,
                            signal_candidates= local_candidates,
                            btc_allow_long   = btc_mtf.allow_long,
                            btc_allow_short  = btc_mtf.allow_short,
                            btc_h1_div       = btc_mtf.h1_divergence,
                            btc_h4_div       = btc_mtf.h4_divergence,
                            btc_h1_stoch_state = btc_mtf.h1_stoch_state,
                        )
                    except Exception as e:
                        print(f"  ❌ [{mode['label']}] {pair}: {e}")
                if local_candidates:
                    with candidates_lock:
                        signal_candidates.extend(local_candidates)

            active_pairs = get_active_pairs()   # ← super scalper: 25 pair; normal: full list
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(process_pair, pair): pair for pair in active_pairs}
                for future in as_completed(futures):
                    pair_done = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"  ❌ Thread error [{pair_done}]: {e}")

            # ── Dedup per pair: ambil score tertinggi ────────────────────────
            seen: dict = {}
            for cand in signal_candidates:
                dir_str  = "LONG" if cand["direction"] == "BULLISH" else "SHORT"
                key      = f"{cand['pair']}|{dir_str}"
                existing = seen.get(key)
                if not existing or cand["score"] > existing["score"] or                    (cand["score"] == existing["score"] and cand["rr"] > existing["rr"]):
                    seen[key] = cand
            signal_candidates = list(seen.values())

            # ── Average Market RSI — dihitung setelah scan semua pair selesai ─
            avg_rsi = get_average_market_rsi(signal_candidates)
            long_rsi_blocked  = avg_rsi > 70
            short_rsi_blocked = avg_rsi < 30
            rsi_status = "🔴 OB" if long_rsi_blocked else ("🟢 OS" if short_rsi_blocked else "✅ Normal")
            print(f"📊 Average Market RSI: {avg_rsi:.2f} {rsi_status}")

            if signal_candidates:
                # ── Hitung slot kosong setelah scan semua pair selesai ────────
                _cur_open  = count_open_positions() + len(pending_limit_orders)
                _dyn_max   = get_dynamic_max_trades()
                _slots_free = max(0, _dyn_max - _cur_open)

                print(f"  📊 Scan selesai — {len(signal_candidates)} sinyal ditemukan | "
                      f"Posisi: {_cur_open}/{_dyn_max} | Slot kosong: {_slots_free}")

                if _slots_free == 0:
                    print(f"  🛑 Tidak ada slot kosong ({_cur_open}/{_dyn_max}) — semua sinyal dilewati")
                else:
                    # ── Filter duplikat dulu sebelum sort ────────────────────
                    filtered_candidates = []
                    for cand in signal_candidates:
                        pair  = cand["pair"]
                        entry, sl = cand["entry"], cand["sl"]
                        if _is_duplicate_signal(pair, cand["direction"], entry, sl):
                            dir_str = "LONG" if cand["direction"] == "BULLISH" else "SHORT"
                            print(f"  ⏩ DUPLIKAT dibuang → {pair} {dir_str}")
                        else:
                            filtered_candidates.append(cand)

                    # ── Sort by score DESC, tiebreak by RR DESC ──────────────
                    filtered_candidates.sort(key=lambda c: (c["score"], c["rr"]), reverse=True)

                    # ── Ambil hanya sejumlah slot kosong (N terbaik) ─────────
                    selected = filtered_candidates[:_slots_free]

                    print(f"  ✅ Memilih {len(selected)} sinyal terbaik dari {len(filtered_candidates)} kandidat "
                          f"(slot kosong: {_slots_free})")
                    for i, cand in enumerate(selected, 1):
                        dir_str = "LONG" if cand["direction"] == "BULLISH" else "SHORT"
                        print(f"     #{i} {cand['pair']} {dir_str} | Score:{cand['score']} RR:1:{cand['rr']} Grade:{cand['signal']['grade']}")

                    # ── Eksekusi sinyal terpilih dengan limit order seperti biasa ──
                    for cand in selected:
                        sig        = cand["signal"]
                        pair       = cand["pair"]
                        entry, sl  = cand["entry"], cand["sl"]
                        dir_str    = "LONG" if cand["direction"] == "BULLISH" else "SHORT"
                        tier_label = cand["tier"]

                        # ── Rally Exhaustion Gate ────────────────────────────
                        if exhaustion_mode and cand["direction"] == "BULLISH":
                            print(f"  ⛽ EXHAUSTION → {pair} LONG diblok ({exhaustion_reason})")
                            continue

                        # ── Average Market RSI Gate ──────────────────────────
                        if long_rsi_blocked and cand["direction"] == "BULLISH":
                            print(f"  📊 AVG RSI {avg_rsi:.1f} OVERBOUGHT → {pair} LONG diblok")
                            continue
                        if short_rsi_blocked and cand["direction"] == "BEARISH":
                            print(f"  📊 AVG RSI {avg_rsi:.1f} OVERSOLD → {pair} SHORT diblok")
                            continue

                        print(f"  🚨 SIGNAL → {pair} {dir_str} | Score:{cand['score']} Grade:{sig['grade']} RR:1:{cand['rr']} Tier:{tier_label}")

                        execute_trade(sig, cand["mode"])
                        _register_signal_hash(pair, cand["direction"], entry, sl)
                        save_state()   # ← simpan signal hash ke disk
            else:
                print("  📭 Tidak ada sinyal pada scan ini")

            print_stats()
            send_daily_summary()
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            print("\n👋 Bot dihentikan manual.")
            send_telegram_raw("👋 <b>Bot dihentikan manual.</b>")
            save_state()   # ← simpan state sebelum keluar
            break
        except Exception as e:
            print(f"❌ Loop error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    # ── Instance lock — cegah dua bot jalan bersamaan ────────────────────────
    # Jika file .lock sudah ada dan proses lama masih hidup → tampilkan error & exit.
    # Ini mencegah double-response di Telegram (dua instance reply command yang sama).
    import os, sys, atexit

    LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.lock")

    def _acquire_instance_lock():
        if os.path.exists(LOCK_FILE):
            try:
                with open(LOCK_FILE) as _lf:
                    old_pid = int(_lf.read().strip())
                # Cek apakah PID lama masih hidup
                try:
                    os.kill(old_pid, 0)   # signal 0 = cek eksistensi proses saja
                    # Proses masih hidup → tolak instance baru
                    print(f"\n{'='*60}")
                    print(f"❌  BOT SUDAH BERJALAN! (PID: {old_pid})")
                    print(f"    Matikan instance lama dulu sebelum jalankan bot baru.")
                    print(f"    Cara: tutup terminal bot lama, atau kill PID {old_pid}")
                    print(f"{'='*60}\n")
                    send_telegram_raw(
                        f"⚠️ <b>Instance Ganda Dicegah</b>\n"
                        f"Bot baru dicoba dijalankan tapi instance lama (PID {old_pid}) masih aktif.\n"
                        f"Matikan instance lama dulu!"
                    )
                    sys.exit(1)
                except OSError:
                    # PID tidak ada (proses lama sudah mati tapi lock file tertinggal) → lanjut
                    print(f"ℹ️  Lock file lama ditemukan (PID {old_pid} sudah tidak aktif) — melanjutkan...")
            except Exception:
                pass  # lock file rusak/kosong → abaikan

        # Tulis PID kita ke lock file
        with open(LOCK_FILE, "w") as _lf:
            _lf.write(str(os.getpid()))

        # Hapus lock file saat bot selesai (normal maupun crash)
        atexit.register(lambda: os.remove(LOCK_FILE) if os.path.exists(LOCK_FILE) else None)

    _acquire_instance_lock()

    try:
        main()
    except SystemExit:
        pass  # Clean shutdown (e.g. drawdown limit reached)
    except Exception as e:
        print("❌ ERROR:", e)
    finally:
        try:
            input("\nTekan ENTER untuk keluar...")
        except (EOFError, OSError, ValueError):
            pass  # non-interactive environment (Docker, screen, nohup) — skip input prompt
