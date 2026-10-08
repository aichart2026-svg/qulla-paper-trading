#!/usr/bin/env python3
"""
Виртуальный счёт по сетапам Qullamaggie (поверх qulla_scanner.py).

  python3 paper_trader.py daily         после закрытия рынка США: данные -> проверка -> сканер -> сделки -> Discord
  python3 paper_trader.py daily force   то же, но игнорировать проверку «сегодня биржа закрыта»
  python3 paper_trader.py weekly        PDF со всеми сделками и общим PnL -> Discord
  python3 paper_trader.py status        текущее состояние счёта в консоль

Настройки: файл qulla.env рядом со скриптом (KEY=VALUE) или переменные окружения:
  DISCORD_WEBHOOK   вебхук Discord
  START_EQUITY=10000  стартовый виртуальный счёт
  RISK_PCT=0.5        риск на сделку, % счёта (умножается на коэффициент режима рынка)
  MAX_POSITIONS=10    максимум открытых позиций
  SLIPPAGE_PCT=0.1    проскальзывание на входе и выходе, %
  MARKET_FILTER=1     рыночные фильтры: SPY SMA10 > SMA20 и режим по ширине рынка
  EP_FUNDAMENTALS=1   подтягивать EPS-сюрприз и рост выручки для EP (yfinance, только справочно)
  PREFLIGHT_ATTEMPTS=3, PREFLIGHT_WAIT_S=300, PREFLIGHT_MIN_COVERAGE=90, PREFLIGHT_MIN_UNIVERSE=0.5
  PAPER_DB, OUT_DIR   пути к базе и к файлам отчётов

Идеи проверки данных и режима рынка по ширине взяты (переписаны) из MIT-проекта
VladPetrariu/Qullamaggie-breakout-scanner.

Правила симуляции (дневные свечи):
  данные:        перед сделками проверяется, что у SPY и у 90%+ акций есть бар последней торговой сессии
                 (иначе повторная загрузка, затем алерт и пропуск дня); в праздники скрипт не работает.
  режим рынка:   по ширине (доля акций выше SMA50/SMA20, максимумы/минимумы 52 недель, VIX) берётся самый
                 осторожный показатель: favorable риск x1, mixed x0.5, caution/risk_off сделок нет.
  вход BREAKOUT: сегодня цена пробила максимум консолидации -> вход по max(open, триггер),
                 стоп = минимум дня; если стоп шире ADR — пропуск.
  вход NEAR:     отложенный ордер на следующий день; если день пробил триггер, но закрылся
                 ниже триггера — выход по закрытию (подтверждение пробоя).
  вход EP:       гэп 10%+ на большом объёме сегодня -> вход по закрытию, стоп = минимум дня гэпа.
  выход:         стоп; через 3 дня продаётся половина и стоп в безубыток;
                 остаток — по первому закрытию ниже SMA10 (breakout) / SMA20 (EP).
Все сделки пишутся в SQLite (таблицы trades, events, candidates) с признаками для самообучения.
"""
import io
import json
import os
import sqlite3
import sys
import time
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dateutil.easter import easter

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)


def load_env(path):
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env(os.path.join(BASE, "qulla.env"))

import qulla_scanner as sc  # noqa: E402

WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
DB_PATH = os.getenv("PAPER_DB", os.path.join(BASE, "paper.db"))
OUT_DIR = os.getenv("OUT_DIR", BASE)
START_EQUITY = float(os.getenv("START_EQUITY", "10000"))
RISK_PCT = float(os.getenv("RISK_PCT", "0.5"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "10"))
SLIPPAGE_PCT = float(os.getenv("SLIPPAGE_PCT", "0.1"))
MARKET_FILTER = os.getenv("MARKET_FILTER", "1") == "1"
EP_FUNDAMENTALS = os.getenv("EP_FUNDAMENTALS", "1") == "1"
PREFLIGHT_ATTEMPTS = int(os.getenv("PREFLIGHT_ATTEMPTS", "3"))
PREFLIGHT_WAIT_S = int(os.getenv("PREFLIGHT_WAIT_S", "300"))
PREFLIGHT_MIN_COVERAGE = float(os.getenv("PREFLIGHT_MIN_COVERAGE", "90"))
PREFLIGHT_MIN_UNIVERSE = float(os.getenv("PREFLIGHT_MIN_UNIVERSE", "0.5"))
MAX_POS_PCT = 30.0
PARTIAL_DAYS = 3
PARTIAL_FRAC = 0.5
MAX_CHASE_PCT = 3.0
ET = ZoneInfo("America/New_York")

REASONS = {
    "stop": "стоп",
    "stop_be": "стоп в безубытке",
    "stop_entry_day": "закрытие ниже триггера в день входа",
    "trail_sma10": "закрытие ниже SMA10",
    "trail_sma20": "закрытие ниже SMA20",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT, setup TEXT, status TEXT,
    entry_date TEXT, entry_price REAL,
    shares_init INTEGER, shares INTEGER,
    stop REAL, stop_init REAL, risk_usd REAL,
    days_held INTEGER DEFAULT 0, partial_done INTEGER DEFAULT 0,
    realized REAL DEFAULT 0, last_date TEXT, last_price REAL,
    exit_date TEXT, exit_price REAL, exit_reason TEXT,
    pnl REAL, r_mult REAL, features TEXT
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER, date TEXT, type TEXT,
    price REAL, shares INTEGER, pnl REAL, note TEXT
);
CREATE TABLE IF NOT EXISTS pending (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created TEXT, ticker TEXT, setup TEXT,
    trigger_price REAL, adr REAL, features TEXT
);
CREATE TABLE IF NOT EXISTS equity (
    date TEXT PRIMARY KEY, equity REAL, cash REAL, open_value REAL
);
CREATE TABLE IF NOT EXISTS candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT, ticker TEXT, setup TEXT, status TEXT, score REAL, features TEXT
);
"""


# ---------------------------------------------------------------- календарь биржи (NYSE)
def _nth_weekday(year, month, weekday, n):
    """n-й (с 1) или последний (n=-1) день недели месяца."""
    if n > 0:
        d = Date(year, month, 1)
        return d + timedelta(days=(weekday - d.weekday()) % 7 + 7 * (n - 1))
    d = Date(year + (month == 12), (month % 12) + 1, 1) - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


def nyse_holidays(year):
    def observed(d, saturday_to_friday=True):
        if d.weekday() == 5:
            return d - timedelta(days=1) if saturday_to_friday else None
        if d.weekday() == 6:
            return d + timedelta(days=1)
        return d

    h = set()
    ny = observed(Date(year, 1, 1), saturday_to_friday=False)
    if ny:
        h.add(ny)
    h.add(_nth_weekday(year, 1, 0, 3))          # день Мартина Лютера Кинга
    h.add(_nth_weekday(year, 2, 0, 3))          # день президентов
    h.add(easter(year) - timedelta(days=2))     # Страстная пятница
    h.add(_nth_weekday(year, 5, 0, -1))         # День памяти
    if year >= 2022:
        h.add(observed(Date(year, 6, 19)))      # Juneteenth
    h.add(observed(Date(year, 7, 4)))           # День независимости
    h.add(_nth_weekday(year, 9, 0, 1))          # День труда
    h.add(_nth_weekday(year, 11, 3, 4))         # День благодарения
    h.add(observed(Date(year, 12, 25)))         # Рождество
    return h


def is_session(d):
    return d.weekday() < 5 and d not in nyse_holidays(d.year)


def last_session(now=None):
    """Последняя завершённая торговая сессия (после 16:30 по Нью-Йорку считается завершённой)."""
    now = now or datetime.now(timezone.utc)
    et = now.astimezone(ET)
    d = et.date()
    if is_session(d) and (et.hour, et.minute) >= (16, 30):
        return d
    d -= timedelta(days=1)
    while not is_session(d):
        d -= timedelta(days=1)
    return d


def preflight(data, requested, now=None):
    """Данные годны, если у SPY есть бар последней сессии, у 90%+ акций тоже,
    и загружено не меньше половины запрошенного списка. Возвращает (ok, сообщение, ожидаемая дата)."""
    exp = last_session(now)
    spy = data.get("SPY")
    if spy is None or len(spy) == 0:
        return False, "нет данных SPY", exp
    spy_last = spy.index[-1].date()
    if spy_last != exp:
        return False, f"последний бар SPY {spy_last}, ожидался {exp}", exp
    stocks = [d for t, d in data.items() if t not in ("SPY", "^VIX")]
    if not stocks:
        return False, "нет данных по акциям", exp
    cov = sum(1 for d in stocks if d.index[-1].date() == exp) / len(stocks) * 100
    frac = len(stocks) / max(requested, 1)
    if cov < PREFLIGHT_MIN_COVERAGE:
        return False, f"у {cov:.1f}% акций есть бар {exp} (нужно {PREFLIGHT_MIN_COVERAGE:.0f}%+)", exp
    if frac < PREFLIGHT_MIN_UNIVERSE:
        return False, f"загружено {frac * 100:.0f}% списка (нужно {PREFLIGHT_MIN_UNIVERSE * 100:.0f}%+)", exp
    return True, f"покрытие {cov:.1f}%, загружено {frac * 100:.0f}% списка", exp


# ---------------------------------------------------------------- режим рынка
REGIME_ORDER = ["risk_off", "caution", "mixed", "favorable"]
BREADTH_TH = {
    "pct50": (60, 45, 30),
    "pct20": (55, 40, 25),
    "hl": (2.0, 1.0, 0.5),
}


def _classify(v, th):
    if v >= th[0]:
        return "favorable"
    if v >= th[1]:
        return "mixed"
    if v >= th[2]:
        return "caution"
    return "risk_off"


def _classify_vix(v):
    if v < 18:
        return "favorable"
    if v < 25:
        return "mixed"
    if v < 32:
        return "caution"
    return "risk_off"


def breadth(data):
    """Доля ликвидных акций выше SMA50 и SMA20, число акций у максимумов/минимумов 52 недель, VIX."""
    n = a50 = a20 = hi = lo = 0
    for t, d in data.items():
        if t in ("SPY", "^VIX") or len(d) < 200:
            continue
        c = d["Close"]
        if (c * d["Volume"]).iloc[-20:].mean() < sc.MIN_DOLLAR_VOL:
            continue
        n += 1
        last = c.iloc[-1]
        a50 += last > c.rolling(50).mean().iloc[-1]
        a20 += last > c.rolling(20).mean().iloc[-1]
        w = d.tail(252)
        if last >= w["High"].max() * 0.98:
            hi += 1
        elif last <= w["Low"].min() * 1.02:
            lo += 1
    if n < 100:
        return None
    vix = None
    v = data.get("^VIX")
    if v is not None and len(v):
        vix = float(v["Close"].iloc[-1])
    return dict(n=n, pct50=a50 / n * 100, pct20=a20 / n * 100, hi=hi, lo=lo,
                hl=(hi / lo if lo > 0 else float(hi)), vix=vix)


def market_state(data):
    spy = data.get("SPY")
    spy_state = "n/a"
    spy_up = True
    if spy is not None and len(spy) >= 25:
        c = spy["Close"]
        spy_up = bool(c.rolling(10).mean().iloc[-1] > c.rolling(20).mean().iloc[-1])
        spy_state = "up" if spy_up else "down"
    b = breadth(data)
    regime = None
    if b:
        ind = [_classify(b["pct50"], BREADTH_TH["pct50"]), _classify(b["pct20"], BREADTH_TH["pct20"])]
        if b["hi"] + b["lo"] > 0:
            ind.append(_classify(b["hl"], BREADTH_TH["hl"]))
        if b["vix"] is not None:
            ind.append(_classify_vix(b["vix"]))
        regime = REGIME_ORDER[min(REGIME_ORDER.index(x) for x in ind)]
    if not MARKET_FILTER:
        allowed, mult = True, 1.0
    else:
        allowed = spy_up and regime in (None, "favorable", "mixed")
        mult = {"favorable": 1.0, "mixed": 0.5}.get(regime, 1.0 if regime is None else 0.0)
    return dict(allowed=allowed, spy=spy_state, regime=regime or "n/a", risk_mult=mult,
                pct50=b["pct50"] if b else None, pct20=b["pct20"] if b else None,
                hl=b["hl"] if b else None, vix=b["vix"] if b else None)


# ---------------------------------------------------------------- инфраструктура
def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def q(conn, sql):
    cur = conn.execute(sql)
    cols = [c[0] for c in cur.description]
    return pd.DataFrame([tuple(r) for r in cur.fetchall()], columns=cols)


def get_meta(conn, k, default=None):
    r = conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return r["v"] if r else default


def set_meta(conn, k, v):
    conn.execute("INSERT INTO meta(k,v) VALUES(?,?) "
                 "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))


def get_cash(conn):
    return float(get_meta(conn, "cash", START_EQUITY))


def set_cash(conn, x):
    set_meta(conn, "cash", x)


def post(text):
    print(text, flush=True)
    if not WEBHOOK:
        return
    for i in range(0, len(text), 1900):
        for _ in range(3):
            try:
                r = requests.post(WEBHOOK, json={"content": text[i:i + 1900]}, timeout=20)
                if r.status_code == 429:
                    wait = 1.0
                    try:
                        wait = float(r.json().get("retry_after", 1.0))
                    except Exception:
                        pass
                    time.sleep(wait + 0.2)
                    continue
                break
            except Exception as e:
                print("discord:", e, file=sys.stderr)
                time.sleep(1)
        time.sleep(0.6)


def post_file(path, text):
    print(text, flush=True)
    if not WEBHOOK:
        return
    for _ in range(3):
        try:
            with open(path, "rb") as f:
                r = requests.post(WEBHOOK, data={"content": text},
                                  files={"file": (os.path.basename(path), f, "application/pdf")},
                                  timeout=60)
            if r.status_code == 429:
                wait = 1.0
                try:
                    wait = float(r.json().get("retry_after", 1.0))
                except Exception:
                    pass
                time.sleep(wait + 0.2)
                continue
            if r.status_code >= 300:
                print("discord file:", r.status_code, r.text[:200], file=sys.stderr)
            break
        except Exception as e:
            print("discord file:", e, file=sys.stderr)
            time.sleep(1)


def prep(d):
    d = d.copy()
    d["date"] = d.index.strftime("%Y-%m-%d")
    d["sma10"] = d["Close"].rolling(10).mean()
    d["sma20"] = d["Close"].rolling(20).mean()
    return d.reset_index(drop=True)


def adr_of(d):
    return ((d["High"] / d["Low"]).iloc[-20:].mean() - 1) * 100


def equity_now(conn, P):
    cash = get_cash(conn)
    val = 0.0
    for t in conn.execute("SELECT * FROM trades WHERE status='open'").fetchall():
        d = P.get(t["ticker"])
        px = float(d["Close"].iloc[-1]) if d is not None else t["entry_price"]
        val += t["shares"] * px
    return cash + val, cash, val


def log_event(conn, tid, date, typ, price, shares, pnl, note=""):
    conn.execute("INSERT INTO events(trade_id,date,type,price,shares,pnl,note) "
                 "VALUES(?,?,?,?,?,?,?)", (tid, date, typ, price, shares, pnl, note))


# ---------------------------------------------------------------- отчётность EP (справочно)
def ep_fundamentals(ticker):
    """EPS-сюрприз последнего отчёта и рост EPS и выручки г/г по данным yfinance.
    Только справочно: что не получилось получить, просто отсутствует."""
    out = {}
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        try:
            ed = t.get_earnings_dates(limit=8)
            ed = ed.dropna(subset=["Reported EPS"]).sort_index(ascending=False)
            if len(ed):
                s = ed["Surprise(%)"].iloc[0]
                if pd.notna(s):
                    out["eps_surprise_pct"] = round(float(s), 1)
                rep = ed["Reported EPS"]
                if len(rep) >= 5 and rep.iloc[4] != 0:
                    out["eps_yoy_pct"] = round(float((rep.iloc[0] - rep.iloc[4]) / abs(rep.iloc[4]) * 100), 1)
        except Exception:
            pass
        try:
            rev = t.quarterly_income_stmt.loc["Total Revenue"].dropna().sort_index(ascending=False)
            if len(rev) >= 5 and rev.iloc[4] != 0:
                out["rev_yoy_pct"] = round(float((rev.iloc[0] / rev.iloc[4] - 1) * 100), 1)
        except Exception:
            pass
    except Exception:
        pass
    return out


# ---------------------------------------------------------------- сделки
def try_enter(conn, P, ticker, setup, date, entry_raw, stop, adr, features, risk_mult=1.0):
    entry = entry_raw * (1 + SLIPPAGE_PCT / 100)
    risk_ps = entry - stop
    if risk_ps <= 0 or risk_ps / entry * 100 > adr or risk_mult <= 0:
        return None
    n_open = conn.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
    if n_open >= MAX_POSITIONS:
        return None
    eq, cash, _ = equity_now(conn, P)
    shares = int(eq * RISK_PCT / 100 * risk_mult / risk_ps)
    shares = min(shares, int(eq * MAX_POS_PCT / 100 / entry), int(cash / entry))
    if shares < 1:
        return None
    risk_usd = shares * risk_ps
    feats = dict(features, setup=setup, equity=round(eq, 2), entry_date=date,
                 risk_pct=round(risk_usd / eq * 100, 3), risk_mult=risk_mult)
    cur = conn.execute(
        "INSERT INTO trades(ticker,setup,status,entry_date,entry_price,shares_init,shares,"
        "stop,stop_init,risk_usd,last_date,last_price,features) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (ticker, setup, "open", date, entry, shares, shares, stop, stop, risk_usd, date,
         entry, json.dumps(feats, default=float, ensure_ascii=False)))
    tid = cur.lastrowid
    set_cash(conn, cash - shares * entry)
    log_event(conn, tid, date, "OPEN", entry, shares, 0.0, setup)
    post(f"Открыта {ticker} [{setup}] {date}: {shares} шт по {entry:.2f}, стоп {stop:.2f}, "
         f"риск ${risk_usd:.0f} ({risk_usd / eq * 100:.2f}% счёта"
         f"{'' if risk_mult == 1.0 else f', режим рынка x{risk_mult:g}'}), "
         f"позиция {shares * entry / eq * 100:.1f}% счёта, ADR {adr:.1f}%")
    return tid


def close_trade(conn, tid, date, price_raw, reason):
    t = dict(conn.execute("SELECT * FROM trades WHERE id=?", (tid,)).fetchone())
    px = price_raw * (1 - SLIPPAGE_PCT / 100)
    total = t["realized"] + (px - t["entry_price"]) * t["shares"]
    r = total / t["risk_usd"] if t["risk_usd"] else 0.0
    set_cash(conn, get_cash(conn) + t["shares"] * px)
    conn.execute(
        "UPDATE trades SET status='closed', exit_date=?, exit_price=?, exit_reason=?, pnl=?, "
        "r_mult=?, realized=?, shares=0, last_date=?, last_price=? WHERE id=?",
        (date, px, reason, total, r, total, date, px, tid))
    log_event(conn, tid, date, "CLOSE", px, t["shares"], total, reason)
    post(f"Закрыта {t['ticker']} [{t['setup']}] {date}: {REASONS.get(reason, reason)}, "
         f"выход {px:.2f} (вход {t['entry_price']:.2f}), итог {'+' if total >= 0 else '-'}"
         f"${abs(total):.2f}, R={r:.2f}, держали {t['days_held']} дн.")


def partial_exit(conn, tid, date, price_raw):
    t = dict(conn.execute("SELECT * FROM trades WHERE id=?", (tid,)).fetchone())
    n = int(t["shares"] * PARTIAL_FRAC)
    if n < 1 or n >= t["shares"]:
        conn.execute("UPDATE trades SET partial_done=1, stop=? WHERE id=?",
                     (t["entry_price"], tid))
        return
    px = price_raw * (1 - SLIPPAGE_PCT / 100)
    pnl = (px - t["entry_price"]) * n
    set_cash(conn, get_cash(conn) + n * px)
    conn.execute("UPDATE trades SET shares=shares-?, realized=realized+?, partial_done=1, "
                 "stop=? WHERE id=?", (n, pnl, t["entry_price"], tid))
    log_event(conn, tid, date, "PARTIAL", px, n, pnl, "")
    post(f"Частичная фиксация {t['ticker']} {date}: продано {n} шт по {px:.2f}, "
         f"{'+' if pnl >= 0 else '-'}${abs(pnl):.2f}, стоп перенесён в безубыток ({t['entry_price']:.2f})")


def manage(conn, P):
    for t0 in conn.execute("SELECT * FROM trades WHERE status='open'").fetchall():
        d = P.get(t0["ticker"])
        if d is None:
            continue
        for b in d[d["date"] > t0["last_date"]].itertuples():
            t = dict(conn.execute("SELECT * FROM trades WHERE id=?", (t0["id"],)).fetchone())
            if t["status"] != "open":
                break
            days = t["days_held"] + 1
            conn.execute("UPDATE trades SET days_held=?, last_date=? WHERE id=?",
                         (days, b.date, t["id"]))
            if b.Low <= t["stop"]:
                px = b.Open if b.Open < t["stop"] else t["stop"]
                close_trade(conn, t["id"], b.date, px,
                            "stop_be" if t["partial_done"] else "stop")
                break
            if not t["partial_done"] and days >= PARTIAL_DAYS:
                partial_exit(conn, t["id"], b.date, b.Close)
            ma = b.sma10 if t["setup"] == "breakout" else b.sma20
            if pd.notna(ma) and b.Close < ma:
                close_trade(conn, t["id"], b.date, b.Close,
                            "trail_sma10" if t["setup"] == "breakout" else "trail_sma20")
                break


def has_ticker(conn, ticker):
    return conn.execute("SELECT 1 FROM trades WHERE ticker=? AND status='open'",
                        (ticker,)).fetchone() is not None


def process_pending(conn, P, mkt):
    for p in conn.execute("SELECT * FROM pending").fetchall():
        d = P.get(p["ticker"])
        if d is None:
            conn.execute("DELETE FROM pending WHERE id=?", (p["id"],))
            continue
        bars = d[d["date"] > p["created"]]
        if bars.empty:
            continue
        b = bars.iloc[0]
        conn.execute("DELETE FROM pending WHERE id=?", (p["id"],))
        trig = p["trigger_price"]
        if not mkt["allowed"] or has_ticker(conn, p["ticker"]) or b["High"] < trig:
            continue
        entry_raw = max(float(b["Open"]), trig)
        if entry_raw > trig * (1 + MAX_CHASE_PCT / 100):
            continue
        feats = json.loads(p["features"])
        tid = try_enter(conn, P, p["ticker"], p["setup"], b["date"], entry_raw,
                        float(b["Low"]), p["adr"], feats, mkt["risk_mult"])
        if tid and b["Close"] < trig:
            close_trade(conn, tid, b["date"], float(b["Close"]), "stop_entry_day")


def save_candidates(conn, today, breakouts, eps, para):
    conn.execute("DELETE FROM candidates WHERE date=?", (today,))
    rows = []
    for b in breakouts:
        rows.append((today, b["ticker"], "breakout", b["status"], b["score"],
                     json.dumps(b, default=float, ensure_ascii=False)))
    for e in eps:
        rows.append((today, e["ticker"], "ep", "gap", e["score"],
                     json.dumps(e, default=float, ensure_ascii=False)))
    for p in para:
        rows.append((today, p["ticker"], "parabolic_" + p["side"], "watch", None,
                     json.dumps(p, default=float, ensure_ascii=False)))
    conn.executemany("INSERT INTO candidates(date,ticker,setup,status,score,features) "
                     "VALUES(?,?,?,?,?,?)", rows)


def process_scan(conn, P, data, mkt, today):
    scan_data = {k: v for k, v in data.items() if k not in ("SPY", "^VIX")}
    breakouts, eps = sc.analyze(scan_data)
    para = sc.find_parabolic(scan_data)
    conn.execute("DELETE FROM pending WHERE created=?", (today,))

    for e in eps:
        e["fund"] = ep_fundamentals(e["ticker"]) if EP_FUNDAMENTALS else {}
    save_candidates(conn, today, breakouts, eps, para)

    market = dict(market=mkt["regime"], spy=mkt["spy"])
    for b in breakouts:
        t = b["ticker"]
        if has_ticker(conn, t):
            continue
        feats = dict(b, **market)
        if b["status"] == "BREAKOUT":
            if not mkt["allowed"]:
                continue
            d = P[t]
            bar = d.iloc[-1]
            if bar["date"] != today or bar["High"] < b["trigger"]:
                continue
            entry_raw = max(float(bar["Open"]), b["trigger"])
            if entry_raw > b["trigger"] * (1 + MAX_CHASE_PCT / 100):
                continue
            try_enter(conn, P, t, "breakout", today, entry_raw, float(bar["Low"]),
                      b["adr"], feats, mkt["risk_mult"])
        elif mkt["allowed"]:
            conn.execute(
                "INSERT INTO pending(created,ticker,setup,trigger_price,adr,features) "
                "VALUES(?,?,?,?,?,?)",
                (today, t, "breakout", b["trigger"], b["adr"],
                 json.dumps(feats, default=float, ensure_ascii=False)))

    if mkt["allowed"]:
        for e in eps:
            t = e["ticker"]
            if e["date"] != today or has_ticker(conn, t):
                continue
            feats = dict(e, **market)
            try_enter(conn, P, t, "ep", today, float(P[t]["Close"].iloc[-1]),
                      e["low_of_gap_day"], adr_of(scan_data[t]), feats, mkt["risk_mult"])
    return breakouts, eps, para


def snapshot(conn, P, today):
    eq, cash, val = equity_now(conn, P)
    for t in conn.execute("SELECT * FROM trades WHERE status='open'").fetchall():
        d = P.get(t["ticker"])
        if d is not None:
            conn.execute("UPDATE trades SET last_price=? WHERE id=?",
                         (float(d["Close"].iloc[-1]), t["id"]))
    conn.execute("INSERT OR REPLACE INTO equity(date,equity,cash,open_value) VALUES(?,?,?,?)",
                 (today, eq, cash, val))
    return eq, cash, val


def export_csv(conn):
    os.makedirs(OUT_DIR, exist_ok=True)
    q(conn, "SELECT * FROM trades ORDER BY id").to_csv(
        os.path.join(OUT_DIR, "trades_all.csv"), index=False)
    q(conn, "SELECT * FROM events ORDER BY id").to_csv(
        os.path.join(OUT_DIR, "events_all.csv"), index=False)
    q(conn, "SELECT * FROM candidates ORDER BY id").to_csv(
        os.path.join(OUT_DIR, "candidates_all.csv"), index=False)


def brief(today, mkt, breakouts, eps, para, eq, n_new, n_open):
    f = lambda x, p=0: "n/a" if x is None else f"{x:.{p}f}"  # noqa: E731
    lines = [f"Скан {today}. Рынок: {mkt['regime']} (SPY {mkt['spy']}, выше SMA50 {f(mkt['pct50'])}%, "
             f"выше SMA20 {f(mkt['pct20'])}%, макс/мин {f(mkt['hl'], 1)}, VIX {f(mkt['vix'], 1)}); "
             f"риск x{mkt['risk_mult']:g}{'' if mkt['allowed'] else ', новые сделки выключены'}"]
    nb = sum(1 for b in breakouts if b["status"] == "BREAKOUT")
    lines.append(f"Кандидаты: breakout {len(breakouts)} (пробой {nb}, у уровня {len(breakouts) - nb}), "
                 f"EP {len(eps)}, парабола short {sum(1 for p in para if p['side'] == 'short')} / "
                 f"long {sum(1 for p in para if p['side'] == 'long')}")
    for b in breakouts[:5]:
        lines.append(f"  {b['ticker']} {b['status']} score {b['score']} 2LYNCH {b['lynch'] or '-'} "
                     f"триггер {b['trigger']} ADR {b['adr']}%")
    for e in eps[:5]:
        fd = e.get("fund") or {}
        extra = "".join([f", EPS сюрприз {fd['eps_surprise_pct']:+.0f}%" if "eps_surprise_pct" in fd else "",
                         f", EPS г/г {fd['eps_yoy_pct']:+.0f}%" if "eps_yoy_pct" in fd else "",
                         f", выручка г/г {fd['rev_yoy_pct']:+.0f}%" if "rev_yoy_pct" in fd else ""])
        lines.append(f"  EP {e['ticker']} {e['date']} score {e['score']} гэп +{e['gap']}% "
                     f"объём x{e['vol_x']}{extra}")
    for p in para[:5]:
        lines.append(f"  парабола {p['side']} {p['ticker']} {p['move10']:+d}% за {sc.PB_LOOKBACK} дн")
    lines.append(f"Открыто сегодня: {n_new}. Открытых позиций: {n_open}. Счёт ${eq:,.2f}")
    return "\n".join(lines)


def run_daily(data):
    P = {t: prep(d) for t, d in data.items() if len(d) >= 30}
    if not P:
        print("нет данных")
        return
    today = max(d["date"].iloc[-1] for d in P.values())
    mkt = market_state(data)
    conn = connect()
    try:
        process_pending(conn, P, mkt)
        conn.commit()
        manage(conn, P)
        conn.commit()
        scanned = None
        if get_meta(conn, "last_scan") != today:
            scanned = process_scan(conn, P, data, mkt, today)
            set_meta(conn, "last_scan", today)
            conn.commit()
        eq, cash, val = snapshot(conn, P, today)
        conn.commit()
        export_csv(conn)
        print(f"[{today}] рынок {mkt['regime']} (SPY {mkt['spy']}), счёт {eq:.2f} "
              f"(кэш {cash:.2f}, позиции {val:.2f})")
        if scanned:
            n_new = conn.execute("SELECT COUNT(*) FROM trades WHERE entry_date=?",
                                 (today,)).fetchone()[0]
            n_open = conn.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
            post(brief(today, mkt, *scanned, eq, n_new, n_open))
    finally:
        conn.close()


# ---------------------------------------------------------------- отчёты
def summary(conn):
    closed = q(conn, "SELECT * FROM trades WHERE status='closed' ORDER BY exit_date, id")
    opened = q(conn, "SELECT * FROM trades WHERE status='open' ORDER BY entry_date, id")
    eq = q(conn, "SELECT * FROM equity ORDER BY date")
    last_eq = float(eq["equity"].iloc[-1]) if len(eq) else START_EQUITY
    last_date = eq["date"].iloc[-1] if len(eq) else datetime.now().strftime("%Y-%m-%d")
    week_start = (datetime.strptime(last_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    prev = eq[eq["date"] <= week_start]
    prev_eq = float(prev["equity"].iloc[-1]) if len(prev) else START_EQUITY
    realized_closed = float(closed["pnl"].sum()) if len(closed) else 0.0
    realized_open = float(opened["realized"].sum()) if len(opened) else 0.0
    total = last_eq - START_EQUITY
    return dict(closed=closed, opened=opened, eq=eq, last_eq=last_eq, last_date=last_date,
                week=last_eq - prev_eq, total=total, realized_closed=realized_closed,
                realized_open=realized_open,
                unrealized=total - realized_closed - realized_open)


def stats_row(label, df):
    n = len(df)
    if n == 0:
        return [label, "0", "-", "-", "-", "-", "-", "0.00"]
    w = df[df["pnl"] > 0]
    l = df[df["pnl"] <= 0]
    gp, gl = float(w["pnl"].sum()), -float(l["pnl"].sum())
    pf = f"{gp / gl:.2f}" if gl > 0 else "n/a"
    return [label, str(n), f"{len(w) / n * 100:.0f}%",
            f"{w['r_mult'].mean():.2f}" if len(w) else "-",
            f"{l['r_mult'].mean():.2f}" if len(l) else "-",
            f"{df['r_mult'].mean():.2f}", pf, f"{df['pnl'].sum():,.2f}"]


def build_pdf(conn, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    font = os.path.join(matplotlib.get_data_path(), "fonts", "ttf", "DejaVuSans.ttf")
    pdfmetrics.registerFont(TTFont("DejaVu", font))
    h1 = ParagraphStyle("h1", fontName="DejaVu", fontSize=16, leading=20, spaceAfter=6)
    h2 = ParagraphStyle("h2", fontName="DejaVu", fontSize=12, leading=15, spaceBefore=10, spaceAfter=4)
    tx = ParagraphStyle("tx", fontName="DejaVu", fontSize=9, leading=12)
    green, red = colors.HexColor("#15803D"), colors.HexColor("#B91C1C")

    def table(data, widths, pnl_cols=()):
        t = Table(data, colWidths=[w * mm for w in widths], repeatRows=1)
        style = [
            ("FONTNAME", (0, 0), (-1, -1), "DejaVu"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E5E7EB")),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#9CA3AF")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F9FAFB")]),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]
        for i, row in enumerate(data[1:], start=1):
            for c in pnl_cols:
                try:
                    v = float(str(row[c]).replace(",", ""))
                    style.append(("TEXTCOLOR", (c, i), (c, i), green if v >= 0 else red))
                except ValueError:
                    pass
        t.setStyle(TableStyle(style))
        return t

    s = summary(conn)
    closed, opened, eq = s["closed"], s["opened"], s["eq"]
    sign = lambda x: f"{'+' if x >= 0 else '-'}${abs(x):,.2f}"  # noqa: E731

    story = [Paragraph(f"Виртуальный счёт: недельный отчёт на {s['last_date']}", h1)]
    story.append(Paragraph(
        f"Стартовый счёт: ${START_EQUITY:,.2f}. Текущий счёт: ${s['last_eq']:,.2f}. "
        f"Общий PnL: {sign(s['total'])} ({s['total'] / START_EQUITY * 100:+.2f}%). "
        f"За неделю: {sign(s['week'])}.", tx))
    story.append(Paragraph(
        f"Реализовано по закрытым сделкам: {sign(s['realized_closed'])}. "
        f"Зафиксировано частично в открытых: {sign(s['realized_open'])}. "
        f"Нереализованный результат открытых позиций: {sign(s['unrealized'])}. "
        f"Открытых позиций: {len(opened)}, закрытых сделок: {len(closed)}.", tx))

    if len(eq) >= 2:
        fig, ax = plt.subplots(figsize=(10, 2.6))
        ax.plot(pd.to_datetime(eq["date"]), eq["equity"], color="#1D4ED8", linewidth=1.6)
        ax.axhline(START_EQUITY, color="#9CA3AF", linewidth=0.8, linestyle="--")
        ax.set_title("Equity")
        ax.grid(alpha=0.25)
        fig.autofmt_xdate()
        fig.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150)
        plt.close(fig)
        buf.seek(0)
        story += [Spacer(1, 6), Image(buf, width=250 * mm, height=250 * mm * 2.6 / 10)]

    story.append(Paragraph("Статистика по закрытым сделкам", h2))
    rows = [["Группа", "Сделок", "Win rate", "Ср. R выигр.", "Ср. R проигр.",
             "Ожидание R", "Profit factor", "PnL $"], stats_row("Все", closed)]
    for setup in sorted(closed["setup"].unique()) if len(closed) else []:
        rows.append(stats_row(setup, closed[closed["setup"] == setup]))
    story.append(table(rows, [30, 20, 22, 28, 28, 26, 26, 28], pnl_cols=(7,)))

    story.append(Paragraph("Открытые позиции", h2))
    if len(opened):
        rows = [["#", "Тикер", "Сетап", "Вход", "Цена вх.", "Акций", "Стоп", "Посл. цена",
                 "Нереал. PnL $", "Зафикс. $"]]
        for r in opened.itertuples():
            last = r.last_price if pd.notna(r.last_price) else r.entry_price
            rows.append([r.id, r.ticker, r.setup, r.entry_date, f"{r.entry_price:.2f}", r.shares,
                         f"{r.stop:.2f}", f"{last:.2f}",
                         f"{(last - r.entry_price) * r.shares:,.2f}", f"{r.realized:,.2f}"])
        story.append(table(rows, [10, 22, 22, 26, 22, 18, 22, 24, 30, 26], pnl_cols=(8, 9)))
    else:
        story.append(Paragraph("Открытых позиций нет.", tx))

    story.append(Paragraph("Все закрытые сделки", h2))
    if len(closed):
        rows = [["#", "Тикер", "Сетап", "Вход", "Цена вх.", "Выход", "Цена вых.", "Акций",
                 "PnL $", "R", "Причина"]]
        for r in closed.itertuples():
            rows.append([r.id, r.ticker, r.setup, r.entry_date, f"{r.entry_price:.2f}",
                         r.exit_date, f"{r.exit_price:.2f}", r.shares_init,
                         f"{r.pnl:,.2f}", f"{r.r_mult:.2f}", REASONS.get(r.exit_reason, r.exit_reason)])
        story.append(table(rows, [10, 20, 22, 24, 22, 24, 22, 16, 26, 14, 52], pnl_cols=(8, 9)))
    else:
        story.append(Paragraph("Закрытых сделок пока нет.", tx))

    doc = SimpleDocTemplate(path, pagesize=landscape(A4), leftMargin=12 * mm, rightMargin=12 * mm,
                            topMargin=12 * mm, bottomMargin=12 * mm, title="Paper trading report")
    doc.build(story)


def cmd_daily(force=False):
    now = datetime.now(timezone.utc)
    et_today = now.astimezone(ET).date()
    if not force and not is_session(et_today):
        print(f"{et_today}: биржа закрыта, пропуск (для принудительного запуска: daily force)")
        return
    conn = connect()
    open_t = [r["ticker"] for r in conn.execute(
        "SELECT ticker FROM trades WHERE status='open'").fetchall()]
    conn.close()
    need = sorted(set(sc.get_universe()) | set(open_t) | {"SPY", "^VIX"})
    ok, msg, exp = False, "", None
    data = {}
    for attempt in range(1, PREFLIGHT_ATTEMPTS + 1):
        data = sc.load(need)
        ok, msg, exp = preflight(data, len(need))
        print(f"проверка данных, попытка {attempt}/{PREFLIGHT_ATTEMPTS}: "
              f"{'ок' if ok else 'не пройдена'}: {msg}", file=sys.stderr)
        if ok:
            break
        if attempt < PREFLIGHT_ATTEMPTS:
            time.sleep(PREFLIGHT_WAIT_S)
    if not ok:
        post(f"Данные не прошли проверку ({msg}). Сессия {exp} не обработана, "
             f"сделок и скана сегодня нет. Повторить вручную: python3 paper_trader.py daily")
        sys.exit(2)
    print(f"загружено тикеров: {len(data)} ({msg})", file=sys.stderr)
    run_daily(data)


def cmd_weekly():
    conn = connect()
    try:
        s = summary(conn)
        os.makedirs(OUT_DIR, exist_ok=True)
        path = os.path.join(OUT_DIR, f"weekly_{s['last_date'].replace('-', '')}.pdf")
        build_pdf(conn, path)
    finally:
        conn.close()
    sg = lambda x: f"{'+' if x >= 0 else '-'}${abs(x):,.2f}"  # noqa: E731
    post_file(path, f"Недельный отчёт {s['last_date']}: счёт ${s['last_eq']:,.2f}, "
                    f"за неделю {sg(s['week'])}, всего {sg(s['total'])}, "
                    f"закрытых сделок {len(s['closed'])}, открытых {len(s['opened'])}")
    print("PDF:", path)


def cmd_status():
    conn = connect()
    try:
        s = summary(conn)
        print(f"счёт {s['last_eq']:.2f} на {s['last_date']}, всего {s['total']:+.2f}, "
              f"за неделю {s['week']:+.2f}")
        print(s["opened"][["id", "ticker", "setup", "entry_date", "entry_price", "shares",
                           "stop", "last_price"]].to_string(index=False)
              if len(s["opened"]) else "открытых позиций нет")
        print(f"закрытых сделок: {len(s['closed'])}")
    finally:
        conn.close()


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "daily"
    if cmd == "daily":
        cmd_daily(force=len(sys.argv) > 2 and sys.argv[2] == "force")
    elif cmd == "weekly":
        cmd_weekly()
    elif cmd == "status":
        cmd_status()
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
