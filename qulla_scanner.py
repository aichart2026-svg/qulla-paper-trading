#!/usr/bin/env python3
"""
Сканер сетапов Qullamaggie (Breakout, Episodic Pivot, параболы) по дневным свечам.
Запускать после закрытия рынка США. Данные: yfinance.

Идеи скоринга 0-100 и проверки 2LYNCH перенесены (переписаны) из MIT-проектов
axidzz/Qullamaggie-Setups и VladPetrariu/Qullamaggie-breakout-scanner.

Переменные окружения (необязательно):
  EQUITY=10000          размер счёта
  RISK_PCT=0.5          риск на сделку, % счёта
  TOP_PCT=2             топ-% лидеров по росту за 1/3/6 месяцев (среди ликвидных)
  UNIVERSE=all          all = все акции США (NASDAQ/NYSE/AMEX), sp500 = только S&P 500
  DISCORD_WEBHOOK=...   куда слать отчёт
  OUT_DIR=.             куда сохранять CSV

Список тикеров: файл tickers.txt (по одному в строке) имеет приоритет над UNIVERSE.
"""
import io
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import requests

EQUITY = float(os.getenv("EQUITY", "10000"))
RISK_PCT = float(os.getenv("RISK_PCT", "0.5"))
TOP_PCT = float(os.getenv("TOP_PCT", "2"))
UNIVERSE = os.getenv("UNIVERSE", "all").lower()
WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
OUT_DIR = os.getenv("OUT_DIR", ".")

MAX_POS_PCT = 30.0          # не больше 30% счёта в одной бумаге
MIN_ADR = 3.5               # минимальная средняя дневная амплитуда, %
MIN_DOLLAR_VOL = 5_000_000  # минимальный средний дневной оборот, $
MIN_PRIOR_MOVE = 30.0       # рывок за последние ~3 месяца, %
EP_GAP = 10.0               # гэп вверх, %
EP_VOL_MULT = 3.0           # объём дня гэпа к среднему за 20 дней
EP_MAX_PRIOR = 50.0         # рост за 6 мес до гэпа не больше, %
PB_LOOKBACK = 10            # парабола: окно движения, дней
PB_MIN_MOVE = 50.0          # парабола: движение за окно, %
PB_MIN_STREAK = 4           # парабола: дней подряд в одну сторону
PB_MIN_STRETCH = 30.0       # парабола: отрыв от SMA20, %
PB_LONG_WINDOW = 5          # парабола long: разворот не позже N баров после обвала
BATCH = 100

NASDAQ_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
BAD_NAME_WORDS = ("warrant", "right", "unit", "preferred", "note", "debenture",
                  "depositary share", "acquisition corp")
_HERE = os.path.dirname(os.path.abspath(__file__))
_CACHE = os.path.join(_HERE, "universe_cache.txt")


# ---------------------------------------------------------------- вселенная
def parse_symdir(nasdaq_text, other_text):
    """Обычные акции США: без ETF, тестовых бумаг, варрантов, прав, юнитов, префов."""
    syms = set()
    for text, sym_col, etf_col, test_col in ((nasdaq_text, "Symbol", "ETF", "Test Issue"),
                                             (other_text, "ACT Symbol", "ETF", "Test Issue")):
        rows = [x for x in text.splitlines() if x and not x.startswith("File Creation")]
        if not rows:
            continue
        df = pd.read_csv(io.StringIO("\n".join(rows)), sep="|", dtype=str).fillna("")
        df = df[(df[etf_col] == "N") & (df[test_col] == "N")]
        for sym, name in zip(df[sym_col], df["Security Name"]):
            low = name.lower()
            if any(w in low for w in BAD_NAME_WORDS):
                continue
            sym = sym.strip().upper().replace(".", "-")
            if sym.replace("-", "").isalpha() and len(sym) <= 6:
                syms.add(sym)
    return sorted(syms)


def sp500_universe():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    df = pd.read_html(io.StringIO(r.text))[0]
    return sorted(df["Symbol"].str.replace(".", "-", regex=False).tolist())


def all_us_universe():
    hdr = {"User-Agent": "Mozilla/5.0"}
    try:
        a = requests.get(NASDAQ_URL, headers=hdr, timeout=30)
        b = requests.get(OTHER_URL, headers=hdr, timeout=30)
        a.raise_for_status()
        b.raise_for_status()
        syms = parse_symdir(a.text, b.text)
        if len(syms) < 1000:
            raise ValueError(f"слишком короткий список: {len(syms)}")
        with open(_CACHE, "w") as f:
            f.write("\n".join(syms))
        return syms
    except Exception as e:
        print("список тикеров не скачался:", e, file=sys.stderr)
        if os.path.exists(_CACHE):
            with open(_CACHE) as f:
                return [x.strip() for x in f if x.strip()]
        print("берём S&P 500", file=sys.stderr)
        return sp500_universe()


def get_universe():
    if os.path.exists("tickers.txt"):
        with open("tickers.txt") as f:
            return sorted({x.strip().upper() for x in f
                           if x.strip() and not x.startswith("#")})
    if UNIVERSE == "sp500":
        return sp500_universe()
    return all_us_universe()


def load(tickers):
    import yfinance as yf
    data = {}
    for i in range(0, len(tickers), BATCH):
        chunk = tickers[i:i + BATCH]
        try:
            df = yf.download(chunk, period="1y", interval="1d",
                             group_by="ticker", auto_adjust=True,
                             threads=True, progress=False)
        except Exception as e:
            print("ошибка загрузки:", e, file=sys.stderr)
            continue
        if df is None or df.empty:
            continue
        for t in chunk:
            try:
                if isinstance(df.columns, pd.MultiIndex):
                    if t not in df.columns.get_level_values(0):
                        continue
                    d = df[t]
                else:
                    d = df
                d = d[["Open", "High", "Low", "Close", "Volume"]].dropna()
            except Exception:
                continue
            if len(d) >= 135:
                data[t] = d.copy()
    return data


# ---------------------------------------------------------------- скоринг и качество
def _tier_ge(v, tiers):
    if v is None or pd.isna(v):
        return 0
    for th, pts in tiers:
        if v >= th:
            return pts
    return 0


def _tier_le(v, tiers):
    if v is None or pd.isna(v):
        return 0
    for th, pts in tiers:
        if v <= th:
            return pts
    return 0


def score_breakout(ret1, ret3, ema10_rising, stacked, dist10, dist20, vol_x, pct52):
    """0-100: импульс (35) + тренд (20) + «серфинг» по EMA10/20 (25) + объём и близость к максимуму (20)."""
    momentum = (_tier_ge(ret1, [(40, 20), (25, 15), (15, 10), (5, 5)])
                + _tier_ge(ret3, [(80, 15), (50, 10), (30, 5)]))
    trend = (10 if ema10_rising else 0) + (10 if stacked else 0)
    surf = _tier_le(dist10, [(2, 20), (4, 15), (7, 10)]) + (5 if dist20 <= 5 else 0)
    vol_ext = _tier_ge(vol_x, [(2, 10), (1.3, 5)]) + _tier_ge(pct52, [(90, 10), (75, 5)])
    return int(momentum + trend + surf + vol_ext)


def score_ep(gap, vol_x, prior3, above20h, pct20h):
    """0-100: гэп (40) + объём (25) + «плоская» база до гэпа (20) + пробой 20-дневного максимума (15)."""
    gap_pts = _tier_ge(gap, [(20, 40), (10, 30), (5, 15)])
    vol_pts = _tier_ge(vol_x, [(3, 25), (2, 20), (1.5, 10)])
    base_pts = _tier_le(abs(prior3), [(10, 20), (25, 15), (40, 10)])
    brk_pts = 15 if above20h else (10 if pct20h >= 95 else 0)
    return int(gap_pts + vol_pts + base_pts + brk_pts)


def lynch_flags(d, consol_len=15, lookback=60):
    """Проверка качества пробоя 2LYNCH (Stockbee). Возвращает (строка выполненных букв, их число).
    2 не росла два дня подряд до сегодня; L линейность рывка; Y молодой тренд (SMA50);
    N узкий или красный день перед пробоем; C спокойная база с сухим объёмом; H закрытие у максимума дня."""
    o, h, l, c, v = d["Open"], d["High"], d["Low"], d["Close"], d["Volume"]
    if len(c) < lookback + 5:
        return "", 0
    f = {}
    f["2"] = not (c.iloc[-2] > c.iloc[-3] and c.iloc[-3] > c.iloc[-4])
    net = abs(c.iloc[-1] - c.iloc[-lookback])
    path = c.diff().abs().iloc[-lookback:].sum()
    f["L"] = bool(path and net / path >= 0.35)
    below = (c < c.rolling(50).mean()).values
    since = 0
    for b in below[::-1]:
        if b:
            break
        since += 1
    f["Y"] = since <= 90
    rng = h - l
    avg_rng = rng.rolling(20).mean()
    f["N"] = bool(rng.iloc[-2] < avg_rng.iloc[-2]) or bool(c.iloc[-2] < c.iloc[-3])
    pct = c.pct_change() * 100
    downs = int((pct.iloc[-consol_len - 1:-1] <= -4).sum())
    f["C"] = bool(downs <= 1 and v.iloc[-consol_len - 1:-1].mean() < v.iloc[-lookback - 1:-1].mean())
    day = h.iloc[-1] - l.iloc[-1]
    f["H"] = bool(day > 0 and (c.iloc[-1] - l.iloc[-1]) / day >= 0.7)
    s = "".join(k for k in "2LYNCH" if f[k])
    return s, len(s)


# ---------------------------------------------------------------- основной анализ
def analyze(data):
    rows = []
    for t, d in data.items():
        c, h, l, v = d["Close"], d["High"], d["Low"], d["Volume"]
        if len(c) < 135:
            continue
        last = c.iloc[-1]
        rows.append(dict(
            ticker=t,
            price=last,
            ret1=(last / c.iloc[-22] - 1) * 100,
            ret3=(last / c.iloc[-64] - 1) * 100,
            ret6=(last / c.iloc[-127] - 1) * 100,
            adr=((h / l).iloc[-20:].mean() - 1) * 100,
            dvol=(c * v).iloc[-20:].mean(),
        ))
    if not rows:
        return [], []

    m = pd.DataFrame(rows)
    m = m[m.dvol >= MIN_DOLLAR_VOL]
    if m.empty:
        return [], []

    q = 1 - TOP_PCT / 100
    thr = {k: m[k].quantile(q) for k in ("ret1", "ret3", "ret6")}
    m["leader"] = ((m.ret1 >= thr["ret1"]) | (m.ret3 >= thr["ret3"])
                   | (m.ret6 >= thr["ret6"]))

    # ---------- Breakout ----------
    breakouts = []
    for r in m[m.leader & (m.adr >= MIN_ADR)].itertuples():
        d = data[r.ticker]
        c, h, l, v = d["Close"], d["High"], d["Low"], d["Volume"]
        last = c.iloc[-1]

        runup = (h.iloc[-63:] / l.iloc[-63:].cummin()).max() * 100 - 100
        if runup < MIN_PRIOR_MOVE:
            continue

        w = d.iloc[-11:-1]            # 10 баров консолидации до сегодняшнего
        a, b = w.iloc[:5], w.iloc[5:]

        def rng(x):
            return (x["High"].max() / x["Low"].min() - 1) * 100

        if b["Low"].min() < a["Low"].min():      # нужны растущие минимумы
            continue
        if rng(b) > rng(a):                      # диапазон должен сужаться
            continue
        if rng(w) > 2.5 * r.adr:                 # консолидация не слишком рваная
            continue

        sma10 = c.rolling(10).mean().iloc[-1]
        sma20 = c.rolling(20).mean().iloc[-1]
        if not (last >= sma20 and sma10 > sma20):
            continue

        trigger = w["High"].max()
        if trigger < last <= trigger * 1.05:
            status = "BREAKOUT"
        elif trigger * 0.97 <= last <= trigger:
            status = "NEAR"
        else:
            continue

        stop_dist = trigger * r.adr / 100        # максимально допустимый стоп
        shares = int(EQUITY * RISK_PCT / 100 / stop_dist)
        shares = min(shares, int(EQUITY * MAX_POS_PCT / 100 / trigger))
        vol_ratio = v.iloc[-1] / v.iloc[-21:-1].mean()

        ema10 = c.ewm(span=10, adjust=False).mean()
        ema20 = c.ewm(span=20, adjust=False).mean()
        sma50 = c.rolling(50).mean().iloc[-1]
        ema10_rising = bool(ema10.iloc[-1] > ema10.iloc[-6])
        stacked = bool(pd.notna(sma50) and ema10.iloc[-1] > ema20.iloc[-1] > sma50)
        dist10 = abs(last / ema10.iloc[-1] - 1) * 100
        dist20 = abs(last / ema20.iloc[-1] - 1) * 100
        pct52 = last / h.iloc[-252:].max() * 100
        score = score_breakout(r.ret1, r.ret3, ema10_rising, stacked, dist10, dist20,
                               vol_ratio, pct52)
        lynch, lynch_n = lynch_flags(d)

        breakouts.append(dict(
            ticker=r.ticker, status=status, price=round(last, 2),
            trigger=round(trigger, 2), adr=round(r.adr, 1),
            runup=round(runup), ret1=round(r.ret1), ret3=round(r.ret3),
            ret6=round(r.ret6), vol_x=round(vol_ratio, 1),
            shares=shares, pos_pct=round(shares * trigger / EQUITY * 100, 1),
            sma10=round(sma10, 2), sma20=round(sma20, 2),
            score=score, lynch=lynch, lynch_n=lynch_n,
            ema_stacked=stacked, above_sma50=bool(pd.notna(sma50) and last > sma50),
            pct_of_52w_high=round(pct52, 1), dist_ema10=round(dist10, 1),
            at_1y_high=bool(last >= h.iloc[-252:-1].max()),
        ))

    # ---------- Episodic Pivot ----------
    eps = []
    for r in m.itertuples():
        d = data[r.ticker]
        o, c, h, v = d["Open"], d["Close"], d["High"], d["Volume"]
        for k in (-1, -2):
            gap = (o.iloc[k] / c.iloc[k - 1] - 1) * 100
            if gap < EP_GAP:
                continue
            vx = v.iloc[k] / v.iloc[k - 20:k].mean()
            prior = (c.iloc[k - 1] / c.iloc[k - 127] - 1) * 100
            if vx < EP_VOL_MULT or prior > EP_MAX_PRIOR:
                continue
            if c.iloc[k] < o.iloc[k]:            # гэп не удержан
                continue
            prior3 = (c.iloc[k - 1] / c.iloc[k - 64] - 1) * 100
            high20 = h.iloc[k - 20:k].max()
            score = score_ep(gap, vx, prior3, bool(c.iloc[k] > high20),
                             c.iloc[k] / high20 * 100)
            eps.append(dict(
                ticker=r.ticker, date=str(d.index[k].date()),
                gap=round(gap, 1), vol_x=round(vx, 1),
                prior6m=round(prior), price=round(c.iloc[-1], 2),
                low_of_gap_day=round(d["Low"].iloc[k], 2), score=score,
            ))
            break

    breakouts.sort(key=lambda x: (x["status"] != "BREAKOUT", -x["score"], -x["ret3"]))
    eps.sort(key=lambda x: (-x["score"], -x["vol_x"]))
    return breakouts, eps


def _streak(close, idx, up=True):
    n = 0
    i = idx
    while -i < len(close):
        cur, prev = close.iloc[i], close.iloc[i - 1]
        if (cur > prev) if up else (cur < prev):
            n += 1
            i -= 1
        else:
            break
    return n


def find_parabolic(data):
    """Параболический short (рывок 50%+, 4+ дня роста, отрыв 30%+ от SMA20)
    и long (обвал по тем же критериям и зелёный разворотный бар). Только сигналы, без сделок."""
    out = []
    for t, d in data.items():
        if len(d) < 60:
            continue
        o, h, l, c, v = d["Open"], d["High"], d["Low"], d["Close"], d["Volume"]
        if (c * v).iloc[-20:].mean() < MIN_DOLLAR_VOL or c.iloc[-1] < 2:
            continue
        adr = ((h / l).iloc[-20:].mean() - 1) * 100
        if adr < MIN_ADR:
            continue
        sma20 = c.rolling(20).mean()
        last = c.iloc[-1]

        move_up = (last / c.iloc[-1 - PB_LOOKBACK] - 1) * 100
        stretch_up = (last / sma20.iloc[-1] - 1) * 100
        st_up = _streak(c, -1, up=True)
        if move_up >= PB_MIN_MOVE and st_up >= PB_MIN_STREAK and stretch_up >= PB_MIN_STRETCH:
            out.append(dict(ticker=t, side="short", price=round(last, 2),
                            move10=int(round(move_up)), streak=int(st_up),
                            stretch=int(round(stretch_up)), adr=round(adr, 1)))
            continue

        if last > o.iloc[-1] and last > c.iloc[-2]:
            for off in range(1, PB_LONG_WINDOW + 1):
                i = -1 - off
                if -(i - PB_LOOKBACK) > len(c):
                    break
                move_dn = (1 - c.iloc[i] / c.iloc[i - PB_LOOKBACK]) * 100
                stretch_dn = (1 - c.iloc[i] / sma20.iloc[i]) * 100
                st_dn = _streak(c, i, up=False)
                if move_dn >= PB_MIN_MOVE and st_dn >= PB_MIN_STREAK and stretch_dn >= PB_MIN_STRETCH:
                    out.append(dict(ticker=t, side="long", price=round(last, 2),
                                    move10=-int(round(move_dn)), streak=int(st_dn),
                                    stretch=-int(round(stretch_dn)), adr=round(adr, 1)))
                    break
    out.sort(key=lambda x: -abs(x["move10"]))
    return out


# ---------------------------------------------------------------- отчёт
def report(breakouts, eps, para=None):
    today = datetime.now().strftime("%Y-%m-%d")
    out = [f"Qullamaggie scan {today} | счёт {EQUITY:.0f}, риск {RISK_PCT}%"]

    out.append("\nBREAKOUT (вход: максимум первой свечи открытия; стоп: минимум дня)")
    if not breakouts:
        out.append("нет кандидатов")
    for b in breakouts:
        out.append(
            f"{b['ticker']} {b['status']} score {b['score']} 2LYNCH {b['lynch'] or '-'} "
            f"цена {b['price']} триггер {b['trigger']} "
            f"ADR {b['adr']}% рывок {b['runup']}% 1м/3м/6м {b['ret1']}/{b['ret3']}/{b['ret6']}% "
            f"объём x{b['vol_x']} | акций {b['shares']} ({b['pos_pct']}% счёта)"
        )
    if breakouts:
        out.append("Выход: 1/3-1/2 через 3-5 дней, стоп в безубыток, остаток по "
                   "первому закрытию ниже 10-дневной SMA.")

    out.append("\nEPISODIC PIVOT (проверь вручную рост EPS/выручки и новость)")
    if not eps:
        out.append("нет кандидатов")
    for e in eps:
        out.append(
            f"{e['ticker']} {e['date']} score {e['score']} гэп +{e['gap']}% объём x{e['vol_x']} "
            f"рост за 6м до гэпа {e['prior6m']}% цена {e['price']} "
            f"стоп: минимум дня гэпа {e['low_of_gap_day']}"
        )

    out.append("\nПАРАБОЛЫ (только наблюдение)")
    if not para:
        out.append("нет кандидатов")
    for p in para or []:
        out.append(f"{p['ticker']} {p['side']} цена {p['price']} за {PB_LOOKBACK} дн {p['move10']:+d}% "
                   f"подряд {p['streak']} дн, отрыв от SMA20 {p['stretch']:+d}%, ADR {p['adr']}%")
    return "\n".join(out)


def discord(text):
    if not WEBHOOK:
        return
    for i in range(0, len(text), 1900):
        try:
            requests.post(WEBHOOK, json={"content": text[i:i + 1900]}, timeout=20)
        except Exception as e:
            print("discord:", e, file=sys.stderr)


def main():
    tickers = get_universe()
    print(f"тикеров: {len(tickers)}", file=sys.stderr)
    data = load(tickers)
    print(f"загружено: {len(data)}", file=sys.stderr)
    breakouts, eps = analyze(data)
    para = find_parabolic(data)

    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    pd.DataFrame(breakouts).to_csv(os.path.join(OUT_DIR, f"breakouts_{stamp}.csv"), index=False)
    pd.DataFrame(eps).to_csv(os.path.join(OUT_DIR, f"ep_{stamp}.csv"), index=False)
    pd.DataFrame(para).to_csv(os.path.join(OUT_DIR, f"parabolic_{stamp}.csv"), index=False)

    text = report(breakouts, eps, para)
    print(text)
    discord(text)


if __name__ == "__main__":
    main()
