# Qullamaggie paper trading

Daily scanner and virtual (paper) account for Qullamaggie-style setups on US stocks:
Breakout, Episodic Pivot and parabolic moves, on daily candles. Every trade is stored in a
SQLite journal, posted to Discord, and a weekly PDF with all trades and total PnL is sent
to Discord. Nothing is traded with real money.

- `qulla_scanner.py` finds setups after the US market close (data: yfinance), scores them 0-100
- `paper_trader.py` runs the virtual account on top of the scanner: data checks, market regime
  filter (SPY SMA10/SMA20 and market breadth), entries, stops, exits, journal, reports

Scoring and checks are rewritten ideas from the MIT projects
axidzz/Qullamaggie-Setups and VladPetrariu/Qullamaggie-breakout-scanner.

## Install

```
pip install -r requirements.txt
cp qulla.env.example qulla.env
# edit qulla.env
```

## Run

```
python3 paper_trader.py daily          # after the US close: data -> checks -> scanner -> trades -> Discord
python3 paper_trader.py daily force    # ignore the "market closed today" check
python3 paper_trader.py weekly         # PDF with all trades and PnL -> Discord
python3 paper_trader.py status         # account state in the console
```

Example cron (server time, adjust to your timezone):

```
30 23 * * 1-5 cd /path/to/qulla && python3 paper_trader.py daily
0 10 * * 6 cd /path/to/qulla && python3 paper_trader.py weekly
```

## Settings

Environment variables or `qulla.env` next to the script, see `qulla.env.example`.
The scanner also accepts `tickers.txt` (one ticker per line) instead of the full US universe.

Not financial advice. This is a research and journaling tool.
