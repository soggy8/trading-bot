# trading-bot

Paper-only pump.fun launch sniper plus tools to backtest it. No wallets, no keys, no real orders.

| file | what it does |
|---|---|
| `yolo_snipe.py` | Live paper sniper: watches PumpPortal for new launches, applies entry filters, simulates fills (fees, delay, price impact) and exits. Uses `yolo_live_watch.py` for the ledger. |
| `yolo_live_watch.py` | Ledger helpers + 15 s watcher for open `yolo-meme` trades (DexScreener prices). Data folder: `YOLO_DESK` env, default `/workspace/trading-desk`. The ledger CSV is private and not in the repo. |
| `analyze_snipe.py` | Summarises a dry-run / backtest jsonl: win rate, EV, exit reasons, per-filter near-miss check. |
| `fetch_history.py` | Downloads pump.fun launches and their first 12.5 min of trades into `data/history/`. The listing only reaches ~1 h back, so run it for hours to build a sample. |
| `snipectl.py` | Control script for the server job: `status`, `start [hours]`, `stop`, `backtest [args]`, `results`, `log`. Sends Telegram messages via Hermes. Service file and Hermes skill in `deploy/`. |
| `backtest_snipe.py` | Replays the downloaded launches through `yolo_snipe.py`'s own filter, exit and fill code on a simulated clock. Skips non-SOL-quoted and mayhem-mode coins (their curves can't be modelled). |

```bash
pip install -r requirements.txt
nohup python3 fetch_history.py --hours 24 > data/fetch.log 2>&1 &   # collect history
python3 backtest_snipe.py                      # results + ledger sim
python3 backtest_snipe.py --no-costs           # same, without fees/slippage
python3 backtest_snipe.py --set min_buyers=6   # try a filter change
python3 analyze_snipe.py data/backtest-dryrun.jsonl
```
