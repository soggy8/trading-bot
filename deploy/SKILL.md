---
name: trading-bot
description: Use when Andrej asks about the trading bot, pump.fun sniper, history download or backtest (status, start, stop, run a backtest, results).
---

# trading-bot (pump.fun paper sniper backtest)

Project: `/mnt/vault/trading-bot` (repo github.com/soggy8/trading-bot). Paper only, no wallets or keys.
A user systemd service `trading-bot-download.service` downloads pump.fun launch history, then runs the
backtest and sends the result to Telegram by itself.

Control everything through one script, always in exactly this form:

    PYTHONPATH=/mnt/vault/trading-bot/.deps python3 /mnt/vault/trading-bot/snipectl.py <command>

| Andrej says | command | kind |
|---|---|---|
| status / how is the download going | `status` | read-only |
| results / details of the last backtest | `results` | read-only |
| show the log | `log 30` | read-only |
| run a backtest now (works mid-download) | `backtest` | read-only on the data; runs in the background |
| backtest with a change, e.g. "try min buyers 6", "3 s delay", "no costs" | `backtest --set min_buyers=6` / `backtest --delay 3` / `backtest --no-costs` | same |
| start / collect more data (optional hours, default 24) | `start 24` | starts the user service |
| stop it | `stop` | stops the user service |

Follow the normal approval rules in AGENTS.md for `start` and `stop`.

`backtest` returns at once and the result arrives as its own Telegram message; tell Andrej that.
`--set` accepts: entry filters `min_age max_age min_buyers min_buys min_net_sol min_mc_mult max_sell_ratio
max_top_buyer_share max_mc_sol max_mc_mult min_avg_buy_sol min_mc_rising_frac`, exits `sl tp be_at trail_at trail_gap
trail_floor stale_s mom_window sell_mult time_stop_s`, costs `fee tx_sol fill_delay_s extra_slip max_slip`.
Several `--set` flags can be combined.

When reporting results: quote the numbers (trades, win %, EV per trade, ledger true P&L), keep it short, and say plainly
when the sample is small (under ~50 trades) or EV is negative. Don't edit the project's code or service without asking.
