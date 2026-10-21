#!/usr/bin/env python3
"""
backtest_snipe.py - replay downloaded pump.fun launches (fetch_history.py) through yolo_snipe.py's own
filter, exit and paper-fill code on a simulated clock.
  python3 backtest_snipe.py [--data data/history] [--delay 1.5] [--detect 1.0] [--no-costs] [--quiet] [--set min_buyers=5 ...]
Writes data/backtest-dryrun.jsonl in the same format as the live dry run, so analyze_snipe.py works on it too.

Model: the bot sees each trade `detect` s after its block time (websocket latency), decides on that view,
and fills `delay` s later on the true curve state at that moment (from the trade prices).
Curve state per trade is rebuilt from priceSol with the coin's constant product k.
"""
import bisect, json, os, sys, types
from datetime import datetime
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
try:
    import yolo_live_watch  # noqa: F401  (real module when present)
except ImportError:          # backtest needs none of the ledger io: stand in a minimal module
    import tempfile
    stub = types.ModuleType("yolo_live_watch"); stub.DESK = tempfile.mkdtemp(prefix="snipe-bt-"); stub.TZ = ZoneInfo("Europe/Skopje")
    stub.ts = lambda t=None: (t or datetime.now(stub.TZ)).strftime("%Y-%m-%d %H:%M:%S")
    sys.modules["yolo_live_watch"] = stub
import yolo_snipe as Y

args = sys.argv[1:]
def opt(name, default):
    return type(default)(args[args.index(name) + 1]) if name in args else default
DATA = opt("--data", os.path.join(HERE, "data", "history"))
OUTF = opt("--out", os.path.join(HERE, "data", "backtest-dryrun.jsonl"))
DETECT = opt("--detect", 1.0)
Y.COST["fill_delay_s"] = opt("--delay", Y.COST["fill_delay_s"])
if "--no-costs" in args: Y.COST.update(fee=0.0, tx_sol=0.0, extra_slip=0.0)
QUIET = "--quiet" in args
for i, a in enumerate(args):   # --set min_buyers=5 --set sl=-0.3 : override any F / X / COST value
    if a == "--set":
        k, v = args[i + 1].split("=")
        d = next(d for d in (Y.F, Y.X, Y.COST) if k in d); d[k] = type(d[k])(float(v))
SOL_USD = opt("--sol", 150.0)
TZ = ZoneInfo("Europe/Skopje")

class Clock:
    t = 0.0
    def time(self): return self.t
CLK = Clock(); Y.time = CLK; Y.S.ws_up = True; Y.S.sol_usd = SOL_USD

def iso(s): return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()

SOL_QUOTE = (None, "11111111111111111111111111111111", "So11111111111111111111111111111111111111112")
SKIPPED = {}
def fit_k(meta, trades):
    """Curve constant from the coin's own buys: tokens = sqrt(k) * (1/sqrt(p0) - 1/sqrt(p1)).
    Returns None when the fills don't follow one constant-product curve (mayhem-mode coins), so they can't be simulated."""
    est = []
    for a, b in zip(trades, trades[1:]):
        if b.get("program") != "pump" or b["type"] != "buy": continue
        try: p0, p1, tok = float(a["priceSol"]), float(b["priceSol"]), float(b["baseAmount"])
        except Exception: continue
        if p1 > p0 * 1.0001 and tok > 0: est.append((tok / (p0 ** -0.5 - p1 ** -0.5)) ** 2)
    listed = (meta["virtual_sol_reserves"] / 1e9) * (meta["virtual_token_reserves"] / 1e6)
    if len(est) < 3: return listed if not meta.get("mayhem_state") else None
    est.sort(); k = est[len(est) // 2]
    return k if est[len(est) // 10] > 0.97 * k and est[-1 - len(est) // 10] < 1.03 * k else None

def load():
    coins = []
    for line in open(os.path.join(DATA, "coins.jsonl")):
        m = json.loads(line)
        why = ("download " + m["status"] if m["status"] != "ok" else "non-SOL quote" if m.get("quote_mint") not in SOL_QUOTE
               else "no reserves" if not m.get("virtual_token_reserves") else None)
        if not why:
            try: tr = json.load(open(os.path.join(DATA, "trades", m["mint"] + ".json")))
            except Exception: why = "missing trades"
        if not why:
            m["k"] = fit_k(m, tr)
            if m["k"] is None: why = "non-constant curve (mayhem mode)"
        if why: SKIPPED[why] = SKIPPED.get(why, 0) + 1; continue
        coins.append((m, tr))
    return coins

class Replay:
    """One coin's launch window, replayed for one stake. Produces closed positions for 'signal' and near-miss sims."""
    def __init__(s, meta, trades, stake, nearmiss=True):
        s.m = meta; s.stake = stake; s.nearmiss = nearmiss
        s.k = meta["k"]
        s.t_create = meta["created_timestamp"] / 1000
        ev = []
        for x in trades:
            try: px = float(x["priceSol"]); ts = iso(x["timestamp"]) + 0.5   # 1 s timestamps: assume mid-second
            except Exception: continue
            if px <= 0: continue
            vsol = (px * s.k) ** 0.5
            ev.append(dict(ts=ts, type=x["type"], user=x["userAddress"], sol=float(x.get("amountSol") or 0), vsol=vsol,
                           curve=x.get("program", "pump") == "pump", tx=x["tx"]))
        # the dev's buy in the creation tx is part of the create message (mc0), not a trade the bot sees
        first_tx = ev[0]["tx"] if ev else None
        s.create_vsol = ev[0]["vsol"] if ev and ev[0]["user"] == meta["creator"] else s.k / Y.INIT_VTOK
        s.ev = [e for e in ev if not (e["tx"] == first_tx and e["user"] == meta["creator"])]
        s.tt = [e["ts"] for e in s.ev]; s.results = []; s.signal_t = None

    def true_reserves(s, t):
        i = bisect.bisect_right(s.tt, t) - 1
        vsol = s.ev[i]["vsol"] if i >= 0 else s.create_vsol
        return vsol, s.k / vsol, any(not e["curve"] for e in s.ev[:i + 1])

    def at_truth(s, c, t):
        """Context: set the coin's reserves to the true state at t (fills), restore the bot's view after."""
        rep = s
        class _Ctx:
            def __enter__(self):
                self.saved = (c.vsol, c.vtok, c.complete); vs, vt, done = rep.true_reserves(t)
                c.vsol, c.vtok = vs, vt; c.complete = c.complete or done
            def __exit__(self, *a): c.vsol, c.vtok, c.complete = self.saved
        return _Ctx()

    def run(s):
        m = s.m; t0 = s.t_create + DETECT; CLK.t = t0
        c = Y.Coin({"mint": m["mint"], "symbol": m.get("symbol", ""), "name": m.get("name", ""), "traderPublicKey": m["creator"],
                    "vSolInBondingCurve": s.create_vsol, "vTokensInBondingCurve": s.k / s.create_vsol}, t0)
        obs = [(e["ts"] + DETECT, 0, i) for i, e in enumerate(s.ev)]
        end = s.t_create + 745
        obs += [(t, 1, None) for t in [t0 + j for j in range(int(end - t0))]]   # 1 s poll ticks
        obs.sort()
        pending = []; open_ = []
        for T, kind, i in obs:
            CLK.t = T
            for f in [f for f in pending if f[0] <= T]:
                pending.remove(f); p = s.fill(c, *f)
                if p: open_.append(p)
            if kind == 0:
                e = s.ev[i]
                c.add({"txType": e["type"], "traderPublicKey": e["user"], "solAmount": e["sol"],
                       "vSolInBondingCurve": e["vsol"], "vTokensInBondingCurve": s.k / e["vsol"]}, T)
                if not e["curve"]: c.complete = True
            if not c.fired and T - c.t0 <= Y.F["max_age"] + 2:
                ok, st, fails = c.passes()
                if st["mc_mult"] > 15 and st["buys"] == 0: c.fired = True
                elif ok:
                    c.fired = True; s.signal_t = T; pending.append((T + Y.COST["fill_delay_s"], "signal", st, (c.vsol, c.vtok)))
                elif s.nearmiss and st["age"] >= Y.F["min_age"] and len(fails) == 1 and fails[0] != "age" and not c.nm_fired:
                    c.nm_fired = True; pending.append((T + Y.COST["fill_delay_s"], "nearmiss:" + fails[0], st, (c.vsol, c.vtok)))
            for p in list(open_):
                reason = p.check()
                if reason: open_.remove(p); s.close(c, p, reason, T + Y.COST["fill_delay_s"])
            if not open_ and not pending and (c.fired or T - c.t0 > Y.F["max_age"] + 2) and (c.nm_fired or not s.nearmiss or T - c.t0 > Y.F["max_age"] + 2):
                break
        for p in open_: s.close(c, p, "end of data", end)
        return s.results

    def fill(s, c, t, kind, st, seen):
        """Same as yolo_snipe.fill: expectation from the bot's view at decision time, fill on the true curve at t."""
        sol_in = (s.stake / SOL_USD - Y.COST["tx_sol"]) * (1 - Y.COST["fee"])
        expect = Y.buy_tokens(*seen, sol_in)
        rec = {"time": fmt(t), "kind": kind, "mint": c.mint, "sym": c.sym, "stats": st}
        with s.at_truth(c, t):
            if c.complete: s.results.append({**rec, "event": "sim_no_fill", "reason": "curve completed before fill"}); return None
            got = Y.buy_tokens(c.vsol, c.vtok, sol_in) * (1 - Y.COST["extra_slip"])
            if got < expect * (1 - Y.COST["max_slip"]):
                s.results.append({**rec, "event": "sim_no_fill", "reason": f"slippage > {Y.COST['max_slip']:.0%}"}); return None
            p = Y.Pos(c, kind, s.stake, SOL_USD, sol_in, got, c.px, t=t)
        s.results.append({**rec, "event": "sim_open", "id": p.id, "stake": s.stake, "entry_cost_pct": round((p.entry / p.mkt - 1) * 100, 2)})
        return p

    def close(s, c, p, reason, t):
        with s.at_truth(c, t):
            ex = p.proceeds_sol() / p.tokens; gross = (c.px / p.mkt - 1) * 100
        pnl = (ex / p.entry - 1) * 100
        s.results.append({"time": fmt(t), "event": "sim_close", "kind": p.kind, "id": p.id, "mint": c.mint, "sym": c.sym, "reason": reason,
                          "held_s": int(t - p.t), "stake": p.stake, "gross_pct": round(gross, 1), "pnl_pct": round(pnl, 1),
                          "net_usd": round(p.tokens * ex * SOL_USD - p.stake, 2), "peak_pct": round((p.peak / p.entry - 1) * 100, 1),
                          "t_open": p.t, "t_close": t})

def fmt(t): return datetime.fromtimestamp(t, TZ).strftime("%Y-%m-%d %H:%M:%S")

def ledger_sim(coins, signals, hours_gate=True):
    """One position at a time, STAKE_FRAC sizing, daily cap, active hours, floor + auto-reset - like the armed bot."""
    bal = 200.0; injected = 0.0; resets = 0; busy = 0.0; per_day = {}; log = []
    for t, (m, tr) in sorted(signals, key=lambda x: x[0]):
        d = datetime.fromtimestamp(t, TZ); hm = (d.hour, d.minute)
        if t < busy: continue
        if hours_gate and not (Y.HOURS[0] <= hm < Y.HOURS[1]): continue
        if per_day.get(d.date(), 0) >= Y.DAILY_CAP: continue
        stake = round(bal * Y.STAKE_FRAC, 2)
        res = [r for r in Replay(m, tr, stake, nearmiss=False).run() if r["event"] == "sim_close" and r["kind"] == "signal"]
        if not res: continue    # no fill
        r = res[0]; per_day[d.date()] = per_day.get(d.date(), 0) + 1
        bal = round(bal + r["net_usd"], 2); busy = r["t_close"]; log.append((r, bal))
        if bal < Y.FLOOR: injected += Y.RESET_TO - bal; resets += 1; bal = Y.RESET_TO
    return bal, injected, resets, log

def main():
    coins = load()
    if not coins: sys.exit(f"no data in {DATA} - run fetch_history.py first")
    t_lo = min(m["created_timestamp"] for m, _ in coins) / 1000; t_hi = max(m["created_timestamp"] for m, _ in coins) / 1000
    out = []; signals = []
    for m, tr in coins:
        rp = Replay(m, tr, Y.SIM_STAKE); res = rp.run(); out += res
        if rp.signal_t: signals.append((rp.signal_t, (m, tr)))
    os.makedirs(os.path.dirname(OUTF), exist_ok=True)
    with open(OUTF, "w") as f:
        for r in out: f.write(json.dumps(r) + "\n")

    closed = [r for r in out if r["event"] == "sim_close" and r["kind"] == "signal"]
    print(f"{len(coins)} launches {fmt(t_lo)} -> {fmt(t_hi)} Skopje ({(t_hi - t_lo) / 3600:.1f} h); "
          f"costs={'OFF' if '--no-costs' in args else Y.COST}; detect {DETECT}s")
    print(f"excluded: {SKIPPED}")
    print(f"filter passes: {len(signals)} ({100 * len(signals) / len(coins):.2f}% of launches), filled {len(closed)}")
    if closed:
        p = sorted(r["pnl_pct"] for r in closed); w = [x for x in p if x > 0]
        print(f"per-trade @ ${Y.SIM_STAKE:.0f}: win {100 * len(w) / len(p):.0f}%  EV {sum(p) / len(p):+.1f}%  median {p[len(p) // 2]:+.1f}%  "
              f"best {p[-1]:+.1f}%  worst {p[0]:+.1f}%  total ${sum(r['net_usd'] for r in closed):+.2f}")
        if not QUIET:
            print("\n  time      sym          peak%   gross%   net%   held  exit")
            for r in sorted(closed, key=lambda r: r["t_open"]):
                print(f"  {r['time'][11:19]}  {r['sym'][:10]:10} {r['peak_pct']:+7.1f} {r['gross_pct']:+8.1f} {r['pnl_pct']:+6.1f} {r['held_s']:5d}s  {r['reason']}")
    for gate in (True, False):
        bal, inj, n, lg = ledger_sim(coins, signals, gate)
        print(f"\nledger sim ({'active hours 08:00-23:50' if gate else 'no hours gate'}): {len(lg)} trades, $200 -> ${bal:.2f}, "
              f"{n} resets (${inj:.2f} injected) => true P&L ${bal - 200 - inj:+.2f}")
    print(f"\nfull detail: python3 analyze_snipe.py {OUTF}")

if __name__ == "__main__": main()
