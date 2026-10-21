#!/usr/bin/env python3
"""
analyze_snipe.py - summarise yolo_snipe.py paper results.
  python3 analyze_snipe.py [dryrun.jsonl] [--since "YYYY-MM-DD HH:MM"]
Groups closed trades by kind:
  signal            every filter pass, simulated independently at a fixed stake (the clean sample)
  real              ledger trades (one at a time, so a subset of signal)
  nearmiss:<filter> coins that failed only that filter. If its EV beats 'signal', the filter is costing money.
  legacy-*          rows written by the old version (no fees/latency model) - not comparable
Also reports ledger reset injections so ledger P&L can be judged net of resets.
"""
import json, os, statistics, sys
from collections import defaultdict

def load(path, since):
    rows = []
    for line in open(path):
        try: r = json.loads(line)
        except Exception: continue
        if since and str(r.get("time", "")) < since: continue
        rows.append(r)
    return rows

def kind_of(r):
    if "kind" in r: return r["kind"]
    return "legacy-real" if r.get("event") == "close" else "legacy-sim"

def summarise(trades):
    p = [t["pnl_pct"] for t in trades]
    wins = [x for x in p if x > 0]; losses = [x for x in p if x <= 0]
    usd = [t.get("net_usd") for t in trades if t.get("net_usd") is not None]
    return dict(n=len(p), win=100 * len(wins) / len(p), avg_win=statistics.mean(wins) if wins else 0.0,
                avg_loss=statistics.mean(losses) if losses else 0.0, ev=statistics.mean(p), med=statistics.median(p),
                worst=min(p), usd=sum(usd) if usd else None)

def main():
    args = sys.argv[1:]; since = None
    if "--since" in args:
        i = args.index("--since"); since = args[i + 1]; del args[i:i + 2]
    path = args[0] if args else None
    W = None
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import yolo_snipe as Y; W = Y.W; path = path or Y.DRY
    except Exception as e:
        print(f"(yolo_snipe/yolo_live_watch not importable: {e}; ledger section skipped)")
    if not path: sys.exit("usage: analyze_snipe.py dryrun.jsonl [--since 'YYYY-MM-DD HH:MM']")
    rows = load(path, since)

    closes = defaultdict(list); no_fill = defaultdict(int); opens = defaultdict(int); reasons = defaultdict(lambda: defaultdict(list))
    for r in rows:
        ev = r.get("event"); k = kind_of(r)
        if ev in ("sim_close", "close") and "pnl_pct" in r:
            closes[k].append(r); reasons[k][r.get("reason", "?").split(" (")[0]].append(r["pnl_pct"])
        elif ev in ("sim_no_fill", "no_fill"): no_fill[k] += 1
        elif ev in ("sim_open", "open"): opens[k] += 1
    skipped = sum(1 for r in rows if r.get("event") == "sim_skip")

    print(f"{path}: {len(rows)} rows" + (f" since {since}" if since else ""))
    print(f"\n{'kind':28} {'n':>5} {'nofill':>6} {'win%':>6} {'avgW%':>7} {'avgL%':>7} {'EV%':>7} {'med%':>7} {'worst%':>7} {'net$':>9}")
    order = sorted(closes, key=lambda k: (k != "signal", k != "real", k.startswith("legacy"), k))
    base = None
    for k in order:
        s = summarise(closes[k])
        if k == "signal": base = s["ev"]
        nf = no_fill[k]; nf_s = f"{100 * nf / (nf + opens[k]):.0f}%" if nf + opens[k] else "-"
        usd = f"{s['usd']:+.2f}" if s["usd"] is not None else "-"
        print(f"{k:28} {s['n']:5d} {nf_s:>6} {s['win']:6.1f} {s['avg_win']:7.1f} {s['avg_loss']:7.1f} {s['ev']:7.1f} {s['med']:7.1f} {s['worst']:7.1f} {usd:>9}")
    if skipped: print(f"\n{skipped} sims skipped at the MAX_SIMS cap (sample is incomplete for that period)")

    for k in ("signal", "real"):
        if k in reasons:
            print(f"\nexit reasons ({k}):")
            for why, v in sorted(reasons[k].items(), key=lambda x: -len(x[1])):
                print(f"  {why:40} n={len(v):4d}  avg {statistics.mean(v):+6.1f}%")

    nm = [k for k in closes if k.startswith("nearmiss:")]
    if nm and base is not None:
        print("\nfilter check (near-miss EV vs signal EV; needs ~30+ trades per row to mean anything):")
        for k in sorted(nm, key=lambda k: -summarise(closes[k])["ev"]):
            s = summarise(closes[k]); verdict = "filter looks USEFUL" if s["ev"] < base else "filter may be COSTING trades"
            print(f"  {k[9:]:20} n={s['n']:4d}  EV {s['ev']:+6.1f}% vs {base:+.1f}%  -> {verdict}")

    if W:
        try:
            _, lrows = W.read_ledger()
            n, inj = Y.reset_injections(lrows)
            snipes = [r for r in lrows if r.get("mode") == Y.MODE_TAG and "CLOSED" in r.get("decision", "")]
            nets = [float(r["result_pct_net"]) for r in snipes if r.get("result_pct_net")]
            print(f"\nledger: {len(snipes)} closed yolo-snipe trades" + (f", avg net {statistics.mean(nets):+.1f}%" if nets else ""))
            print(f"ledger: balance {W.ledger_balance(lrows)}, {n} yolo resets injected ${inj:.2f} - subtract that from the balance before calling it profit")
        except Exception as e:
            print(f"(ledger section failed: {e})")

if __name__ == "__main__": main()
