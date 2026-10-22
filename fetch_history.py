#!/usr/bin/env python3
"""
fetch_history.py - download pump.fun launches + their first WINDOW_S seconds of trades for backtest_snipe.py.
  python3 fetch_history.py [--hours H] [--out data/history]
The coin listing only reaches back ~1-1.5 h, so this grabs what is listed now, then keeps polling and saves
each new coin once its launch window has passed (run it for H hours to build a bigger sample).
Read-only public APIs, no keys.
"""
import json, os, re, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
import requests

LIST = "https://frontend-api-v3.pump.fun/coins?offset={}&limit=50&sort=created_timestamp&order=DESC&includeNsfw=true"
TRADES = "https://swap-api.pump.fun/v2/coins/{}/trades?limit=100&cursor={}"
H = {"User-Agent": "Mozilla/5.0 yolo-paper-backtest (read-only)"}
WINDOW_S = 750; MAX_PAGES = 200; MIN_GAP = 4.0   # seconds between requests (all threads): swap-api allows ~1 / 4 s per IP, bursts get a long 429 penalty
MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,48}$")

args = sys.argv[1:]
HOURS = float(args[args.index("--hours") + 1]) if "--hours" in args else 0.0
OUT = args[args.index("--out") + 1] if "--out" in args else os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "history")
TDIR = os.path.join(OUT, "trades"); META = os.path.join(OUT, "coins.jsonl")
os.makedirs(TDIR, exist_ok=True)

class Pacer:
    """Per-host pacing: MIN_GAP between requests, x2 on 429 (or wait out x-ratelimit-reset), back to MIN_GAP quickly."""
    def __init__(s, gap): s.lock = threading.Lock(); s.last = 0.0; s.min = gap; s.gap = gap
    def wait(s):
        with s.lock:
            w = s.last + s.gap - time.time()
            if w > 0: time.sleep(w)
            s.last = time.time()
PACE = {"frontend-api-v3.pump.fun": Pacer(1.0), "swap-api.pump.fun": Pacer(MIN_GAP)}
def get(url):
    p = PACE[url.split("/")[2]]
    for attempt in range(12):
        p.wait()
        try:
            r = requests.get(url, headers=H, timeout=20)
            if r.status_code == 200:
                p.gap = max(p.min, p.gap * 0.7); return r.json()
            if r.status_code == 429:
                p.gap = min(10.0, p.gap * 2)
                reset = r.headers.get("x-ratelimit-reset")
                if reset and reset.isdigit() and r.headers.get("x-ratelimit-remaining") == "0": time.sleep(min(60, int(reset)))
                continue
            return None
        except Exception: time.sleep(2)
    return None

def fetch_trades(c):
    """All trades from creation to creation+WINDOW_S, oldest first. Pages backwards from a timestamp cursor."""
    t0 = c["created_timestamp"]; cur = f"0000000000000000000000-{t0 + WINDOW_S * 1000}"; out = {}
    for _ in range(MAX_PAGES):
        d = get(TRADES.format(c["mint"], cur))
        if d is None: return None, "api error"
        for x in d.get("trades") or []: out[x["tx"] + x["type"] + x.get("baseAmount", "")] = x
        if not d.get("pagination", {}).get("hasMore"): return sorted(out.values(), key=lambda x: x["slotIndexId"]), "ok"
        cur = d["pagination"]["nextCursor"]
    return sorted(out.values(), key=lambda x: x["slotIndexId"]), "truncated"

KEEP = ("mint", "name", "symbol", "creator", "bonding_curve", "created_timestamp", "virtual_sol_reserves", "virtual_token_reserves",
        "virtual_quote_reserves", "quote_mint", "mayhem_state", "complete")
def save(c):
    trades, status = fetch_trades(c)
    if trades is None: return False
    keep = ("timestamp", "slotIndexId", "tx", "type", "userAddress", "amountSol", "baseAmount", "priceSol", "program")
    with open(os.path.join(TDIR, c["mint"] + ".json"), "w") as f:
        json.dump([{k: x.get(k) for k in keep} for x in trades], f)
    with open(META, "a") as f:
        f.write(json.dumps({**{k: c.get(k) for k in KEEP}, "status": status, "n_trades": len(trades), "fetched": time.time()}) + "\n")
    _saved[0] += 1
    if _saved[0] % 25 == 0: print(f"{time.strftime('%H:%M:%S')} +{_saved[0]} coins this run (gap {PACE['swap-api.pump.fun'].gap:.2f}s)", flush=True)
    return True
_saved = [0]

def done_mints():
    try: return {json.loads(l)["mint"] for l in open(META)}
    except FileNotFoundError: return set()

def list_coins():
    out = []
    for off in range(0, 3000, 50):
        d = get(LIST.format(off))
        if not d: break
        out += [c for c in d if MINT_RE.match(c.get("mint", ""))]
    return out

def main():
    end = time.time() + HOURS * 3600; done = done_mints(); waiting = {}
    while True:
        for c in list_coins(): waiting.setdefault(c["mint"], c)
        now = time.time()
        ready = [c for m, c in waiting.items() if m not in done and now - c["created_timestamp"] / 1000 > WINDOW_S + 30]
        print(f"{time.strftime('%H:%M:%S')} listed {len(waiting)} waiting, {len(ready)} ready, {len(done)} saved", flush=True)
        with ThreadPoolExecutor(8) as ex:   # busy coins take ~3 s per page, so overlap them
            for c, ok in zip(ready, ex.map(save, ready)):
                if ok: done.add(c["mint"])
        for m in [m for m in waiting if m in done]: waiting.pop(m)
        print(f"{time.strftime('%H:%M:%S')} saved total {len(done)} (request gap {PACE['swap-api.pump.fun'].gap:.2f}s)", flush=True)
        if time.time() > end: break
        time.sleep(max(0, 600 - (time.time() - now)))   # listing covers ~1h, so every 10 min is plenty

if __name__ == "__main__": main()
