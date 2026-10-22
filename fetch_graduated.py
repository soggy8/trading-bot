#!/usr/bin/env python3
"""
fetch_graduated.py - download price candles of pump.fun coins that graduated to PumpSwap, for testing slower strategies.
  python3 fetch_graduated.py [--until EPOCH | --hours H] [--age-h 12] [--out data/graduated]
The graduated listing reaches back ~1 day (~1000 graduations), so the first pass backfills that; then it polls and saves each coin once it is
AGE_H hours old (so every coin has the same amount of history after launch). Per coin: the last 1000 1-minute and
1000 5-minute candles (USD prices; minutes with no trades are skipped). Read-only public APIs, no keys.
"""
import json, os, re, sys, threading, time
import requests

LIST = "https://frontend-api-v3.pump.fun/coins?offset={}&limit=50&sort=created_timestamp&order=DESC&includeNsfw=true&complete=true"
CANDLES = "https://swap-api.pump.fun/v2/coins/{}/candles?interval={}&limit=1000&createdTs={}"
H = {"User-Agent": "Mozilla/5.0 yolo-paper-backtest (read-only)"}
MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,48}$")

args = sys.argv[1:]
opt = lambda name, default: type(default)(args[args.index(name) + 1]) if name in args else default
UNTIL = opt("--until", 0.0) or time.time() + opt("--hours", 0.0) * 3600
AGE_H = opt("--age-h", 12.0)
OUT = opt("--out", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "graduated"))
CDIR = os.path.join(OUT, "candles"); META = os.path.join(OUT, "coins.jsonl")
os.makedirs(CDIR, exist_ok=True)

class Pacer:
    """Per-host pacing: gap between requests, x2 on 429, back down on success."""
    def __init__(s, gap): s.lock = threading.Lock(); s.last = 0.0; s.min = gap; s.gap = gap
    def wait(s):
        with s.lock:
            w = s.last + s.gap - time.time()
            if w > 0: time.sleep(w)
            s.last = time.time()
PACE = {"frontend-api-v3.pump.fun": Pacer(1.0), "swap-api.pump.fun": Pacer(0.15)}
def get(url):
    p = PACE[url.split("/")[2]]
    for attempt in range(12):
        p.wait()
        try:
            r = requests.get(url, headers=H, timeout=20)
            if r.status_code == 200: p.gap = max(p.min, p.gap * 0.7); return r.json()
            if r.status_code == 429: p.gap = min(10.0, p.gap * 2); continue
            return None
        except Exception: time.sleep(2)
    return None

KEEP = ("mint", "name", "symbol", "creator", "created_timestamp", "quote_mint", "pump_swap_pool", "total_supply", "program")
def save(c):
    m1 = get(CANDLES.format(c["mint"], "1m", c["created_timestamp"]))
    m5 = get(CANDLES.format(c["mint"], "5m", c["created_timestamp"]))
    if m1 is None or m5 is None: return False
    with open(os.path.join(CDIR, c["mint"] + ".json"), "w") as f: json.dump({"1m": m1, "5m": m5}, f)
    # market cap / ATH at fetch time are future information for any strategy: kept only for sanity checks
    with open(META, "a") as f:
        f.write(json.dumps({**{k: c.get(k) for k in KEEP}, "usd_market_cap_at_fetch": c.get("usd_market_cap"),
                            "n_1m": len(m1), "n_5m": len(m5), "fetched": time.time()}) + "\n")
    return True

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
    done = done_mints(); waiting = {}
    while True:
        for c in list_coins(): waiting.setdefault(c["mint"], c)
        now = time.time()
        ready = [c for m, c in waiting.items() if m not in done and now - c["created_timestamp"] / 1000 > AGE_H * 3600]
        oldest = min((c["created_timestamp"] for c in waiting.values()), default=now * 1000)
        print(f"{time.strftime('%H:%M:%S')} {len(waiting)} listed (oldest {(now - oldest / 1000) / 3600:.1f} h), {len(ready)} ready, "
              f"{len(done)} saved", flush=True)
        for c in ready:
            if save(c): done.add(c["mint"]); waiting.pop(c["mint"])
        print(f"{time.strftime('%H:%M:%S')} saved total {len(done)}", flush=True)
        if time.time() > UNTIL: break
        time.sleep(max(0, 1800 - (time.time() - now)))   # listing covers ~1 day, every 30 min is plenty

if __name__ == "__main__": main()
