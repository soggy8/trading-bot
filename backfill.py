#!/usr/bin/env python3
"""
backfill.py - download past days of pump.fun data instead of waiting for them to happen.
  python3 backfill.py [--days 7] [--launches-per-day 1000] [--grads-per-day 400] [--swap-rate 0.22] [--out data/backfill]
pump.fun's own listings only reach back ~1 h (launches) / ~1 day (graduations), so coin addresses come from the chain:
every launch is signed by the pump.fun mint authority and every graduation by the migration account. The free public
Solana RPC lists those transactions quickly but serves full transactions slowly (~0.8/s), so this takes an even random
sample per day. Then, per coin, the same data as the live downloaders, in the same formats:
  <out>/history/{coins.jsonl,trades/}     launches: first 750 s of trades   (like fetch_history.py)
  <out>/graduated/{coins.jsonl,candles/}  graduations: 1m + 5m candles       (like fetch_graduated.py)
Resumable: rerun the same command and it skips what is done. Read-only public APIs, no keys.
"""
import json, os, random, sys, threading, time, queue
import requests

RPC = "https://api.mainnet-beta.solana.com"
MINT_AUTHORITY = "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM"
MIGRATION = "39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg"
QUOTES = {"So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"}
META = "https://frontend-api-v3.pump.fun/coins-v2/{}"
TRADES = "https://swap-api.pump.fun/v2/coins/{}/trades?limit=100&cursor={}"
CANDLES = "https://swap-api.pump.fun/v2/coins/{}/candles?interval={}&limit=1000&createdTs={}"
H = {"User-Agent": "Mozilla/5.0 yolo-paper-backtest (read-only)"}
WINDOW_S = 750; MAX_PAGES = 200

args = sys.argv[1:]
opt = lambda name, default: type(default)(args[args.index(name) + 1]) if name in args else default
DAYS = opt("--days", 7); PER_DAY = opt("--launches-per-day", 1000); GRADS_PER_DAY = opt("--grads-per-day", 400)
SWAP_RATE = opt("--swap-rate", 0.22)   # swap-api allows ~0.25 requests/s per IP (shared with the other downloaders)
OUT = opt("--out", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "backfill"))
HIST = os.path.join(OUT, "history"); GRAD = os.path.join(OUT, "graduated")
for d in (os.path.join(HIST, "trades"), os.path.join(GRAD, "candles")): os.makedirs(d, exist_ok=True)
log = lambda *a: print(time.strftime("%m-%d %H:%M:%S"), *a, flush=True)

class Pacer:
    """Steady pacing (no bursts: pump.fun answers bursts with a long 429 penalty). Backs off on 429, creeps back slowly."""
    def __init__(s, rate): s.lock = threading.Lock(); s.next = 0.0; s.min = 1 / rate; s.gap = s.min
    def wait(s):
        with s.lock:
            now = time.time(); t = max(now, s.next); s.next = t + s.gap
        time.sleep(max(0, t - now))
    def ok(s): s.gap = max(s.min, s.gap * 0.95)
    def limited(s): s.gap = min(30.0, s.gap * 2)
SWAP, FRONT, CHAIN = Pacer(SWAP_RATE), Pacer(1.0), Pacer(0.8)

def get(url, pacer):
    for attempt in range(15):
        pacer.wait()
        try:
            r = requests.get(url, headers=H, timeout=30)
            if r.status_code == 200: pacer.ok(); return r.json()
            if r.status_code == 429: pacer.limited(); continue
            return None
        except Exception: time.sleep(3)
    return None

def rpc(method, params):
    for attempt in range(15):
        CHAIN.wait()
        try:
            r = requests.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=60)
            if r.status_code == 429: CHAIN.limited(); continue
            d = r.json()
            if "result" in d: CHAIN.ok(); return d["result"]
        except Exception: time.sleep(3)
    return None

def signatures(address, name):
    """All successful signatures of `address` over the last DAYS days, cached in <out>/<name>-sigs.jsonl."""
    path = os.path.join(OUT, name + "-sigs.jsonl"); since = time.time() - DAYS * 86400
    sigs = [json.loads(l) for l in open(path)] if os.path.exists(path) else []
    if sigs and sigs[-1]["t"] < since: return sigs
    before = sigs[-1]["sig"] if sigs else None
    with open(path, "a") as f:
        while True:
            p = {"limit": 1000, **({"before": before} if before else {})}
            page = rpc("getSignaturesForAddress", [address, p])
            if not page: break
            for s in page:
                if not s["err"] and s.get("blockTime"): x = {"sig": s["signature"], "t": s["blockTime"]}; sigs.append(x); f.write(json.dumps(x) + "\n")
            before = page[-1]["signature"]
            if page[-1].get("blockTime", 0) < since: break
            if len(sigs) % 20000 < 1000: log(f"{name}: {len(sigs)} signatures, back to {time.strftime('%m-%d %H:%M', time.localtime(page[-1]['blockTime']))}")
    return sigs

def sample(sigs, per_day, seed):
    """Even random sample: per_day signatures from each 24 h slice, counted back from the newest signature (so a rerun
    draws the same sample)."""
    rng = random.Random(seed); now = max(s["t"] for s in sigs); out = []
    for day in range(DAYS):
        lo, hi = now - (day + 1) * 86400, now - day * 86400
        bucket = [s for s in sigs if lo <= s["t"] < hi]
        out += rng.sample(bucket, min(per_day, len(bucket)))
    return out

def mint_of(sig, kind):
    tx = rpc("getTransaction", [sig, {"encoding": "json", "maxSupportedTransactionVersion": 1}])
    if not tx: return None
    if kind == "launch":   # the new mint signs its own creation; the fee payer is account 0
        keys = tx["transaction"]["message"]["accountKeys"]; n = tx["transaction"]["message"]["header"]["numRequiredSignatures"]
        signers = keys[1:n]
        return next((k for k in signers if k.endswith("pump")), signers[0] if signers else None)
    mints = {b["mint"] for b in tx["meta"].get("postTokenBalances") or []} - QUOTES
    return next((m for m in mints if m.endswith("pump")), next(iter(mints), None))

def done(path):
    try: return {json.loads(l)["mint"] for l in open(path)}
    except FileNotFoundError: return set()

KEEP = ("mint", "name", "symbol", "creator", "bonding_curve", "created_timestamp", "virtual_sol_reserves", "virtual_token_reserves",
        "virtual_quote_reserves", "quote_mint", "mayhem_state", "complete")
def save_launch(c):
    t0 = c["created_timestamp"]; cur = f"0000000000000000000000-{t0 + WINDOW_S * 1000}"; out = {}; status = "truncated"
    for _ in range(MAX_PAGES):
        d = get(TRADES.format(c["mint"], cur), SWAP)
        if d is None: return False
        for x in d.get("trades") or []: out[x["tx"] + x["type"] + x.get("baseAmount", "")] = x
        if not d.get("pagination", {}).get("hasMore"): status = "ok"; break
        cur = d["pagination"]["nextCursor"]
    trades = sorted(out.values(), key=lambda x: x["slotIndexId"])
    keep = ("timestamp", "slotIndexId", "tx", "type", "userAddress", "amountSol", "baseAmount", "priceSol", "program")
    with open(os.path.join(HIST, "trades", c["mint"] + ".json"), "w") as f: json.dump([{k: x.get(k) for k in keep} for x in trades], f)
    with open(os.path.join(HIST, "coins.jsonl"), "a") as f:
        f.write(json.dumps({**{k: c.get(k) for k in KEEP}, "status": status, "n_trades": len(trades), "fetched": time.time(),
                            "source": "backfill"}) + "\n")
    return True

GKEEP = ("mint", "name", "symbol", "creator", "created_timestamp", "quote_mint", "pump_swap_pool", "total_supply", "program")
def save_grad(c):
    m1 = get(CANDLES.format(c["mint"], "1m", c["created_timestamp"]), SWAP)
    m5 = get(CANDLES.format(c["mint"], "5m", c["created_timestamp"]), SWAP) if m1 is not None and len(m1) >= 1000 else []
    if m1 is None or m5 is None: return False
    with open(os.path.join(GRAD, "candles", c["mint"] + ".json"), "w") as f: json.dump({"1m": m1, "5m": m5}, f)
    with open(os.path.join(GRAD, "coins.jsonl"), "a") as f:
        f.write(json.dumps({**{k: c.get(k) for k in GKEEP}, "usd_market_cap_at_fetch": c.get("usd_market_cap"),
                            "n_1m": len(m1), "n_5m": len(m5), "fetched": time.time(), "source": "backfill"}) + "\n")
    return True

def main():
    launches = sample(signatures(MINT_AUTHORITY, "launch"), PER_DAY, 1)
    grads = sample(signatures(MIGRATION, "grad"), GRADS_PER_DAY, 2)
    log(f"sampled {len(launches)} launches + {len(grads)} graduations over {DAYS} days")
    mints_path = os.path.join(OUT, "mints.jsonl")
    known = {json.loads(l)["sig"]: json.loads(l) for l in open(mints_path)} if os.path.exists(mints_path) else {}
    have = done(os.path.join(HIST, "coins.jsonl")) | done(os.path.join(GRAD, "coins.jsonl"))
    work = queue.Queue(maxsize=200)

    def resolve():   # chain: signature -> mint (slowest step, ~0.8/s)
        with open(mints_path, "a") as f:
            for kind, s in [("launch", s) for s in launches] + [("grad", s) for s in grads]:
                x = known.get(s["sig"])
                if not x:
                    m = mint_of(s["sig"], kind)
                    if not m: continue
                    x = {"sig": s["sig"], "kind": kind, "mint": m, "t": s["t"]}; f.write(json.dumps(x) + "\n"); f.flush()
                if x["mint"] not in have: work.put(x)
        work.put(None)

    def fetch():   # pump.fun: metadata, then trades or candles
        n = 0
        while (x := work.get()) is not None:
            c = get(META.format(x["mint"]), FRONT)
            if not c or not c.get("created_timestamp"): continue
            if not (save_launch if x["kind"] == "launch" else save_grad)(c): continue
            n += 1; have.add(x["mint"])
            if n % 50 == 0: log(f"saved {n} coins (swap gap {SWAP.gap:.1f}s, rpc gap {CHAIN.gap:.1f}s, queue {work.qsize()})")
        log(f"done: saved {n} coins this run")

    threading.Thread(target=resolve, daemon=True).start()
    fetch()

if __name__ == "__main__": main()
