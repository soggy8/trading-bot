#!/usr/bin/env python3
"""
yolo_snipe.py - PAPER ONLY fast launch sniper for the yolo ledger.
No wallets, no keys, no orders. Reads PumpPortal public websocket (wss://pumpportal.fun/api/data),
Solana public RPC (curve state + latency sampling), pump.fun trades API (fallback only) and CoinGecko (SOL/USD).

Modes:
  OBSERVE (before ARM_AT or with --observe): no ledger writes.
  ARMED  (from ARM_AT, Skopje): writes OPEN/CLOSE rows (mode "yolo-snipe") to yolo-log.csv and
          events to yolo-live-events.jsonl (same format as yolo_live_watch.py).
  In both modes every filter pass ("signal") and every coin that misses exactly one filter ("nearmiss:<filter>")
  is also simulated independently at a fixed $SIM_STAKE and logged to yolo-snipe-dryrun.jsonl.
  Run analyze_snipe.py on that file to judge the filters/exits.
  ARM_AT: --arm-at "YYYY-MM-DD HH:MM" or env YOLO_SNIPE_ARM_AT (Skopje time).

Paper fill model (COST): a buy/sell lands fill_delay_s after the decision, against the curve state at that moment,
  with constant-product price impact of our own size, pump.fun fee per side, extra adverse slip and a SOL tx cost.
  Buys fail (no fill) if they would get > max_slip fewer tokens than expected at decision time.
  Positions are marked as if our own buy were in the curve, so selling gives back our impact, not a free gain.

Stop: kill $(cat /workspace/trading-desk/yolo-snipe.pid)   or  touch /workspace/trading-desk/yolo-snipe.STOP
Pause entries only: touch /workspace/trading-desk/yolo-snipe.PAUSE
"""
import asyncio, base64, fcntl, json, os, re, shutil, struct, sys, threading, time, traceback
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import requests, websockets
import yolo_live_watch as W   # reuse ledger io, balance parser, formats (its main() is guarded)

DESK = W.DESK; TZ = W.TZ
DRY = os.path.join(DESK, "yolo-snipe-dryrun.jsonl")
LOG = os.path.join(DESK, "yolo-snipe.log")
LAT = os.path.join(DESK, "yolo-snipe-latency.jsonl")
PIDF = os.path.join(DESK, "yolo-snipe.pid")
STOPF = os.path.join(DESK, "yolo-snipe.STOP"); PAUSEF = os.path.join(DESK, "yolo-snipe.PAUSE")
TESTF = os.path.join(DESK, "yolo-snipe-test.json")
POSF = os.path.join(DESK, "yolo-snipe-pos.json")
WS = "wss://pumpportal.fun/api/data"
RPC = "https://api.mainnet-beta.solana.com"
SWAP = "https://swap-api.pump.fun/v2/coins/{}/trades?limit=100"
MODE_TAG = "yolo-snipe"

def arm_at():
    v = os.environ.get("YOLO_SNIPE_ARM_AT")
    if "--arm-at" in sys.argv: v = sys.argv[sys.argv.index("--arm-at") + 1]
    if v: return datetime.strptime(v, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
    return datetime(2026, 10, 21, 8, 0, tzinfo=TZ)
ARM_AT = arm_at()
force_observe = "--observe" in sys.argv

# ---- entry filter (evaluated on every trade + every poll while coin age 15-60 s)
F = dict(min_age=15, max_age=60, min_buyers=8, min_buys=10, min_net_sol=3.0, min_mc_mult=1.30,
         max_sell_ratio=0.5, max_top_buyer_share=0.35, max_mc_sol=150.0, max_mc_mult=3.0, min_avg_buy_sol=0.15, min_mc_rising_frac=0.92)
# ---- exits (checked on every trade + every poll / 2 s tick)
X = dict(sl=-0.40, tp=0.80, be_at=0.25, trail_at=0.40, trail_gap=0.20, trail_floor=0.15, stale_s=90,
         mom_window=30, sell_mult=2.5, time_stop_s=600)
# ---- paper fill costs (fee: pump.fun protocol+creator fee per side - check the current schedule)
COST = dict(fee=0.0125, tx_sol=0.002, fill_delay_s=1.5, extra_slip=0.01, max_slip=0.15)
STAKE_FRAC = 0.20          # armed trades risk this fraction of the yolo ledger, not all of it
SIM_STAKE = 40.0; MAX_SIMS = 80
HOURS = ((8, 0), (23, 50)); DAILY_CAP = 10; FLOOR = 25.0; RESET_TO = 200.0
API_MAX = 90; API_RESERVE = 30   # trades API calls/min; the last API_RESERVE are kept for open positions
INIT_VSOL = 30.0; INIT_VTOK = 1.073e9   # pump.fun initial virtual reserves (SOL, tokens)

def now(): return datetime.now(TZ)
def log(m):
    line = f"{W.ts()} {m}"
    with open(LOG, "a") as f: f.write(line + "\n")
    print(line, flush=True)
def jl(path, obj):
    with open(path, "a") as f: f.write(json.dumps(obj) + "\n")

def test_state():
    try:
        t = json.load(open(TESTF))
        if t.get("remaining", 0) > 0 and now() < datetime.strptime(t["until"], "%Y-%m-%d %H:%M").replace(tzinfo=TZ): return t
    except Exception: pass
    return None
def test_used():
    try:
        t = json.load(open(TESTF)); t["remaining"] = max(0, t["remaining"] - 1); json.dump(t, open(TESTF, "w"))
    except Exception as e: log(f"WARN test_used: {e}")

class State:
    def __init__(s):
        s.coins = {}; s.real = None; s.sims = []; s.entering = False
        s.ws = None; s.ws_up = False; s.sol_usd = None; s.tasks = set(); s.api_times = []; s.lat_n = 0
        s.cnt = {"create": 0, "trade": 0, "eval": 0, "api": 0}; s.fails = {}
S = State()

def armed(): return not force_observe and (now() >= ARM_AT or test_state() is not None)
def positions(): return ([S.real] if S.real else []) + S.sims

def spawn(coro):
    t = asyncio.create_task(coro); S.tasks.add(t); t.add_done_callback(_task_done)
def _task_done(t):
    S.tasks.discard(t)
    if not t.cancelled() and t.exception():
        log("ERROR task " + "".join(traceback.format_exception(t.exception())))

async def send(o):
    try: await S.ws.send(json.dumps(o))
    except Exception: pass

# ---------------- external data
def sol_px_loop():
    while True:
        try:
            S.sol_usd = float(requests.get("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd", timeout=8).json()["solana"]["usd"])
        except Exception as e:
            log(f"WARN sol price: {e}")
        time.sleep(60)

def sample_latency(sig, recv_ts, mint):
    time.sleep(3)
    try:
        r = requests.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": "getTransaction",
              "params": [sig, {"maxSupportedTransactionVersion": 0, "encoding": "json", "commitment": "confirmed"}]}, timeout=10).json()
        bt = (r.get("result") or {}).get("blockTime")
        if bt: jl(LAT, {"mint": mint, "blockTime": bt, "recv": round(recv_ts, 3), "latency_s": round(recv_ts - bt, 2)})
    except Exception as e:
        log(f"WARN latency sample: {e}")

def rpc_curves(keys):
    r = requests.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
          "params": [keys, {"encoding": "base64", "commitment": "processed"}]}, timeout=6).json()
    out = {}
    for k, v in zip(keys, (r.get("result") or {}).get("value") or []):
        if v:
            b = base64.b64decode(v["data"][0])
            if len(b) >= 49:
                vt, vs, rt, rs, sup = struct.unpack_from("<5Q", b, 8); out[k] = (vt, vs, bool(b[48]))
    return out

async def refresh_curves(cs):
    cs = [c for c in dict.fromkeys(cs) if c.curve]
    for i in range(0, len(cs), 100):
        chunk = cs[i:i + 100]
        try: cv = await asyncio.to_thread(rpc_curves, [c.curve for c in chunk])
        except Exception as e: log(f"WARN rpc curves: {type(e).__name__}: {e}"); return
        for c in chunk:
            if c.curve in cv: c.set_curve(*cv[c.curve])

def swap_trades(mint, candidate):
    """Fallback trade history for coins without a complete websocket stream. Candidates can't use the reserved budget."""
    t = time.time()
    while S.api_times and t - S.api_times[0] > 60: S.api_times.pop(0)
    if len(S.api_times) >= API_MAX - (API_RESERVE if candidate else 0): return None
    S.api_times.append(t); S.cnt["api"] += 1
    try:
        r = requests.get(SWAP.format(mint), headers={"User-Agent": "Mozilla/5.0 yolo-paper-sniper (read-only)"}, timeout=6)
        return r.json().get("trades") if r.status_code == 200 else None
    except Exception as e:
        log(f"WARN trades api {mint}: {type(e).__name__}: {e}"); return None

# ---------------- bonding curve math (SOL and token units, constant product on virtual reserves)
def buy_tokens(vsol, vtok, sol_in): return vtok * sol_in / (vsol + sol_in)
def sell_sol(vsol, vtok, tokens): return vsol * tokens / (vtok + tokens)

class Coin:
    def __init__(s, m, t):
        s.mint = m["mint"]; s.name = m.get("name", ""); s.sym = m.get("symbol", ""); s.dev = m.get("traderPublicKey")
        s.curve = m.get("bondingCurveKey"); s.t0 = t
        s.vsol = m.get("vSolInBondingCurve") or INIT_VSOL; s.vtok = m.get("vTokensInBondingCurve") or INIT_VTOK
        s.mc0 = s.mc; s.peak_mc = s.mc0
        s.trades = []; s.dev_sold = False; s.fired = False; s.nm_fired = False; s.complete = False
        s.last_trade_ts = t; s.last_tcheck = 0; s.last_poll_ok = 0
        s.ws_ok = True   # False = websocket stream incomplete (reconnect gap / reattach / not delivering): use trades API
    @property
    def px(s): return s.vsol / s.vtok          # SOL per token
    @property
    def mc(s): return s.px * 1e9               # SOL, 1B supply
    def set_reserves(s, vsol, vtok):
        if vtok > 0: s.vsol, s.vtok = vsol, vtok; s.peak_mc = max(s.peak_mc, s.mc)
    def set_px(s, px):
        k = INIT_VSOL * INIT_VTOK; vsol = (px * k) ** 0.5; s.set_reserves(vsol, k / vsol)
    def set_curve(s, vtok, vsol, complete):
        s.set_reserves(vsol / 1e9, vtok / 1e6); s.complete = s.complete or complete
    def set_trades(s, tl):
        out = []
        for x in tl:
            try:
                te = datetime.fromisoformat(x["timestamp"].replace("Z", "+00:00")).timestamp()
                out.append((te, x["type"], x["userAddress"], float(x.get("amountSol") or x.get("quoteAmount") or 0)))
            except Exception: pass
        out.sort(); s.trades = out; s.last_poll_ok = time.time()
        if out: s.last_trade_ts = max(s.last_trade_ts, out[-1][0])
        s.dev_sold = any(x[1] == "sell" and x[2] == s.dev for x in out)
    def add(s, m, t):
        if m.get("vTokensInBondingCurve"): s.set_reserves(m["vSolInBondingCurve"], m["vTokensInBondingCurve"])
        s.trades.append((t, m.get("txType"), m.get("traderPublicKey"), float(m.get("solAmount") or 0))); s.last_trade_ts = t
        if m.get("txType") == "sell" and m.get("traderPublicKey") == s.dev: s.dev_sold = True
    def stats(s):
        buys = [x for x in s.trades if x[1] == "buy" and x[2] != s.dev]
        sells = [x for x in s.trades if x[1] == "sell"]
        bsol = sum(x[3] for x in buys); ssol = sum(x[3] for x in sells)
        per = {}
        for x in buys: per[x[2]] = per.get(x[2], 0) + x[3]
        top = max(per.values()) / bsol if bsol else 1
        return dict(age=round(time.time() - s.t0, 1), buyers=len(per), buys=len(buys), sells=len(sells),
                    buy_sol=round(bsol, 2), sell_sol=round(ssol, 2), net_sol=round(bsol - ssol, 2),
                    mc_sol=round(s.mc, 1), mc_mult=round(s.mc / s.mc0, 2), top_share=round(top, 2),
                    dev_sold=s.dev_sold)
    def passes(s):
        st = s.stats(); fails = []
        if not F["min_age"] <= st["age"] <= F["max_age"]: fails.append("age")
        if st["buyers"] < F["min_buyers"]: fails.append("buyers")
        if st["buys"] < F["min_buys"]: fails.append("buys")
        if st["net_sol"] < F["min_net_sol"]: fails.append("net_sol")
        if st["mc_mult"] < F["min_mc_mult"]: fails.append("mc_mult")
        if st["buys"] and st["sells"] / st["buys"] > F["max_sell_ratio"]: fails.append("sell_ratio")
        if st["top_share"] > F["max_top_buyer_share"]: fails.append("top_buyer")
        if st["mc_sol"] > F["max_mc_sol"]: fails.append("mc_high")
        if s.mc < F["min_mc_rising_frac"] * s.peak_mc: fails.append("off_peak")
        if s.dev_sold: fails.append("dev_sold")
        if st["mc_mult"] > F["max_mc_mult"]: fails.append("mc_mult_high")
        if st["buys"] and st["buy_sol"] / st["buys"] < F["min_avg_buy_sol"]: fails.append("tiny_buys(bot)")
        return not fails, st, fails

class Pos:
    """A filled paper position. kind: 'real' (ledger), 'signal' (shadow sim of a pass) or 'nearmiss:<filter>'.
    entry is the effective SOL/token paid (fees, impact, tx cost included); mkt is the curve price before our buy."""
    def __init__(s, coin, kind, stake, sol_usd, sol_net, tokens, mkt, t=None):
        s.c = coin; s.kind = kind; s.armed = kind == "real"; s.stake = stake; s.sol = sol_usd
        s.sol_net = sol_net; s.tokens = tokens; s.mkt = mkt; s.entry = stake / sol_usd / tokens
        s.t = t or time.time(); s.stop = s.entry * (1 + X["sl"]); s.tp = s.entry * (1 + X["tp"]); s.peak = s.mark()
        s.stage = "initial"; s.row_key = None; s.test = False; s.closing = False; s._saved = None
        s.id = f"{coin.mint[:8]}-{kind}-{int(s.t)}"
    def reserves(s):
        """Curve reserves as if our paper buy were really in the curve (k constant)."""
        k = s.c.vsol * s.c.vtok; vsol = s.c.vsol + s.sol_net
        return vsol, k / vsol
    def mark(s):
        vsol, vtok = s.reserves(); return vsol / vtok
    def proceeds_sol(s):
        vsol, vtok = s.reserves()
        return max(0.0, sell_sol(vsol, vtok, s.tokens) * (1 - COST["fee"]) * (1 - COST["extra_slip"]) - COST["tx_sol"])
    def save(s):
        json.dump({"mint": s.c.mint, "curve": s.c.curve, "dev": s.c.dev, "sym": s.c.sym, "name": s.c.name, "t": s.t,
                   "sol": s.sol, "stake": s.stake, "sol_net": s.sol_net, "tokens": s.tokens, "mkt": s.mkt, "entry": s.entry,
                   "stop": s.stop, "tp": s.tp, "peak": s.peak, "stage": s.stage,
                   "row_key": list(s.row_key) if s.row_key else None, "test": s.test}, open(POSF, "w"))
    def check(s):
        c = s.c; px = s.mark(); s.peak = max(s.peak, px); g = px / s.entry - 1; t = time.time()
        if px >= s.tp: return "TP"
        if s.peak / s.entry - 1 >= X["trail_at"]:
            ns = max(s.peak * (1 - X["trail_gap"]), s.entry * (1 + X["trail_floor"]))
            if ns > s.stop: s.stop = ns; s.stage = "trail"
        elif g >= X["be_at"] and s.stop < s.entry:
            s.stop = s.entry; s.stage = "breakeven"
        if s.armed and (s.stop, s.stage) != s._saved:
            s._saved = (s.stop, s.stage); s.save()
        if c.complete: return "curve completed (migration)"
        if px <= s.stop: return "SL" if s.stage == "initial" else f"stop ({s.stage})"
        if c.dev_sold: return "dev sold"
        recent = [x for x in c.trades if x[0] >= t - X["mom_window"]]
        b = sum(x[3] for x in recent if x[1] == "buy"); sl = sum(x[3] for x in recent if x[1] == "sell")
        if t - s.t > 15 and sl > b * X["sell_mult"] and sl > 1.0 and px < s.entry:
            return f"early exit ({X['mom_window']}s sell flow > {X['sell_mult']}x buys, below entry)"
        data_ok = (c.ws_ok and S.ws_up) or t - c.last_poll_ok < 20   # only call it stale if we'd have seen trades
        if data_ok and t - max(s.t, c.last_trade_ts) > X["stale_s"]: return f"stale ({X['stale_s']}s no trades)"
        if t - s.t > X["time_stop_s"]: return f"{X['time_stop_s'] // 60}-min time stop"
        return None

async def fill(c, kind, stake):
    """Simulate a buy landing fill_delay_s after the decision, on the curve state at that moment. Returns (Pos, None) or (None, why)."""
    sol = S.sol_usd
    sol_in = (stake / sol - COST["tx_sol"]) * (1 - COST["fee"])
    if sol_in <= 0: return None, "stake below tx cost"
    expect = buy_tokens(c.vsol, c.vtok, sol_in); px0 = c.px
    await asyncio.sleep(COST["fill_delay_s"])
    await refresh_curves([c])
    if c.complete: return None, "curve completed before fill"
    got = buy_tokens(c.vsol, c.vtok, sol_in) * (1 - COST["extra_slip"])
    if got < expect * (1 - COST["max_slip"]):
        return None, f"slippage > {COST['max_slip']:.0%} (price {(c.px / px0 - 1) * 100:+.1f}% during {COST['fill_delay_s']}s delay)"
    return Pos(c, kind, stake, sol, sol_in, got, c.px), None

# ---------------- ledger gates (shared with scalper)
def today_rows(rows):
    d = now().strftime("%Y-%m-%d")
    return [r for r in rows if r.get("mode", "").startswith("yolo") and r.get("date", "").startswith(d) and "TRADE" in r.get("decision", "").upper()]
def gates(mint):
    if os.path.exists(PAUSEF): return "paused (PAUSE file)"
    t = now(); hm = (t.hour, t.minute)
    if not (HOURS[0] <= hm < HOURS[1]): return "outside active hours"
    fields, rows = W.read_ledger()
    if any(r.get("mode", "").startswith("yolo") and "OPEN" in r.get("decision", "").upper() for r in rows): return "another yolo trade open"
    tr = today_rows(rows); counted = [r for r in tr if "snipe test" not in r.get("notes", "")]
    if test_state() is None and len(counted) >= DAILY_CAP: return f"daily cap {len(counted)}/{DAILY_CAP}"
    if any(mint in (r.get("asset", "") + r.get("notes", "")) for r in tr): return "coin already traded today"
    bal = W.ledger_balance(rows)
    if bal is None or bal < FLOOR: return f"ledger {bal} under ${FLOOR} floor"
    return None

RESET_RE = re.compile(r"([\d.]+) -> ([\d.]+) \(reset\)")
def reset_injections(rows):
    """(count, USD) of paper cash added by yolo LEDGER RESET rows, so results can be judged net of resets."""
    n, tot = 0, 0.0
    for r in rows:
        if r.get("decision") == "LEDGER RESET" and r.get("mode", "").startswith("yolo"):
            m = RESET_RE.search(r.get("notes", ""))
            if m: n += 1; tot += float(m.group(2)) - float(m.group(1))
    return n, tot

def ledger_open(p, st):
    with open(W.LOCKFILE, "a+") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            fields, rows = W.read_ledger()
            bal = W.ledger_balance(rows); n = len(today_rows(rows)) + 1; ts_ = test_state()
            lbl = (f"snipe test #{ts_['total']-ts_['remaining']+1} of {ts_['total']} (outside daily cap, Vangel-approved test batch)" if ts_ else f"Trade #{n} of {DAILY_CAP} today")
            t = now(); c = p.c; ep = p.entry * p.sol
            r = {k: "" for k in fields}
            r.update(date=t.strftime("%Y-%m-%d %H:%M"), mode=MODE_TAG, asset_class="meme (Solana)",
                asset=f"{c.sym} {c.name} ({c.mint})", decision="TRADE (OPEN)", direction="long",
                entry=W.fmt_px(ep), stop=W.fmt_px(p.stop * p.sol), target=W.fmt_px(p.tp * p.sol),
                size=f"${p.stake:.2f} of ${bal:.2f} yolo ledger (~{p.tokens:,.1f} tokens), {STAKE_FRAC:.0%} of ledger",
                risk_pct=f"-40% stop on {STAKE_FRAC:.0%} of ledger (full stake at risk on rug/gap), rug checks skipped",
                skeptic_verdict="NONE - Vangel: paper launch sniper (yolo)",
                reasoning=f"Launch sniper {t.strftime('%H:%M:%S')}: pump.fun bonding curve coin {st['age']}s old; {st['buyers']} distinct buyers, {st['buys']} buys vs {st['sells']} sells, net +{st['net_sol']} SOL, mcap {st['mc_sol']} SOL ({st['mc_mult']}x creation), top buyer {int(st['top_share']*100)}% of buy vol, dev not selling. Fill = simulated buy landing {COST['fill_delay_s']}s after signal: curve price {W.fmt_px(p.mkt*p.sol)} -> effective {W.fmt_px(ep)} incl. {COST['fee']*100:.2f}% fee, own price impact, {COST['extra_slip']*100:.0f}% extra slip, {COST['tx_sol']} SOL tx cost (SOL ${p.sol}).",
                invalidation="SL -40%, BE at +25%, trail 20% under peak (floor +15%) from +40%, TP +80%, early exit if 30s sells > 2.5x buys and below entry / dev sell / 90s no trades, 10-min time stop",
                notes=f"Paper only, virtual fill, yolo ledger, launch sniper. mint {c.mint}; https://pump.fun/coin/{c.mint}. Ledger balance {bal:.2f}; free cash {bal - p.stake:.2f} while open. {lbl}. curve {c.curve}; dev {c.dev}; SOLUSD {p.sol}. Managed by yolo_snipe.py (not the 15s watcher).")
            rows.append(r); W.write_ledger(fields, rows)
            p.row_key = (r["date"], r["asset"]); p.bal = bal; p.test = bool(ts_)
            W.append_event({"time": W.ts(t), "coin": f"{c.sym} {c.name}", "event": "open (snipe)", "price": ep, "net": None, "balance": bal, "stop": p.stop * p.sol, "target": p.tp * p.sol})
        finally: fcntl.flock(lf, fcntl.LOCK_UN)

def auto_reset():
    """Standing rule (Vangel 20:39 Oct 20): if yolo ledger < $25 and no yolo trade open, reset to $200 (paper)."""
    with open(W.LOCKFILE, "a+") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            fields, rows = W.read_ledger(); bal = W.ledger_balance(rows)
            if bal is None or bal >= FLOOR: return None
            if any(r.get("mode", "").startswith("yolo") and "OPEN" in r.get("decision", "").upper() for r in rows): return None
            t = now(); shutil.copy2(W.LEDGER, W.LEDGER + ".bak-" + t.strftime("%Y%m%d-%H%M%S") + "-autoreset")
            r = {k: "" for k in fields}; d = t.strftime("%Y-%m-%d %H:%M")
            r.update(date=d, mode="yolo-meme", decision="LEDGER RESET",
                     reasoning=f"Standing rule: auto-reset below $25 at {d[11:]} {t.strftime('%b')} {t.day} (ledger ${bal:.2f}). Prior results kept above as history.",
                     notes=f"Paper only. auto-reset below $25: {bal:.2f} -> {RESET_TO:.2f}. Ledger balance {RESET_TO:.2f}; free cash {RESET_TO:.2f}. yolo ledger {bal:.2f} -> {RESET_TO:.2f} (reset).")
            rows.append(r); W.write_ledger(fields, rows)
            n, inj = reset_injections(rows)
            W.append_event({"time": W.ts(t), "coin": None, "event": "reset", "price": None, "net": None,
                            "old_balance": round(bal, 2), "new_balance": RESET_TO, "balance": RESET_TO, "reason": "auto-reset below $25",
                            "resets_total": n, "injected_total": round(inj, 2)})
            log(f"AUTO-RESET ledger {bal:.2f} -> {RESET_TO:.2f} (reset #{n}, ${inj:.2f} injected in total - count this against P&L)")
            return bal
        finally: fcntl.flock(lf, fcntl.LOCK_UN)

def ledger_close(p, reason, exit_sol, px_sol):
    with open(W.LOCKFILE, "a+") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            fields, rows = W.read_ledger()
            r = next((x for x in rows if (x["date"], x["asset"]) == p.row_key and "OPEN" in x["decision"]), None)
            if not r: log("WARN open row vanished"); return
            bal = W.ledger_balance(rows)
            entry = W.fnum(r["entry"]); ex = exit_sol * p.sol; px = px_sol * p.sol
            net = p.tokens * ex - p.stake; nb = round(bal + net, 2)
            gross = (px_sol / p.mkt - 1) * 100; netp = (ex / entry - 1) * 100
            t = now(); stamp = f"{t.strftime('%H:%M')} {t.strftime('%b')} {t.day}"
            r.update(decision="TRADE (CLOSED)", exit_price=W.fmt_px(ex), result_pct_gross=f"{gross:.2f}",
                     fees_slippage_pct=f"{gross - netp:.1f}", result_pct_net=f"{netp:.2f}", result_R=f"{netp / (abs(X['sl']) * 100):.2f}")
            r["notes"] = r["notes"].rstrip() + f" CLOSED {stamp} Skopje by launch sniper ({reason}): curve price {W.fmt_px(px)}; exit {W.fmt_px(ex)} on ~{p.tokens:,.1f} tokens; held {int(time.time()-p.t)}s; net {net:+.2f} USD ({netp:+.1f}%); yolo ledger {bal:.2f} -> {nb:.2f}. Paper only."
            W.write_ledger(fields, rows)
            W.append_event({"time": W.ts(t), "coin": f"{p.c.sym} {p.c.name}", "event": f"close ({reason})", "price": px, "exit_price": ex, "net": round(net, 2), "balance": nb})
        finally: fcntl.flock(lf, fcntl.LOCK_UN)

# ---------------- entries
async def open_sim(c, st, kind):
    if not S.sol_usd: return
    if len(S.sims) >= MAX_SIMS: jl(DRY, {"time": W.ts(), "event": "sim_skip", "kind": kind, "mint": c.mint, "reason": "MAX_SIMS"}); return
    p, why = await fill(c, kind, SIM_STAKE)
    base = {"time": W.ts(), "kind": kind, "mint": c.mint, "sym": c.sym}
    if not p: jl(DRY, {**base, "event": "sim_no_fill", "reason": why, "stats": st}); return
    S.sims.append(p)
    jl(DRY, {**base, "event": "sim_open", "id": p.id, "stats": st, "stake": SIM_STAKE, "mkt_usd": p.mkt * p.sol,
             "entry_usd": p.entry * p.sol, "entry_cost_pct": round((p.entry / p.mkt - 1) * 100, 2)})

async def open_real(c, st):
    try:
        g = await asyncio.to_thread(gates, c.mint)
        if g: jl(DRY, {"time": W.ts(), "event": "real_skip", "mint": c.mint, "sym": c.sym, "reason": g}); return
        bal = W.ledger_balance((await asyncio.to_thread(W.read_ledger))[1])
        p, why = await fill(c, "real", round(bal * STAKE_FRAC, 2))
        if not p:
            jl(DRY, {"time": W.ts(), "event": "no_fill", "kind": "real", "mint": c.mint, "sym": c.sym, "reason": why})
            log(f"NO FILL {c.sym} {c.mint}: {why}"); return
        await asyncio.to_thread(ledger_open, p, st); p.save(); S.real = p
        jl(DRY, {"time": W.ts(), "event": "open", "kind": "real", "id": p.id, "mint": c.mint, "sym": c.sym, "stats": st,
                 "stake": p.stake, "mkt_usd": p.mkt * p.sol, "entry_usd": p.entry * p.sol})
        log(f"ARMED OPEN {c.sym} {c.mint} ${p.stake:.2f} entry cost {(p.entry / p.mkt - 1) * 100:+.1f}% vs curve {st}")
    finally:
        S.entering = False

def on_signal(c, st):
    a = armed()
    rec = {"time": W.ts(), "event": "buy_signal" if a else "would_buy", "mint": c.mint, "sym": c.sym, "name": c.name,
           "stats": st, "price_usd": c.px * (S.sol_usd or 0), "detect_to_signal_s": st["age"]}
    spawn(open_sim(c, st, "signal"))
    if a:
        if S.real or S.entering: rec["skipped"] = "position busy"
        elif not S.sol_usd: rec["skipped"] = "no SOL price"
        else: S.entering = True; spawn(open_real(c, st))
    jl(DRY, rec); log(f"{'SIGNAL' if a else 'WOULD-BUY'} {c.sym} {c.mint} {st}" + (f" ({rec['skipped']})" if "skipped" in rec else ""))

def maybe_enter(c):
    if c.fired: return
    ok, st, fails = c.passes(); S.cnt["eval"] += 1
    in_window = st["age"] >= F["min_age"]
    if in_window:
        for f_ in fails: S.fails[f_] = S.fails.get(f_, 0) + 1
    if st["mc_mult"] > 15 and st["buys"] == 0: c.fired = True; return  # non-standard curve / unindexed, ignore
    if ok: c.fired = True; on_signal(c, st); return
    if in_window and len(fails) == 1 and fails[0] != "age" and not c.nm_fired:
        c.nm_fired = True; spawn(open_sim(c, st, "nearmiss:" + fails[0]))
    if fails != ["age"] and S.cnt["eval"] % 25 == 0: log(f"debug eval {c.sym} {c.mint} mc0={c.mc0:.1f} {st} fails={fails}")

# ---------------- exits
def manage(coin=None):
    for p in positions():
        if p.closing or (coin and p.c is not coin): continue
        reason = p.check()
        if reason: p.closing = True; spawn(close_pos(p, reason))

async def close_pos(p, reason):
    try:
        await asyncio.sleep(COST["fill_delay_s"])
        await refresh_curves([p.c])
        ex = p.proceeds_sol() / p.tokens; pnl = (ex / p.entry - 1) * 100
        rec = {"time": W.ts(), "event": "close" if p.armed else "sim_close", "kind": p.kind, "id": p.id, "mint": p.c.mint, "sym": p.c.sym,
               "reason": reason, "held_s": int(time.time() - p.t), "stake": p.stake, "entry_usd": p.entry * p.sol, "exit_usd": ex * p.sol,
               "gross_pct": round((p.c.px / p.mkt - 1) * 100, 1), "pnl_pct": round(pnl, 1), "net_usd": round(p.tokens * ex * p.sol - p.stake, 2),
               "peak_pct": round((p.peak / p.entry - 1) * 100, 1)}
        if p.armed:
            await asyncio.to_thread(ledger_close, p, reason, ex, p.c.px)
            S.real = None
            await asyncio.to_thread(auto_reset)
            try: os.remove(POSF)
            except Exception: pass
            if p.test: test_used(); log(f"test batch: {(test_state() or {}).get('remaining', 0)} left")
            log(f"CLOSE {p.c.sym} {reason} {pnl:+.1f}% net ${rec['net_usd']:+.2f}")
        else:
            S.sims.remove(p)
        jl(DRY, rec)
    except Exception:
        log(f"ERROR closing {p.id}, will retry: " + traceback.format_exc()); p.closing = False

# ---------------- loops
async def ticker():
    k = 0
    while True:
        await asyncio.sleep(2); k += 1
        if os.path.exists(STOPF):
            log("STOP file seen; exiting")
            for f_ in (STOPF, PIDF):
                try: os.remove(f_)
                except Exception: pass
            os._exit(0)
        try:
            if k % 30 == 0:
                log(f"heartbeat 60s: {S.cnt} tracked={len(S.coins)} real={S.real.c.sym if S.real else None} sims={len(S.sims)} "
                    f"ws={'up' if S.ws_up else 'DOWN'} fail_counts={dict(sorted(S.fails.items(), key=lambda x: -x[1]))}")
                for kk in S.cnt: S.cnt[kk] = 0
                S.fails.clear()
            manage()
            t = time.time(); held = {p.c.mint for p in positions()}
            old = [m for m, c in S.coins.items() if t - c.t0 > 75 and m not in held]
            for m in old: S.coins.pop(m, None)
            if old: await send({"method": "unsubscribeTokenTrade", "keys": old})
        except Exception: log("ERROR ticker " + traceback.format_exc())

async def poller():
    while True:
        t0 = time.time()
        try:
            held = [p.c for p in positions()]
            live = [c for c in S.coins.values() if not c.fired and t0 - c.t0 <= F["max_age"] + 2]
            await refresh_curves(live + held)
            for c in live:   # curve moved but the websocket delivered no trades: stream isn't working for this coin
                if c.ws_ok and not c.trades and t0 - c.t0 > 10 and c.mc >= F["min_mc_mult"] * c.mc0:
                    c.ws_ok = False; log(f"WARN no websocket trades for {c.sym} {c.mint} despite curve move; using trades API")
            pos_c = [c for c in dict.fromkeys(held) if not c.ws_ok and t0 - c.last_tcheck >= 3]
            cands = [c for c in live if not c.ws_ok and t0 - c.t0 >= F["min_age"]
                     and c.mc >= F["min_mc_mult"] * c.mc0 and t0 - c.last_tcheck >= 4]
            for c, cand in [(c, False) for c in pos_c] + [(c, True) for c in cands[:6]]:
                c.last_tcheck = t0
                tl = await asyncio.to_thread(swap_trades, c.mint, cand)
                if tl is not None: c.set_trades(tl)
            for c in live: maybe_enter(c)
        except Exception as e:
            log(f"WARN poller: {type(e).__name__}: {e}")
        try: manage()
        except Exception: log("ERROR manage " + traceback.format_exc())
        await asyncio.sleep(max(0.2, 1.0 - (time.time() - t0)))

async def run_ws():
    while True:
        try:
            async with websockets.connect(WS, ping_interval=20, open_timeout=15, max_size=2**22) as ws:
                S.ws = ws
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                if S.coins:   # trades during the disconnect are missing: these coins fall back to the trades API
                    for c in S.coins.values(): c.ws_ok = False
                    await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": list(S.coins)}))
                S.ws_up = True; log(f"connected {WS} ({'ARMED' if armed() else 'OBSERVE'}; arms at {ARM_AT})")
                async for raw in ws:
                    t = time.time(); m = json.loads(raw); tx = m.get("txType")
                    if tx == "create":
                        S.cnt["create"] += 1
                        c = Coin(m, t); S.coins[c.mint] = c
                        await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [c.mint]}))
                        S.lat_n += 1
                        if S.lat_n % 15 == 1 and m.get("signature"):
                            threading.Thread(target=sample_latency, args=(m["signature"], t, c.mint), daemon=True).start()
                    elif tx in ("buy", "sell"):
                        S.cnt["trade"] += 1
                        c = S.coins.get(m.get("mint"))
                        if not c: continue
                        c.add(m, t)
                        manage(c)
                        if not c.fired and t - c.t0 <= F["max_age"]: maybe_enter(c)
        except Exception as e:
            log(f"WARN ws: {type(e).__name__}: {e}; reconnect in 3s")
        S.ws_up = False; S.ws = None
        await asyncio.sleep(3)

def reattach():
    fields, rows = W.read_ledger()
    op = [r for r in rows if r.get("mode") == MODE_TAG and "OPEN" in r.get("decision", "").upper()]
    if not op: return
    r = op[-1]; st = {}
    try: st = json.load(open(POSF))
    except Exception: pass
    mint = W.find_mint(r)
    if st.get("mint") != mint: st = {}
    sm = re.search(r"SOLUSD (\d+(?:\.\d+)?)", r["notes"])
    sol = st.get("sol") or (float(sm.group(1)) if sm else None)
    for _ in range(60):
        if sol: break
        time.sleep(0.5); sol = S.sol_usd
    if not sol:
        log(f"ERROR reattach: no SOL/USD for open row {r['date']} {r['asset']}; refusing to start blind"); sys.exit(1)
    if not st:
        cm = re.search(r"curve (\w+)", r["notes"]); dm = re.search(r"dev (\w+)", r["notes"])
        e = W.fnum(r["entry"]) / sol; stake, tokens = W.stake_and_tokens(r, W.fnum(r["entry"]))
        st = {"mint": mint, "curve": cm.group(1) if cm else None, "dev": dm.group(1) if dm else None, "sym": r["asset"].split()[0], "name": "",
              "t": W.parse_row_time(r).timestamp(), "entry": e, "stake": stake, "tokens": tokens,
              "stop": W.fnum(r["stop"]) / sol, "tp": W.fnum(r["target"]) / sol, "peak": e, "stage": "initial", "test": "snipe test" in r["notes"]}
    # older position files have no sol_net/mkt: estimate them from the stake and the old +3% fill
    sol_net = st.get("sol_net") or (st["stake"] / sol - COST["tx_sol"]) * (1 - COST["fee"])
    mkt = st.get("mkt") or st["entry"] / 1.03
    c = Coin({"mint": mint, "name": st.get("name", ""), "symbol": st["sym"], "traderPublicKey": st.get("dev"), "bondingCurveKey": st.get("curve")}, st["t"])
    c.fired = c.nm_fired = True; c.ws_ok = False; c.last_trade_ts = time.time(); c.set_px(mkt)
    try:
        cv = rpc_curves([c.curve]) if c.curve else {}
        if c.curve in cv: c.set_curve(*cv[c.curve])
        else: log("WARN reattach: no curve state; marking at entry until the poller refreshes it")
    except Exception as e: log(f"WARN reattach price: {e}")
    p = Pos(c, "real", st["stake"], sol, sol_net, st["tokens"], mkt, t=st["t"])
    p.entry = st["entry"]; p.stop = st["stop"]; p.tp = st["tp"]; p.peak = max(st["peak"], p.peak); p.stage = st["stage"]
    p.row_key = (r["date"], r["asset"]); p.test = st.get("test", False)
    S.coins[mint] = c; S.real = p
    log(f"REATTACHED open {MODE_TAG} row {p.row_key} stop {p.stop*p.sol} stage {p.stage} mark {p.mark()*p.sol}")

async def main():
    with open(PIDF, "w") as f: f.write(str(os.getpid()))
    threading.Thread(target=sol_px_loop, daemon=True).start()
    for _ in range(20):
        if S.sol_usd: break
        time.sleep(0.5)
    reattach()
    if not S.real: auto_reset()
    log(f"yolo_snipe start pid {os.getpid()} PAPER ONLY filter={F} exits={X} costs={COST} stake_frac={STAKE_FRAC}")
    await asyncio.gather(run_ws(), ticker(), poller())

if __name__ == "__main__": asyncio.run(main())
