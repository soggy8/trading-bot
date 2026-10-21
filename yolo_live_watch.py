#!/usr/bin/env python3
"""
yolo_live_watch.py - PAPER ONLY live watcher for open yolo-meme trades.

Never touches a wallet, exchange or pump.fun login. It only reads public
DexScreener price data and edits the paper ledger yolo-log.csv.

Every 15 s: re-read the ledger, collect OPEN yolo-meme rows, fetch prices in
one batched DexScreener call, apply exit rules, write changes under a lock.

Usage:
  yolo_live_watch.py              run forever
  yolo_live_watch.py --once --dry-run   one pass, print what it would do, no writes
"""
import csv, fcntl, io, json, os, re, shutil, sys, time, argparse, traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

DESK = os.environ.get("YOLO_DESK", "/workspace/trading-desk")
LEDGER = os.path.join(DESK, "yolo-log.csv")
LOCKFILE = os.path.join(DESK, "yolo-log.csv.lock")
EVENTS = os.path.join(DESK, "yolo-live-events.jsonl")
LOG = os.path.join(DESK, "yolo-live-watch.log")
TZ = ZoneInfo("Europe/Skopje")

POLL_S = 15
MAX_REQ_PER_MIN = 50          # hard cap, under ~60/min
BATCH = 30                    # DexScreener max pairs per call
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) yolo-paper-watch/1.0 (paper trading, read-only)",
      "Accept": "application/json"}
LIQ_FLOOR = 20_000.0
MISS_LIMIT = 3
MAX_HOLD = timedelta(hours=4)
BE_TRIGGER = 1.25
TP_FILL = 0.99
STOP_FILL = 0.98
HEARTBEAT_EVERY = 4           # log a heartbeat every N passes (~1 min); always on events

BASE58 = r"[1-9A-HJ-NP-Za-km-z]{32,44}"

miss_counts = {}              # pair -> consecutive missing count
pair_cache = {}               # row key -> pair address (from token lookup)
req_times = []


# ---------------------------------------------------------------- utils
def now():
    return datetime.now(TZ)

def ts(dt=None):
    return (dt or now()).strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    line = f"{ts()} {msg}"
    try:
        with open(LOG, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass
    print(line, flush=True)

def fnum(s):
    try:
        return float(str(s).replace(",", "").replace("$", "").strip())
    except Exception:
        return None

def fmt_px(x):
    if x == 0:
        return "0"
    s = f"{x:.10g}" if x >= 1e-4 else f"{x:.6e}"
    if "e" in s:  # keep plain decimal like the ledger
        s = f"{x:.12f}".rstrip("0").rstrip(".")
    return s

def row_key(r):
    return (r.get("date", ""), r.get("asset", ""))

def is_open_yolo(r):
    return (r.get("mode", "").strip() == "yolo-meme"
            and "OPEN" in r.get("decision", "").upper())

def coin_name(r):
    a = r.get("asset", "")
    return a.split(" (")[0].strip() or a


# ---------------------------------------------------------------- ledger io
def read_ledger():
    with open(LEDGER, newline="") as f:
        rd = csv.DictReader(f)
        return rd.fieldnames, list(rd)

def write_ledger(fields, rows):
    stamp = now().strftime("%Y%m%d-%H%M%S")
    bak = f"{LEDGER}.bak-{stamp}"
    shutil.copy2(LEDGER, bak)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, lineterminator="\r\n")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    tmp = f"{LEDGER}.tmp-{os.getpid()}"
    with open(tmp, "w", newline="") as f:
        f.write(buf.getvalue())
        f.flush()
        os.fsync(f.fileno())
    shutil.copymode(LEDGER, tmp)
    os.replace(tmp, LEDGER)
    return bak

def append_event(ev):
    with open(EVENTS, "a") as f:
        f.write(json.dumps(ev) + "\n")


# ---------------------------------------------------------------- parsing
def parse_row_time(r):
    try:
        return datetime.strptime(r["date"].strip()[:16], "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
    except Exception:
        return None

def find_pair(r):
    notes = r.get("notes", "") + " " + r.get("asset", "")
    m = re.search(r"\b[Pp]air\s+(" + BASE58 + r")", notes)
    if m:
        return m.group(1)
    m = re.search(r"dexscreener\.com/solana/(" + BASE58 + r")", notes)
    if m:
        return m.group(1)
    return pair_cache.get(row_key(r))

def find_mint(r):
    m = re.search(r"\((" + BASE58 + r")\)", r.get("asset", ""))
    if m:
        return m.group(1)
    m = re.search(r"\bmint\s+(" + BASE58 + r")", r.get("notes", ""))
    return m.group(1) if m else None

def stake_and_tokens(r, entry):
    size = r.get("size", "")
    m = re.search(r"\$\s*([\d,]+(?:\.\d+)?)", size)
    stake = fnum(m.group(1)) if m else None
    m = re.search(r"~?\s*([\d,]+(?:\.\d+)?)\s*tokens", size)
    tokens = fnum(m.group(1)) if m else None
    if tokens is None and stake and entry:
        tokens = stake / entry
    if stake is None and tokens and entry:
        stake = tokens * entry
    return stake, tokens

def init_stop_pct(r, entry, stop):
    m = re.search(r"-?\s*(\d+(?:\.\d+)?)\s*%\s*stop", r.get("risk_pct", ""))
    if m:
        return float(m.group(1))
    if entry and stop and stop < entry:
        return (entry - stop) / entry * 100
    return 30.0

_MON = {m: i for i, m in enumerate(
    ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"], 1)}

def ledger_balance(rows):
    """Latest yolo ledger balance, taken from the most recent dated note."""
    best = None  # (dt, order, value)
    for i, r in enumerate(rows):
        notes = r.get("notes", "")
        rdt = parse_row_time(r) or datetime(1970, 1, 1, tzinfo=TZ)
        for m in re.finditer(r"Ledger balance\s+\$?(\d+(?:\.\d+)?)", notes):
            cand = (rdt, i, m.start(), float(m.group(1)))
            if best is None or cand[:3] > best[:3]:
                best = cand
        for m in re.finditer(r"yolo ledger\s+\$?(\d+(?:\.\d+)?)\s*->\s*\$?(\d+(?:\.\d+)?)", notes):
            cdt = rdt
            # look back for the CLOSED stamp preceding this transition
            seg = notes[:m.start()]
            cm = list(re.finditer(r"CLOSED\s+(\d{1,2}):(\d{2})(?:\s+([A-Z][a-z]{2})\s+(\d{1,2}))?", seg))
            if cm:
                c = cm[-1]
                hh, mm = int(c.group(1)), int(c.group(2))
                if c.group(3) and c.group(3) in _MON:
                    cdt = rdt.replace(month=_MON[c.group(3)], day=int(c.group(4)), hour=hh, minute=mm)
                else:
                    cdt = rdt.replace(hour=hh, minute=mm)
                    if cdt < rdt:
                        cdt += timedelta(days=1)
            cand = (cdt, i, m.start(), float(m.group(2)))
            if best is None or cand[:3] > best[:3]:
                best = cand
    return best[3] if best else None


# ---------------------------------------------------------------- network
def rate_ok():
    t = time.time()
    while req_times and t - req_times[0] > 60:
        req_times.pop(0)
    return len(req_times) < MAX_REQ_PER_MIN

def get_json(url):
    if not rate_ok():
        raise RuntimeError("local rate cap reached, skipping request")
    req_times.append(time.time())
    resp = requests.get(url, headers=UA, timeout=10)
    if resp.status_code == 429:
        raise RuntimeError("HTTP 429 rate limited")
    resp.raise_for_status()
    return resp.json()

def fetch_pairs(pairs):
    """Return dict pair(lower)->pair json, or None on network error."""
    out = {}
    for i in range(0, len(pairs), BATCH):
        chunk = pairs[i:i + BATCH]
        url = "https://api.dexscreener.com/latest/dex/pairs/solana/" + ",".join(chunk)
        data = get_json(url)
        lst = data.get("pairs") or ([data["pair"]] if data.get("pair") else [])
        for p in lst or []:
            if p and p.get("pairAddress"):
                out[p["pairAddress"].lower()] = p
    return out

def lookup_pair_by_mint(mint):
    data = get_json(f"https://api.dexscreener.com/latest/dex/tokens/{mint}")
    ps = [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
    if not ps:
        return None
    ps.sort(key=lambda p: (p.get("liquidity") or {}).get("usd") or 0, reverse=True)
    return ps[0]["pairAddress"]


# ---------------------------------------------------------------- decisions
def decide(r, p, missing):
    """Return action dict or None. p = pair json or None."""
    entry, stop, tp = fnum(r["entry"]), fnum(r["stop"]), fnum(r["target"])
    opened = parse_row_time(r)
    t = now()
    price = fnum(p.get("priceUsd")) if p else None
    liq = fnum((p.get("liquidity") or {}).get("usd")) if p else None
    act = {"price": price, "liq": liq}

    if price is not None:
        if tp and price >= tp:
            return {**act, "type": "close", "reason": "TP", "base": tp, "exit": tp * TP_FILL, "slip": 1.0}
        reasons = []
        if stop and price <= stop:
            reasons.append("SL")
        if liq is not None and liq < LIQ_FLOOR:
            reasons.append("liquidity under $20K")
        if opened and t - opened > MAX_HOLD:
            reasons.append("4h time stop")
        if reasons:
            return {**act, "type": "close", "reason": " + ".join(reasons),
                    "base": price, "exit": price * STOP_FILL, "slip": 2.0}
        if entry and stop is not None and stop < entry and price >= BE_TRIGGER * entry:
            return {**act, "type": "stop_move", "new_stop": entry}
        return None

    # no price available
    if missing >= MISS_LIMIT:
        last = fnum(r.get("_last_price")) or None
        return {**act, "type": "close", "reason": f"pair missing on {MISS_LIMIT} consecutive fetches",
                "base": last, "exit": (last * STOP_FILL) if last else 0.0, "slip": 2.0}
    if opened and t - opened > MAX_HOLD and r.get("_last_price"):
        last = fnum(r["_last_price"])
        return {**act, "type": "close", "reason": "4h time stop (no fresh price)",
                "base": last, "exit": last * STOP_FILL, "slip": 2.0}
    return None

last_prices = {}

def apply_action(r, a, balance):
    """Mutate row r in place. Return event dict."""
    t = now()
    stamp = f"{t.strftime('%H:%M')} {t.strftime('%b')} {t.day}"
    entry = fnum(r["entry"])
    coin = coin_name(r)
    if a["type"] == "stop_move":
        old = r["stop"]
        r["stop"] = r["entry"]
        gain = (a["price"] / entry - 1) * 100
        r["notes"] = (r.get("notes", "").rstrip() +
                      f" {stamp} Skopje: SL raised to breakeven {r['entry']} (from {old}) at +{gain:.1f}% "
                      f"(DexScreener {fmt_px(a['price'])}) by live watcher.")
        return {"time": ts(t), "coin": coin, "event": "stop_to_breakeven", "price": a["price"],
                "net": None, "balance": balance, "new_stop": entry, "old_stop": fnum(old)}

    stake, tokens = stake_and_tokens(r, entry)
    exitp = a["exit"]
    net = tokens * exitp - stake
    new_bal = round(balance + net, 2) if balance is not None else None
    gross_pct = ((a["base"] / entry - 1) * 100) if a.get("base") else -100.0
    net_pct = (exitp / entry - 1) * 100
    spct = init_stop_pct(r, entry, fnum(r["stop"]))
    r["decision"] = "TRADE (CLOSED)"
    r["exit_price"] = fmt_px(exitp)
    r["result_pct_gross"] = f"{gross_pct:.2f}"
    r["fees_slippage_pct"] = f"{a['slip']:.1f}"
    r["result_pct_net"] = f"{net_pct:.2f}"
    r["result_R"] = f"{net_pct / spct:.2f}" if spct else ""
    px = f"DexScreener {fmt_px(a['price'])}" if a.get("price") is not None else "no DexScreener price"
    liq = f", liq ${a['liq']:,.0f}" if a.get("liq") is not None else ""
    if a["reason"] == "TP":
        how = f"above TP {r['target']}; exit TP -1% slip = {fmt_px(exitp)}"
    else:
        basep = fmt_px(a["base"]) if a.get("base") else "n/a"
        how = f"exit price {basep} -2% = {fmt_px(exitp)}"
    bal_txt = f"yolo ledger {balance:.2f} -> {new_bal:.2f}" if balance is not None else "yolo ledger balance unknown"
    r["notes"] = (r.get("notes", "").rstrip() +
                  f" CLOSED {stamp} Skopje by live watcher ({a['reason']}): {px}{liq}; {how} on ~{tokens:,.1f} tokens; "
                  f"net {net:+.2f} USD ({net_pct:+.1f}%); {bal_txt}. Paper only.")
    return {"time": ts(t), "coin": coin, "event": f"close ({a['reason']})", "price": a.get("price"),
            "exit_price": exitp, "net": round(net, 2), "balance": new_bal}


# ---------------------------------------------------------------- main pass
pass_no = 0

def one_pass(dry=False):
    global pass_no
    pass_no += 1
    fields, rows = read_ledger()
    opens = [r for r in rows if is_open_yolo(r)]
    if not opens:
        if dry or pass_no % HEARTBEAT_EVERY == 1:
            log("heartbeat: 0 open yolo-meme rows")
        return []

    # resolve pairs
    for r in opens:
        if not find_pair(r):
            mint = find_mint(r)
            if mint:
                try:
                    pa = lookup_pair_by_mint(mint)
                    if pa:
                        pair_cache[row_key(r)] = pa
                        log(f"resolved pair {pa} for {coin_name(r)} via token lookup")
                except Exception as e:
                    log(f"WARN token lookup failed for {coin_name(r)}: {e}")
    pairs = sorted({find_pair(r) for r in opens if find_pair(r)})
    try:
        data = fetch_pairs(pairs) if pairs else {}
    except Exception as e:
        log(f"WARN fetch failed ({type(e).__name__}: {e}); will retry next pass")
        return []

    actions, status = [], []
    for r in opens:
        pa = find_pair(r)
        p = data.get(pa.lower()) if pa else None
        if pa:
            miss_counts[pa] = 0 if p else miss_counts.get(pa, 0) + 1
        missing = miss_counts.get(pa, MISS_LIMIT if not pa else 0)
        if p and fnum(p.get("priceUsd")):
            last_prices[row_key(r)] = fnum(p["priceUsd"])
        if row_key(r) in last_prices:
            r["_last_price"] = last_prices[row_key(r)]
        a = decide(r, p, missing)
        entry = fnum(r["entry"])
        stake, tokens = stake_and_tokens(r, entry)
        if p:
            price = fnum(p.get("priceUsd")); liq = fnum((p.get("liquidity") or {}).get("usd"))
            upnl = tokens * price * STOP_FILL - stake if tokens else None
            status.append(f"{coin_name(r)} px {fmt_px(price)} ({(price/entry-1)*100:+.1f}%) liq ${liq or 0:,.0f} "
                          f"SL {r['stop']} TP {r['target']} uPnL(after 2% slip) {upnl:+.2f}")
        else:
            status.append(f"{coin_name(r)} pair {pa} MISSING ({missing}/{MISS_LIMIT})")
        if a:
            actions.append((row_key(r), a))

    if dry or pass_no % HEARTBEAT_EVERY == 1 or actions:
        log("heartbeat: " + " | ".join(status))

    if not actions:
        return []
    if dry:
        for k, a in actions:
            log(f"DRY-RUN would apply {a['type']} {a.get('reason','')} to {k}")
        return actions

    events = []
    with open(LOCKFILE, "a+") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            fields, rows = read_ledger()          # re-read right before writing
            balance = ledger_balance(rows)
            by_key = {row_key(r): r for r in rows}
            changed = False
            for k, a in actions:
                r = by_key.get(k)
                if not r or not is_open_yolo(r):
                    log(f"skip {k}: no longer OPEN at write time")
                    continue
                if a["type"] == "stop_move":
                    e, s = fnum(r["entry"]), fnum(r["stop"])
                    if not (s is not None and e and s < e):
                        continue
                ev = apply_action(r, a, balance)
                if a["type"] == "close" and ev.get("balance") is not None:
                    balance = ev["balance"]
                events.append(ev)
                changed = True
            if changed:
                bak = write_ledger(fields, rows)
                for ev in events:
                    append_event(ev)
                    log(f"EVENT {json.dumps(ev)} (backup {os.path.basename(bak)})")
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)
    return events


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if args.once:
        one_pass(dry=args.dry_run)
        return
    log(f"watcher start pid {os.getpid()} (PAPER ONLY, poll {POLL_S}s)")
    while True:
        t0 = time.time()
        try:
            one_pass(dry=args.dry_run)
        except Exception as e:
            log(f"ERROR pass failed: {type(e).__name__}: {e}\n{traceback.format_exc()}")
        time.sleep(max(1.0, POLL_S - (time.time() - t0)))

if __name__ == "__main__":
    main()
