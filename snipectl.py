#!/usr/bin/env python3
"""
snipectl.py - control the history download + backtest job (used from the shell and from Hermes/Telegram).
  python3 snipectl.py status              progress of the current job
  python3 snipectl.py start [hours]       start a download job (default 24 h); backtest + Telegram message when it ends
  python3 snipectl.py stop                stop the job (data is kept; start again to continue collecting)
  python3 snipectl.py backtest [args]     backtest the data collected so far, in the background; result arrives on Telegram
                                          args go to backtest_snipe.py, e.g.  --set min_buyers=6  --delay 3  --no-costs
  python3 snipectl.py results             last backtest results
  python3 snipectl.py log [n]             last n lines of the download log
  python3 snipectl.py job                 (what the systemd service runs)
"""
import json, os, shutil, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data"); HIST = os.path.join(DATA, "history")
STATE = os.path.join(DATA, "job.json"); FETCH_LOG = os.path.join(DATA, "fetch.log")
RESULTS = os.path.join(DATA, "backtest-results.txt")
UNIT = "trading-bot-download.service"
HERMES = os.path.expanduser("~/.local/bin/hermes"); HERMES_HOME = "/mnt/vault/hermes"
DEFAULT_HOURS = 24
os.environ.setdefault("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")   # systemctl --user from the Hermes gateway

def load():
    try: return json.load(open(STATE))
    except Exception: return {}
def save(st):
    os.makedirs(DATA, exist_ok=True); json.dump(st, open(STATE + ".tmp", "w")); os.replace(STATE + ".tmp", STATE)

def notify(text):
    try:
        subprocess.run([HERMES, "send", "-q", "--to", "telegram", text], env={**os.environ, "HERMES_HOME": HERMES_HOME},
                       timeout=60, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e: print(f"notify failed: {e}")

def systemctl(*a): return subprocess.run(["systemctl", "--user", *a], capture_output=True, text=True)
def service_active(): return systemctl("is-active", UNIT).stdout.strip() in ("active", "activating")
def hm(sec): sec = max(0, int(sec)); return f"{sec // 3600}h{sec % 3600 // 60:02d}m"
def clock(ts): return time.strftime("%H:%M %b %d", time.localtime(ts))

def coin_stats():
    n, last = 0, 0.0
    try:
        with open(os.path.join(HIST, "coins.jsonl")) as f:
            for line in f:
                n += 1
                if line.strip(): last = json.loads(line).get("fetched", last)
    except FileNotFoundError: pass
    return n, last

def status():
    st = load(); n, last = coin_stats(); now = time.time()
    du = sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(HIST) for f in fs) if os.path.isdir(HIST) else 0
    lines = [f"job: {st.get('phase', 'never started')}" + (" (service running)" if service_active() else "")]
    if st.get("start"):
        lines.append(f"started {clock(st['start'])}, download ends {clock(st['end'])}"
                     + (f" ({hm(st['end'] - now)} left)" if st.get("phase") == "downloading" else ""))
    lines.append(f"coins saved: {n}" + (f", last one {int(now - last)}s ago" if last else "") + f"; data {du / 1e6:.0f} MB")
    lines.append(f"SSD free: {shutil.disk_usage(HERE).free / 1e9:.0f} GB")
    if os.path.exists(RESULTS): lines.append(f"last backtest: {clock(os.path.getmtime(RESULTS))} (snipectl results)")
    return "\n".join(lines)

def start(hours):
    if service_active(): return "already running:\n" + status()
    now = time.time(); save({"phase": "downloading", "start": now, "end": now + hours * 3600, "hours": hours})
    systemctl("reset-failed", UNIT); r = systemctl("start", UNIT)
    if r.returncode: return f"failed to start {UNIT}: {r.stderr.strip()}"
    return f"started: downloading for {hours:g} h (until {clock(now + hours * 3600)}), then backtest + Telegram message"

def stop():
    st = load()
    if st.get("phase") in ("downloading", "backtesting"): st["phase"] = "stopped"; save(st)
    systemctl("stop", UNIT)
    return f"stopped. {coin_stats()[0]} coins kept; `snipectl backtest` works on them, `snipectl start` collects more"

def run_backtest(extra):
    """Backtest current data; returns the message text and writes RESULTS."""
    py = [sys.executable, os.path.join(HERE, "backtest_snipe.py")]
    main = subprocess.run(py + ["--quiet", *extra], capture_output=True, text=True, cwd=HERE)
    if main.returncode: return "backtest failed:\n" + (main.stderr or main.stdout)[-1500:]
    out = main.stdout.strip()
    if not extra:   # default run: also show how much the costs/latency model matters
        free = subprocess.run(py + ["--quiet", "--no-costs", "--delay", "0", "--detect", "0", "--out", os.path.join(DATA, "backtest-nocost.jsonl")],
                              capture_output=True, text=True, cwd=HERE)
        per = [l for l in free.stdout.splitlines() if l.startswith("per-trade")]
        if per: out += "\n\nsame trades with zero fees, zero delay (best case): " + per[0]
    detail = subprocess.run([sys.executable, os.path.join(HERE, "analyze_snipe.py"), os.path.join(DATA, "backtest-dryrun.jsonl")],
                            capture_output=True, text=True, cwd=HERE).stdout
    header = f"backtest {clock(time.time())}" + (f" with {' '.join(extra)}" if extra else "")
    with open(RESULTS, "w") as f: f.write(f"{header}\n{out}\n\n{detail}")
    return f"{header}\n{out}"

def job():
    """systemd entry point: download until the job's end time (resumes after restarts), then backtest and notify."""
    st = load()
    if st.get("phase") == "downloading":
        if not st.get("announced"):
            notify(f"📥 trading-bot: history download started, runs until {clock(st['end'])}. I'll send the backtest when it's done.")
            st["announced"] = True; save(st)
        left = st["end"] - time.time()
        if left > 60:
            with open(FETCH_LOG, "a") as log:
                r = subprocess.run([sys.executable, os.path.join(HERE, "fetch_history.py"), "--hours", f"{left / 3600:.4f}"],
                                   stdout=log, stderr=subprocess.STDOUT, cwd=HERE)
            if r.returncode: sys.exit(r.returncode)   # systemd restarts us and we resume with the remaining time
            if load().get("phase") != "downloading": return   # stopped meanwhile
        st = load(); st["phase"] = "backtesting"; save(st)
    if st.get("phase") == "backtesting":
        msg = run_backtest([])
        st = load(); st["phase"] = "done"; st["finished"] = time.time(); save(st)
        notify(f"✅ trading-bot: download finished ({coin_stats()[0]} coins).\n\n{msg}\n\nFull detail: ask me for `snipectl results`.")

def main():
    a = sys.argv[1:] or ["status"]; cmd = a[0]
    if cmd == "status": print(status())
    elif cmd == "start": print(start(float(a[1]) if len(a) > 1 else DEFAULT_HOURS))
    elif cmd == "stop": print(stop())
    elif cmd == "results": print(open(RESULTS).read() if os.path.exists(RESULTS) else "no backtest yet")
    elif cmd == "log": print(subprocess.run(["tail", "-n", a[1] if len(a) > 1 else "20", FETCH_LOG], capture_output=True, text=True).stdout or "no log yet")
    elif cmd == "backtest":   # detach: big datasets take longer than a chat tool call may wait
        unit = f"trading-bot-backtest-{int(time.time())}"
        r = subprocess.run(["systemd-run", "--user", "--unit", unit, "--setenv", f"PYTHONPATH={os.environ.get('PYTHONPATH', '')}",
                            sys.executable, os.path.abspath(__file__), "_backtest", *a[1:]], capture_output=True, text=True)
        print(f"backtest running on {coin_stats()[0]} coins; result comes as a Telegram message" if r.returncode == 0 else f"failed: {r.stderr.strip()}")
    elif cmd == "_backtest": notify("📊 trading-bot " + run_backtest(a[1:]))
    elif cmd == "job": job()
    else: print(__doc__); sys.exit(2)

if __name__ == "__main__": main()
