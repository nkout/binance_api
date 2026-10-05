"""Tests for snapshot_borrow.py with the network mocked: schema, idempotence, atomic writes, failure handling."""
import glob, gzip, json, os, sys, tempfile
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import r9_borrow as R
import snapshot_borrow as S
fails = 0


def check(name, cond, extra=""):
    global fails
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else "")); fails += (not cond)


calls = {"n": 0}
ASSETS = [dict(assetName=f"A{i}", specs=[dict(vipLevel="0", dailyInterestRate=str(0.0001 * (i + 1)), borrowLimit="1000.0"), dict(vipLevel="1", dailyInterestRate="0.00001", borrowLimit="9")]) for i in range(150)]
PI = [dict(symbol="A1USDT", markPrice="10.0", lastFundingRate="-0.0005"), dict(symbol="A2USDT", markPrice="5.0", lastFundingRate="0.0001"),
      dict(symbol="BTCUSDT_261225", markPrice="1.0", lastFundingRate="0.0"), dict(symbol="A3USDT", markPrice="2.0", lastFundingRate="")]


def fake_get(url, tries=5):
    calls["n"] += 1
    if "list-all" in url:
        return dict(data=fake_get.assets)
    if "premiumIndex" in url:
        return PI
    if "fundingRate" in url:
        import time
        now = int(time.time() * 1000)
        return [dict(fundingTime=now - k * 8 * 3600 * 1000, fundingRate="-0.0005") for k in range(30)]
    raise AssertionError(url)


R.get = fake_get; fake_get.assets = ASSETS
R.time.sleep = lambda s: None
d = tempfile.mkdtemp(prefix="snap_")
rc = S.main(["--out-dir", d])
files = glob.glob(os.path.join(d, "borrow_*.json.gz"))
check("writes exactly one file named for the UTC day, exit 0", rc == 0 and len(files) == 1 and not glob.glob(os.path.join(d, "*.tmp")))
snap = json.load(gzip.open(files[0], "rt"))
check("schema: assets keep VIP0 rate and limit only", len(snap["assets"]) == 150 and snap["assets"]["A0"] == dict(daily=0.0001, limit=1000.0))
check("schema: perps exclude delivery contracts and blank funding; candidates are the <= -0.01 % ones with FUND7",
      set(snap["perps"]) == {"A1USDT", "A2USDT"} and [c["symbol"] for c in snap["candidates"]] == ["A1USDT"] and snap["candidates"][0]["base"] == "A1")
check("FUND7 in the snapshot = (22 events within 7 d, the one at 168 h included) x -0.0005 / 21", abs(snap["candidates"][0]["fund7"] - (-0.0005 * 22 / 21)) < 1e-12, str(snap["candidates"][0]["fund7"]))
n0 = calls["n"]; rc2 = S.main(["--out-dir", d])
check("idempotent: second run the same UTC day makes no network calls and keeps the file", rc2 == 0 and calls["n"] == n0 and len(glob.glob(os.path.join(d, "*.json.gz"))) == 1)
mt = os.path.getmtime(files[0]); rc3 = S.main(["--out-dir", d, "--force"])
check("--force refetches and replaces atomically", rc3 == 0 and calls["n"] > n0 and not glob.glob(os.path.join(d, "*.tmp")))
d2 = tempfile.mkdtemp(prefix="snap_bad_"); fake_get.assets = ASSETS[:10]
check("failure: a short borrow list (endpoint changed) exits 1 and writes nothing", S.main(["--out-dir", d2]) == 1 and not glob.glob(os.path.join(d2, "*")))
def boom(url, tries=5): raise OSError("network down")
R.get = boom
check("failure: network error exits 1 and writes nothing", S.main(["--out-dir", d2]) == 1 and not glob.glob(os.path.join(d2, "*")))
print("\nFAILS:", fails)
sys.exit(fails)
