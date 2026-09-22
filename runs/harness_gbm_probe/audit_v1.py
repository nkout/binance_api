"""Year-wide audit of the v1 monthly tars.

Two passes:
  1. filename-only coverage (cheap, exact): collector instances, unique days, gaps
  2. column population, sampling N files per month

Funding rate updates every 8 h, so a CONSTANT value inside a 4 h file is normal,
not a defect. Only a constant -1 means "never collected".
"""
import tarfile, io, os, glob, re
import pandas as pd, numpy as np

DATA = "/home/nkout/projects/binance_api/data/v1_year"
N_SAMPLE = 3
OPT = ['opt_funding_rate_sample','opt_open_interest_sample','opt_long_short_ratio_sample',
       'opt_mark_price_sample','opt_index_price_sample','opt_spread_sample',
       'opt_long_force_exit_qty_sum','opt_short_force_exit_qty_sum']
SHORT = {c: c.replace('opt_','').replace('_sample','').replace('_qty_sum','') for c in OPT}
SIZES = {'month.2509.tar':260044800,'month.2510.tar':491089920,'month.2511.tar':449699840,
         'month.2512.tar':491950080,'month.2601.tar':447416320,'month.2602.tar':438405120,
         'month.2603.tar':398151680,'month.2604.tar':472104960,'month.2605.tar':483614720,
         'month.2606.tar':469985280,'month.2607.tar':478586880,'month.2608.tar':214272000}
NAME_RE = re.compile(r'out\.([a-z]+)\.(\d+)\.(\d{6})\.csv\.gz$')

def status(col, v):
    v = pd.to_numeric(v, errors='coerce')
    if v.notna().mean() < 0.5: return 'nan'
    u = v.dropna().unique()
    if len(u) <= 1:
        only = u[0] if len(u) else None
        if only == -1: return 'DEAD'
        if only == 0:  return 'zero'
        return 'ok8h' if 'funding' in col else 'const'
    return 'ok'

tars, skipped = [], []
for t in sorted(glob.glob(os.path.join(DATA, "month.*.tar"))):
    b = os.path.basename(t)
    (skipped if (b in SIZES and os.path.getsize(t) != SIZES[b]) else tars).append(t if True else b)
tars = [t for t in tars if os.path.basename(t) not in [os.path.basename(x) for x in skipped]]
if skipped: print(f"skipping incomplete: {', '.join(os.path.basename(x) for x in skipped)}\n")

print("PASS 1 — coverage from filenames (exact, no file reads)")
hdr = f"{'month':<7} {'files':>6} {'instances':<16} {'days':>5} {'exp':>4} {'files/day':>10}  gaps"
print(hdr); print('-'*max(len(hdr), 78))
alldays = set(); total_files = 0; permonth = {}
for t in tars:
    mon = os.path.basename(t).split('.')[1]
    with tarfile.open(t) as tf:
        names = [n for n in tf.getnames() if n.endswith('.csv.gz')]
    inst, days = {}, {}
    for n in names:
        m = NAME_RE.search(os.path.basename(n))
        if not m: continue
        pre, _, d = m.groups()
        inst[pre] = inst.get(pre, 0) + 1
        days.setdefault(d, 0); days[d] += 1
    ds = sorted(days)
    dts = [pd.Timestamp(f"20{d[:2]}-{d[2:4]}-{d[4:]}") for d in ds]
    exp = pd.Period(f"20{mon[:2]}-{mon[2:]}").days_in_month
    gaps = []
    for a, b in zip(dts, dts[1:]):
        if (b - a).days > 1: gaps.append(f"{a:%m-%d}->{b:%m-%d}")
    fpd = np.mean(list(days.values())) if days else 0
    permonth[mon] = dict(files=len(names), days=len(ds), exp=exp, inst=inst, fpd=fpd)
    alldays |= {f"20{d[:2]}-{d[2:4]}-{d[4:]}" for d in ds}
    total_files += len(names)
    istr = ' '.join(f"{k}:{v}" for k, v in sorted(inst.items()))
    print(f"{mon:<7} {len(names):>6} {istr:<16} {len(ds):>5} {exp:>4} {fpd:>10.1f}  "
          f"{('; '.join(gaps[:3]) + (' ...' if len(gaps) > 3 else '')) if gaps else '-'}")

print(f"\ntotal files {total_files:,} | unique calendar days {len(alldays)} | "
      f"{total_files*4/24:.0f} file-days of 4h coverage")
print("(files/day > 6 means two collector instances wrote the same hours -> dedupe needed)")

print("\nPASS 2 — column population")
hdr2 = f"{'month':<7} " + " ".join(f"{SHORT[c][:9]:>9}" for c in OPT)
print(hdr2); print('-'*len(hdr2))
res = {}
for t in tars:
    mon = os.path.basename(t).split('.')[1]
    with tarfile.open(t) as tf:
        names = sorted(n for n in tf.getnames() if n.endswith('.csv.gz'))
        pick = [names[int(i*(len(names)-1)/max(1, N_SAMPLE-1))] for i in range(N_SAMPLE)]
        st = {c: [] for c in OPT}
        for nm in pick:
            try:
                df = pd.read_csv(io.BytesIO(tf.extractfile(nm).read()), sep=';',
                                 compression='gzip', low_memory=False)
            except Exception:
                continue
            for c in OPT:
                st[c].append(status(c, df[c]) if c in df.columns else 'miss')
    agg = {c: (st[c][0] if len(set(st[c])) == 1 else '/'.join(sorted(set(st[c])))) if st[c] else '?'
           for c in OPT}
    res[mon] = agg
    print(f"{mon:<7} " + " ".join(f"{agg[c][:9]:>9}" for c in OPT))

print("\nlegend: ok=varying | ok8h=constant within a 4h file (normal for 8h-cadence funding)")
print("        DEAD=constant -1 (never collected) | zero=all zeros | miss=absent")
def usable(c):
    return [m for m, a in res.items() if 'DEAD' not in a[c] and 'miss' not in a[c] and 'zero' != a[c]]
for c in ['opt_funding_rate_sample','opt_open_interest_sample','opt_long_short_ratio_sample',
          'opt_long_force_exit_qty_sum']:
    u = usable(c)
    print(f"  {SHORT[c]:<14} usable in {len(u):>2}/{len(res)} months: {','.join(sorted(u))}")
