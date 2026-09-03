"""Effective ancestry diversity per run in manifest.test3.tsv, and the exact
power floors for trait-level paired tests at these run counts.

Diversity measure: the inverse Simpson / participation ratio
    A_eff = (sum_a N_a)^2 / sum_a N_a^2
which equals the number of ancestry arms when they carry equal effective
sample size and tends to 1 when a single arm dominates. It is the natural
"how many ancestries do you effectively have" statistic, and unlike the raw
arm count it does not treat an N=1,419 arm as a whole ancestry.
"""
import csv, math
from collections import defaultdict
from itertools import combinations

rows = list(csv.DictReader(open('/sessions/gallant-compassionate-gauss/mnt/nf-fine-mapping/testdata/manifest.test3.tsv'), delimiter='\t'))
runs = defaultdict(list)
for r in rows:
    runs[r['runId']].append((r['majorAncestry'], int(r['effectiveSampleSize'])))

def a_eff(ns):
    s = sum(ns)
    return s * s / sum(n * n for n in ns)

recs = []
for run, arms in runs.items():
    ns = [n for _, n in arms]
    recs.append((a_eff(ns), len(arms), max(ns) / min(ns), min(ns), max(ns),
                 ','.join(f'{a}:{n}' for a, n in sorted(arms, key=lambda t: -t[1]))))
recs.sort()

print(f'{"A_eff":>6} {"arms":>4} {"maxN/minN":>9} {"minN":>7} {"maxN":>7}  arms')
for ae, k, ratio, mn, mx, desc in recs:
    print(f'{ae:6.2f} {k:4d} {ratio:9.1f} {mn:7d} {mx:7d}  {desc}')

thr = 1.40
s1 = [r for r in recs if r[0] >= thr]
s2 = [r for r in recs if r[0] < thr]
print(f'\nStratum split at A_eff >= {thr}:')
print(f'  stratum 1 (balanced):   {len(s1)} runs, A_eff {min(r[0] for r in s1):.2f}-{max(r[0] for r in s1):.2f}')
print(f'  stratum 2 (token arm):  {len(s2)} runs, A_eff {min(r[0] for r in s2):.2f}-{max(r[0] for r in s2):.2f}')

def sign_p(k, n):
    """Exact two-sided sign-test p for k of n differences favouring one arm."""
    tail = sum(math.comb(n, j) for j in range(k, n + 1)) / 2 ** n
    return min(1.0, 2 * tail)

print('\nExact two-sided sign test, trait-level pairing (one number per trait):')
for n in (len(recs), len(s1), len(s2)):
    print(f'  n = {n:2d} traits: floor (all favour joint) p = {sign_p(n, n):.2e}')
    for k in range(n, max(n // 2, 0), -1):
        p = sign_p(k, n)
        if p > 0.05:
            print(f'                 smallest k reaching p<0.05: k = {k+1}/{n} '
                  f'(p = {sign_p(k+1, n):.4f}); k = {k}/{n} gives p = {p:.4f}')
            break

def srt_floor(n):
    """Minimum attainable two-sided exact Wilcoxon signed-rank p at n pairs
    (all differences same sign, no ties): 2 / 2^n."""
    return 2 / 2 ** n

print('\nExact Wilcoxon signed-rank minimum attainable two-sided p:')
for n in (len(recs), len(s1), len(s2), 25, 40):
    print(f'  n = {n:2d} pairs: {srt_floor(n):.2e}')

print('\nExact McNemar floor (discordant pairs d, all favouring one arm):')
for d in range(3, 13):
    print(f'  d = {d:2d}: p = {2 / 2 ** d:.4f}'
          f'{"   <- first d that can reach p<0.05" if d == 6 else ""}')
