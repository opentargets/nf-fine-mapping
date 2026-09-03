"""Verify the LD matrix that belongs to IVW-meta-analysed z-scores.

Claim to test
-------------
Per ancestry a, summary-statistic model:  z_a ~ N(R_a lambda_a, R_a)

Per-variant IVW weights, normalised so sum_a u_{a,i}^2 = 1:

    u_{a,i} = (1/se_{a,i}) / sqrt( sum_b 1/se_{b,i}^2 )
    z_meta,i = sum_a u_{a,i} z_{a,i}

Then, with independent samples across ancestries,

    R_meta = sum_a D_a R_a D_a,      D_a = diag(u_{a,.})

and diag(R_meta) = 1 exactly, so R_meta is a proper correlation matrix.

This matters here specifically because se_{a,i} depends on MAF, and MAF differs
across ancestries -- which is the very mechanism the pipeline exploits. So the
weights are genuinely per-variant, and the simpler sqrt(N)-weighted form
R_meta = sum_a (N_a/N) R_a is an approximation that degrades exactly in the
cases of interest. Both are checked below.
"""
import numpy as np

rng = np.random.default_rng(20260903)
p, A = 40, 3
N = np.array([400_000.0, 80_000.0, 4_000.0])          # the real EUR:AFR:EAS shape


def ar1_plus_noise(p, rho, rng):
    """A plausible LD matrix: AR(1) decay plus a random PSD perturbation."""
    idx = np.arange(p)
    R = rho ** np.abs(idx[:, None] - idx[None, :])
    B = rng.normal(size=(p, p)) / np.sqrt(p)
    R = R + 0.25 * (B @ B.T)
    d = np.sqrt(np.diag(R))
    return R / np.outer(d, d)


# genuinely different LD per ancestry, and genuinely different MAF
Rs = [ar1_plus_noise(p, rho, rng) for rho in (0.90, 0.70, 0.85)]
maf = np.column_stack([rng.uniform(0.05, 0.45, p),
                       rng.uniform(0.01, 0.45, p),
                       rng.uniform(0.01, 0.45, p)])

# se for a per-allele beta under a standardised trait:
#   se_{a,i}^2 = 1 / (N_a * 2 f (1-f))
se = 1.0 / np.sqrt(N[None, :] * 2 * maf * (1 - maf))          # p x A

w = 1.0 / se ** 2                                              # IVW weights
u = (1.0 / se) / np.sqrt(w.sum(axis=1))[:, None]               # p x A
print("check sum_a u_{a,i}^2 == 1 :", np.allclose((u ** 2).sum(axis=1), 1.0))

# --- predicted R_meta, exact form -------------------------------------------
R_pred = np.zeros((p, p))
for a in range(A):
    D = np.diag(u[:, a])
    R_pred += D @ Rs[a] @ D
print("diag(R_meta) all 1      :", np.allclose(np.diag(R_pred), 1.0))

# --- predicted R_meta, sqrt(N) approximation --------------------------------
R_apx = sum((N[a] / N.sum()) * Rs[a] for a in range(A))

# --- Monte Carlo: draw z_a, form z_meta, compare empirical covariance -------
chol = [np.linalg.cholesky(R + 1e-9 * np.eye(p)) for R in Rs]
M = 400_000
Zm = np.zeros((M, p))
for a in range(A):
    Za = rng.standard_normal((M, p)) @ chol[a].T        # z_a ~ N(0, R_a)
    Zm += Za * u[:, a]                                   # weighted sum
R_emp = np.cov(Zm, rowvar=False)

mc_se = 1.0 / np.sqrt(M)          # ~ SE of a correlation entry at M draws
print(f"\nMonte Carlo M = {M:,}   (per-entry SE approx {mc_se:.5f})")
for name, R in (("exact  sum_a D_a R_a D_a", R_pred),
                ("approx sum_a (N_a/N) R_a", R_apx)):
    off = ~np.eye(p, dtype=bool)
    err = np.abs(R - R_emp)
    print(f"  {name}:  max|err| = {err.max():.5f}   "
          f"mean|err off-diag| = {err[off].mean():.5f}   "
          f"= {err[off].mean()/mc_se:5.1f} x MC SE")

# --- does the choice change fine-mapping-relevant quantities? ---------------
# chi-square of a signal under each assumed LD, via z' R^-1 z on a small block
blk = slice(0, 8)
z_one = Zm[0, blk]
for name, R in (("empirical", R_emp[blk, blk]),
                ("exact    ", R_pred[blk, blk]),
                ("approx   ", R_apx[blk, blk])):
    q = z_one @ np.linalg.solve(R + 1e-6 * np.eye(8), z_one)
    print(f"  z' R^-1 z under {name} LD: {q:8.3f}")
