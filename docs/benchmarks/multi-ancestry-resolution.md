# Multi-ancestry resolution benchmark — statistical analysis plan

Status: **draft plan, not yet run.** Written before results exist, so that the
primary endpoint is fixed in advance rather than chosen after looking.

Purpose: establish, defensibly, whether joint multi-ancestry fine-mapping in
this pipeline improves credible-set resolution relative to single-ancestry
fine-mapping of the same data — and quantify by how much, under what
conditions, and at what cost in reliability.

---

## 1. Estimands — define these before any test

"Does multi-ancestry fine-mapping improve resolution?" is not an estimand and
cannot be tested. Three distinct quantities are in play, and they trade off
against each other:

**E1 — Resolution.** The ratio of expected 95% credible-set size under joint
analysis to that under the single-ancestry comparator, among credible sets that
contain a true causal variant, at a fixed false-discovery rate.

**E2 — Discovery.** The difference in the expected number of true causal
signals captured per region.

**E3 — Calibration.** `P(true causal variant ∈ S | S reported)`, which should
equal the nominal 0.95.

The two conditionings in E1 are what make it a real estimand:

- **conditional on the set being a true positive** — without this, a method
  that emits tiny spurious sets wins;
- **at fixed FDR** — without this, the winner is decided by threshold choice,
  not by the method.

E1, E2 and E3 form a three-way frontier. Any one of them can be improved by
sacrificing the others, so a benchmark reporting a single number is
uninterpretable: it must report all three, or report one at pinned values of
the other two. This is the reason §8 exists.

---

## 2. Why MultiSuSiE-vs-26.06 is not a benchmark

The available comparison — this pipeline's MultiSuSiE output against SuSiE-inf
credible sets from Open Targets Platform release 26.06 — differs in at least
seven ways simultaneously:

| dimension | pipeline arm | 26.06 arm |
|---|---|---|
| ancestries | many, jointly | one |
| effective sample size | pooled across arms | single study |
| model class | SuSiE (multi-ancestry) | SuSiE **+ infinitesimal random effects** |
| LD reference | Pan-UKBB block matrices via Hailing Ducks | Gentropy's reference — **pin this down** |
| locus definition | Locus Breaker + Canonical Region Collection | Gentropy's windowing |
| analysis unit | per trait, joint over studies | per study |
| variant set | union across ancestries | per-study variants after per-study QC |

No p-value computed from that contrast is interpretable as evidence about
ancestry, because ancestry is one of seven things that changed. The
infinitesimal term is a confounder pointing the wrong way for us: SuSiE-inf is
*designed* to be conservative under polygenic background and LD mismatch, so
any credible-set shrinkage partly reflects our having dropped that term rather
than our having added ancestries.

> **To verify before writing anything down:** whether MultiSuSiE includes an
> infinitesimal / polygenic component. The plan below assumes it does not
> (Rossen J, et al. *Nat Genet* 2026;58:67–76). Confirm from the paper.

**Consequence for framing.** Treat 26.06 as a *resource baseline* — "this is
what the Platform provides today; this is what the pipeline adds" — and keep it
out of the method comparison entirely. That is both the statistically honest
framing and the one that avoids reading as a scoreboard against Gentropy.

---

## 3. Comparator ladder — one change per rung

Each rung differs from the next by exactly one factor.

| rung | analysis | what it isolates |
|---|---|---|
| **A** | MultiSuSiE, single ancestry (largest arm), same pipeline, same LD source, same regions | implementation baseline; no ancestry contribution |
| **B** | fixed-effect IVW meta-analysis across arms → single-effect-set fine-mapping with the **meta-analysis LD** (§3.1) | same total N as joint, one LD structure |
| **B′** | joint analysis with LD contrast removed (§3.2) | LD contrast specifically — simulation only |
| **C** | MultiSuSiE, all ancestry arms | the treatment |
| **D** | SuSiE-inf per ancestry, run in-pipeline | the infinitesimal term |
| **E** | Platform 26.06 SuSiE-inf | external resource reference, reported separately |

**Primary contrast: C vs B.** It is the only rung pair holding total sample
size fixed while varying LD structure, i.e. the only one that tests the
mechanistic claim. C vs A conflates added N with added diversity, which is the
error most multi-ancestry comparisons make and which reviewers now look for
specifically. Both the SuSiEx and MultiSuSiE papers use a meta-analysis
comparator; expect to be asked why you did not.

### 3.1 The meta-analysis arm needs the *right* LD, or the comparison is rigged

Under the standard summary-statistic model with standardised genotypes,
per-ancestry z-scores satisfy

```
z_a ~ N(R_a λ_a , R_a),        λ_a = sqrt(N_a) · β_a^std
```

Combining z-scores with fixed-effect weights `w_a = sqrt(N_a) / sqrt(Σ N_a)`
gives `z_meta = Σ_a w_a z_a`, and therefore

```
Var(z_meta) = Σ_a w_a² R_a = Σ_a (N_a / N_total) · R_a
```

So the LD matrix belonging to the meta-analysed statistics is the
**sample-size-weighted average of the per-ancestry LD matrices**, not the LD of
any single ancestry.

Giving the meta arm only the largest ancestry's LD misspecifies its likelihood
and will make it fail for reasons unrelated to the science. If the comparator
is handicapped, the primary result is worthless. Use `Σ (N_a/N) R_a`.

### 3.2 Rung B′ — and an honest caveat about it

The sharpest possible control is: run the joint model with everything held
fixed except LD contrast — same code, same prior, same N, same effects — by
supplying the *same* LD matrix for every ancestry arm.

**This only works in simulation.** In real data, substituting one ancestry's LD
for all arms makes the analysis misspecified (the z-scores were generated under
different LD), so a poor result would conflate "diversity removed" with "LD
now wrong". In simulation you can do it properly: generate the z-scores under
identical LD across arms, keeping N and effect sizes unchanged. Then the joint
analysis has full N and zero diversity, and the contrast is clean.

If the gain survives B but vanishes in B′, the gain is LD contrast — the claim
in the abstract. If it survives B′ as well, the gain is coming from the prior
or from effect-size sharing, and the story needs rewriting. This is the single
most informative run in the plan.

---

## 4. Unit of analysis, pairing, and set matching

### 4.1 Why pair

Between-region variance in credible-set size vastly exceeds the within-region
between-method variance: sizes span 1 to several hundred depending on LD-block
length, signal strength and number of causals. Writing
`|S| = μ · exp(u_r) · exp(β · 1[joint])` with `u_r ~ N(0, σ²)`, the unpaired
estimator of β carries an extra `2σ²/R` in its variance that the paired
estimator does not. With σ² as large as it is here, pairing is worth roughly a
5–20× gain in effective sample size.

This requires that both arms be run on **the same regions**, not on two
overlapping catalogues that are then intersected after the fact.

### 4.2 The nesting: region within trait

Regions are nested inside traits (`runId`). Regions from one trait share the
same studies, the same ancestry composition and the same LD panels, so they are
not independent. Two consequences at this manifest's size (19 traits):

- A trait-level random effect has 19 levels, so its variance component is
  poorly estimated and the resulting standard errors are optimistic.
- Cluster-robust standard errors with 19 clusters are unreliable under the
  usual CR0/CR1 estimators (the rule of thumb is ≈40 clusters). Use the
  bias-reduced **CR2** estimator with **Satterthwaite** degrees of freedom
  (Bell–McCaffrey), and report the effective df — it will be well below 18.

Recommended structure: **region-level primary analysis with a trait random
effect**, and a **trait-level aggregate analysis as the assumption-free
sensitivity** (one number per trait, exact signed-rank; see §12 for what that
can and cannot achieve).

### 4.3 Matching credible sets between arms — pre-specify the rule

The arms report different numbers of sets per region (`k_joint ≠ k_meta`), so
sets cannot be paired without a rule. Fix it now.

**Rule M1 (primary).** Sets `S_i` (joint) and `S_j` (comparator) are candidate
matches iff the max-PIP variant of either lies in the other. Candidate matches
are resolved to a one-to-one assignment by maximum-weight bipartite matching
(Hungarian algorithm) with weight = Jaccard overlap
`|S_i ∩ S_j| / |S_i ∪ S_j|`. Deterministic; ties do not occur in practice.

**Rule M2 (sensitivity).** Restrict to regions with `k_joint = k_meta = 1`.
Unambiguous, but selects for easy regions. State the expected direction of the
resulting bias in advance: if the gain concentrates in multi-signal regions,
M2 biases toward the null; if the joint model splits single signals into
multiple sets, M2 biases away from it.

**Unmatched sets are data, not nuisance.** They go to the discovery analysis
(§6) and must be counted in both directions.

---

## 5. Statistic 1 — resolution

### 5.1 Primary model: negative-binomial GLMM on excess set size

```
(|S_rmj| − 1)  ~  NegBin(μ_rm , φ)
log μ_rm       =  β₀ + β₁·1[m = joint] + u_r + v_t(r)
u_r ~ N(0, σ²_region)      v_t ~ N(0, σ²_trait)
```

**Model `|S| − 1`, not `|S|`.** Credible-set size is ≥ 1, so a count model on
`|S|` places mass on an impossible zero and fits worst exactly where it matters
— at `|S| = 1`, the perfectly resolved case. Subtracting 1 makes perfect
resolution the natural zero and makes `exp(β₁)` the multiplicative change in
*excess* variants beyond the one you were looking for. This is the
interpretable quantity and it is rarely done.

- Estimand: `exp(β₁)` = within-region ratio of expected excess set size.
- H₀: `β₁ = 0`.
- Test: Wald `z = β̂₁ / SE(β̂₁)` against N(0,1); or, preferably at this sample
  size, a likelihood-ratio test against the model without the method term,
  `LR = 2(ℓ_full − ℓ_null) ~ χ²₁`. With fewer than ~50 regions, use a
  parametric bootstrap of the LR statistic instead of the asymptotic χ².
- Report: `exp(β̂₁)` with `exp(β̂₁ ± 1.96·SE)`, the p-value, **and the absolute
  medians and IQRs of both arms**.

**Assumptions, how each fails here, and the remedy:**

1. *Gaussian region effect on the log scale.* A handful of catastrophically
   unresolved regions (|S| ≈ 500) puts a heavy right tail on `u_r`, inflating
   `σ̂²` and deflating the test statistic. Diagnostic: QQ plot of predicted
   `u_r`. Remedy: pre-specified cap `min(|S|, 100)` as a sensitivity analysis,
   or fall back to §5.2, which is immune.
2. *Shared dispersion φ across arms.* The joint arm will produce more
   1-variant sets, so its dispersion genuinely differs, and a shared-φ model
   misstates the standard errors. Remedy: fit a dispersion sub-model
   `log φ = γ₀ + γ₁·1[joint]` (`glmmTMB`'s `dispformula`) and test `γ₁ = 0`. If
   it rejects, report the estimate from the heterogeneous-dispersion fit.
3. *Regions independent given the random effects.* Fails if regions overlap or
   share an LD block. Remedy: verify that Canonical Region Collection actually
   emits disjoint intervals — check, do not assume — and report CR2
   cluster-robust SEs clustered on chromosome arm as a sensitivity.
4. *Multiple sets within a region independent.* They compete for the same
   signal, so they are not. A set-level nested effect is over-parameterised
   with 1–3 sets per region. Remedy: sensitivity analysis restricted to the
   top-PIP set per region per arm, one observation per pair.

### 5.2 Robustness check: exact Wilcoxon signed-rank

For matched pairs `d_i = |S_joint,i| − |S_meta,i|`:

- **The estimand is the pseudomedian**, i.e. the median of the Walsh averages
  `(d_i + d_j)/2`, not the difference of medians. Describing this test as a
  test of medians is a common misstatement — state the pseudomedian, or use the
  Hodges–Lehmann estimator's own definition.
- H₀: the distribution of `d` is symmetric about zero.
- Statistic: `W⁺ = Σ_{d_i > 0} rank(|d_i|)` over nonzero differences. Exact
  null distribution by enumeration for small n; asymptotically
  `N(n(n+1)/4, n(n+1)(2n+1)/24)` with the tie correction subtracting
  `Σ_t (t³ − t)/48`.
- **Ties are a first-order problem here, not a technicality.** Set sizes are
  small integers, so `d_i = 0` will be common (many regions resolve to one
  variant in both arms) and `|d_i|` will tie heavily. The classical test
  *discards* zero differences, which silently changes the estimand to "among
  regions where the arms disagree". With 40% zeros that is a materially
  different question. Handle it by: (i) reporting the number of zeros and
  interpreting conditionally; (ii) using the exact permutation distribution
  with the observed tie pattern (`coin::wilcoxsign_test`, `distribution =
  "exact"`); or (iii) reporting the transparent three-way split — joint
  smaller / equal / larger — and testing it with a sign test.
- Point estimate: Hodges–Lehmann, with the CI obtained by inverting the test.
  Report it **in variants**, not as a ratio.

### 5.3 Do not use

A paired t-test on `|S|` is driven entirely by the tail. On `log|S|` it
estimates a geometric-mean ratio, which is defensible in principle, but the
log of a small count is coarse and normality of its mean at n ≈ 50 with a heavy
tail is not reliable. **Mean credible-set size must never be the headline**: a
single 400-variant region moves it further than fifty well-resolved ones.

### 5.4 Binary summaries

Per matched pair: `A_i = 1[|S_joint,i| = 1]`, `B_i = 1[|S_meta,i| = 1]`.
Pre-specify exactly two thresholds — `= 1` and `≤ 5`. More is
multiplicity-hunting.

- Contingency: `n₁₁` (both), `n₁₀` (joint only), `n₀₁` (meta only), `n₀₀`
  (neither).
- H₀: marginal homogeneity, `E[n₁₀] = E[n₀₁]`.
- McNemar: `χ² = (n₁₀ − n₀₁)² / (n₁₀ + n₀₁) ~ χ²₁`.
- **Only discordant pairs carry information.** Power depends on `n₁₀ + n₀₁`,
  not on the number of regions: R = 500 can still be underpowered if the arms
  usually agree. Compute the discordant count before interpreting anything
  (§12).
- Use the **exact** conditional test when `n₁₀ + n₀₁ < 25`: given
  `d = n₁₀ + n₀₁`, `n₁₀ ~ Binomial(d, ½)` under H₀. Do **not** use the
  continuity-corrected χ² — it is neither exact nor the asymptotic test, and
  its conservatism is uncharacterisable.
- Estimate: `δ̂ = (n₁₀ − n₀₁)/n`, with a Newcombe–Wilson score interval for the
  paired difference (Wald intervals fail near the boundary, and one arm will be
  near it).

---

## 6. Statistic 2 — discovery

Two separate questions; do not conflate them.

**(a) Sets per region, `k_rm`.** Poisson or NB GLMM, `k ~ method + (1|trait/region)`.

- `k` is bounded above by `L`, the maximum number of single effects allowed.
  **If `L` differs between arms the comparison is meaningless.** Pin `L` and
  report its value.
- `k` counts *reported* sets, including false ones. Without §7 it cannot be
  read as discovery. Say so in the text.

**(b) Regions with at least one set, `1[k_rm ≥ 1]`.** McNemar as in §5.4.

> **Most likely fatal flaw in the current setup — check this first.** If
> regions were defined by running Locus Breaker on one arm's statistics, that
> arm has `k ≥ 1` by construction and the comparison is circular. Region
> definition must be arm-blind. Acceptable options: define regions from a
> source common to both arms; or define from each arm and analyse the union,
> reporting the asymmetry explicitly. The union-based definition favours
> neither arm and is the recommended choice.

---

## 7. Statistic 3 — calibration and FDR (simulation only)

Coverage and FDR both require knowing the causal variant. **There is no way to
estimate either from real data.** The proxies in §10 are proxies, not
measurements. Any report of "FDR" from real fine-mapping without a simulation
or an experimental gold standard is reporting something else.

### 7.1 Coverage

- Definition: `P(∃ causal c : c ∈ S | S reported)`, nominal 0.95.
- Estimator: sets containing ≥1 true causal, over sets reported.
- **The unit matters.** Sets from the same simulated region are dependent;
  pooling all sets and applying a binomial SE understates uncertainty. Choose
  one and pre-specify: one set per replicate (top-PIP); a mixed logistic with a
  replicate random effect; or a cluster bootstrap over replicates.
- Against nominal: Clopper–Pearson exact interval; miscalibration if it
  excludes 0.95. This tests a point null, it is not a method comparison.
- Between arms: McNemar on `1[covers]` for replicates where both report a set.

### 7.2 FDR

- Definition at set level: the expected false-discovery proportion,
  `FDP = V / max(R, 1)` where V is the number of reported sets containing no
  true causal. State whether you report `E[FDP]` or the ratio of expectations —
  they differ at small R and papers routinely elide this.
- **This is not a Benjamini–Hochberg quantity.** No multiple-testing procedure
  is controlling it. It is an empirical property of the method at its chosen
  coverage setting. Never describe it as "FDR-controlled".
- Estimator: pooled proportion with a cluster bootstrap CI over replicates.
- Note the identity: with "false set" defined as "contains no causal",
  set-level FDP is exactly `1 − coverage`. Reporting both is legitimate, but
  they are one number seen from two sides and must not be presented as two
  independent lines of evidence.

### 7.3 Power

Variant-level: `P(causal c ∈ some reported set)`. The unit is the causal
variant; causals within a replicate are dependent, so cluster on replicate.
**Always report power and FDR as a pair** — either alone is meaningless.

### 7.4 PIP calibration

- Bin variants by PIP; plot mean PIP against observed fraction causal with
  Clopper–Pearson intervals. Perfect calibration is the identity line.
- Summary: logistic regression `logit P(causal) = α + β·logit(PIP)`. Perfect
  calibration is `α = 0, β = 1`; `β < 1` means overconfidence at the extremes.
  Report `(α̂, β̂)` with CIs.
- **Do not use Hosmer–Lemeshow.** Its p-value depends on an arbitrary bin count
  and its power properties are poor. Show the curve; give `(α, β)`.
- Report bin counts. The variant-level sample is dominated by near-zero-PIP
  variants, so the 0.9–1.0 bin — the only one anyone cares about — will be
  sparse.

---

## 8. The headline: resolution at matched FDR

This is the analysis that makes the benchmark hard to attack.

For each arm, sweep a tuning parameter τ that trades resolution against
reliability — the credible-set coverage target (0.80, 0.90, 0.95, 0.99), the
purity threshold (minimum absolute within-set correlation), or a grid over
both.

For each `(arm, τ)`, compute:

- `FDR(arm, τ)` — empirical, from §7.2;
- `size(arm, τ)` — median set size **among true-positive sets**.

Plot `size` against `FDR`, one curve per arm: an operating-characteristic curve
directly analogous to an ROC. Read off **size at FDR = 0.05**, linearly
interpolated between the bracketing grid points, with a cluster bootstrap CI
over replicates for both coordinates.

Why this rather than "median 1 vs 15 at nominal 95%": if the joint arm's
empirical FDR at nominal 0.95 is 12% and the comparator's is 4%, then part of
that difference is a threshold, and a referee will say exactly that. Matching
FDR removes the objection completely, and it upgrades the claim from "our
numbers are smaller" to "**at equal reliability, our sets are smaller**" —
which is the claim you actually want and is far harder to dislodge.

---

## 9. Simulation design

### 9.1 Simulate summary statistics, not genotypes

Use the real Pan-UKBB LD matrices the pipeline already retrieves, so LD is
realistic, and sample z-scores directly from the model in §3.1:

```
z_a = R_a λ_a + R_a^(1/2) ε_a ,   ε_a ~ N(0, I)
λ_a[j] = sqrt(N_a) · β_a^std[j]   (zero except at causal variants)
```

This is exact under the standard summary-statistic model, orders of magnitude
faster than genotype simulation, and it makes the LD-mismatch experiment
trivial: generate `z` using the true in-sample `R`, then fine-map using the
reference-panel `R̂`.

Practical note: reference-panel `R` is frequently not positive semi-definite.
Take `R^(1/2)` by eigendecomposition with eigenvalues clipped at zero, and
record how much spectral mass was clipped — that quantity is itself a measure
of panel quality and worth reporting.

### 9.2 Grid — and why each axis is there

The mechanism claimed is: causal-variant tagging differs across ancestries, so
the intersection of two ancestries' evidence excludes variants that neither
alone can exclude. That mechanism has specific dependencies, and the grid must
span them.

1. **Cross-ancestry effect-size correlation** ρ ∈ {1.0, 0.8, 0.5, 0.0}. At
   ρ = 1 MultiSuSiE's shared-effect prior is correctly specified and should win
   maximally. At ρ = 0 the shared-effect prior is misspecified and joint
   analysis can *hurt*. **You must locate the ρ at which the gain disappears.**
   Not showing it is the gap a reviewer will find; finding it yourself is a
   strength.
2. **Causal-variant MAF differentiation**, e.g. MAF pairs (0.20, 0.20),
   (0.20, 0.05), (0.20, 0.01). Low MAF in the second ancestry means little
   independent information — the realistic case for AFR–EUR pairs at
   EUR-discovered loci.
3. **Per-arm sample-size ratio** ∈ {1:1, 3:1, 10:1, 100:1, 300:1}. The upper
   end is not hypothetical: it is what this manifest contains (§11). The
   extreme cells are the ones that test whether *diversity* rather than *N* is
   doing the work, so they are the scientifically load-bearing ones.
4. **Causal variants per region** ∈ {1, 2, 3}, including one scenario with two
   causals in moderate LD (r² ≈ 0.4). Single-causal simulations flatter every
   fine-mapping method; resolution claims degrade sharply beyond one causal,
   and that is where methods separate.
5. **LD reference**: in-sample vs out-of-sample Pan-UKBB, at panel sizes
   n ∈ {500, 5000, full}. Out-of-sample LD mismatch is the production condition
   and the dominant cause of fine-mapping failure. It interacts with axis 3: a
   small AFR panel has noisy LD, which can *inject* false signals. This is
   precisely where SuSiE-inf's infinitesimal term earns its keep and where
   MultiSuSiE without it may be more fragile. **If there is a weakness in this
   pipeline, it is here.** Test it deliberately rather than waiting to be
   asked.
6. **Replicates.** For a coverage estimate near 0.95 the binomial SE is
   `sqrt(0.95·0.05/n)`: n = 200 → 1.5 pp, n = 1000 → 0.7 pp. Distinguishing
   0.95 from 0.92 at 80% power and α = 0.05 needs roughly n ≥ 400.

The full grid is 4 × 3 × 5 × 3 × 3 = 540 cells. Do not run 1000 replicates
everywhere. Designate **one primary cell** — ρ = 0.8, MAF (0.20, 0.05), ratio
10:1, one causal, out-of-sample panel — at n = 1000, and n = 200 elsewhere.
Pre-specify which cell is primary before running.

---

## 10. Real-data accuracy proxies

None of these measures accuracy. Each is consistent with accuracy. Write them
up that way.

### 10.1 Coding gold standard

Regions where a protein-altering variant (missense or pLoF, VEP with a fixed
transcript set) is in at least one arm's credible set, and where the gene is
independently implicated in the trait.

> **Circularity warning.** Open Targets locus-to-gene scores are trained in
> part on fine-mapping output. Using them as ground truth for a fine-mapping
> benchmark is circular. Restrict to sources independent of GWAS fine-mapping:
> OMIM, ClinVar pathogenic variants in Mendelian forms of the trait, or
> experimentally validated variants (MPRA, CRISPR).

Statistics: `1[gold variant ∈ S]` per arm → exact McNemar. PIP of the gold
variant per arm → signed-rank with a Hodges–Lehmann estimate.

**Compute the power floor first.** With d discordant pairs, the exact McNemar
p-value cannot fall below `2/2^d` however lopsided the result:

| discordant pairs d | 3 | 4 | 5 | **6** | 7 | 8 | 10 | 12 |
|---|---|---|---|---|---|---|---|---|
| minimum attainable two-sided p | 0.250 | 0.125 | 0.063 | **0.031** | 0.016 | 0.008 | 0.002 | 0.0005 |

So **fewer than six discordant pairs cannot reach p < 0.05 under any outcome**.
If that is where you land, report the descriptive result and state that the
test is underpowered. Do not run it and then read p = 0.22 as "no difference".

### 10.2 Functional enrichment

Model: `annotated ~ method + s(PIP) + MAF_bin + region`, or conditional
logistic within matched variant strata.

**The confounding here is severe and specific.** Functional annotations
correlate with MAF, variant class, gene density and LD-block length. If joint
analysis systematically retains higher-MAF variants — and it will, because
low-MAF variants in the smaller ancestry are less informative — then a naive
enrichment is *entirely* a MAF shift. MAF-match the comparison or include MAF
as a spline, and print the MAF distributions of both arms' retained sets side
by side. Without that, this analysis is worthless.

### 10.3 The subset test — the cleanest real-data test available

Restrict to matched pairs where `S_joint ⊊ S_meta`: the joint analysis has
excluded specific variants, and we can ask whether the exclusion was
informative.

Under the null of uninformative exclusion, `S_joint` is a uniformly random
subset of `S_meta` of size `|S_joint|`, so any given variant is retained with
probability `p_r = |S_joint,r| / |S_meta,r|`.

Statistic: the number of regions in which the top-annotated variant of `S_meta`
is retained. Under H₀ this is a sum of independent Bernoulli variables with
region-specific `p_r` — a **Poisson-binomial**. Compute the exact p-value from
its distribution, or by 10⁶ Monte Carlo draws.

This is strong because the null is exact and derived internally: no external
comparator, no MAF matching, no distributional assumption. Make it a
pre-specified secondary endpoint.

### 10.4 Held-out replication

If an ancestry has an independent cohort excluded from the fine-mapping input:
does the joint arm's top-PIP variant show a larger `|z|` there than the
comparator's? Paired signed-rank on the held-out `|z|` difference.

Caveats to state: both leads are selected on the same discovery data, so
absolute values are inflated by winner's curse — the *comparison* remains valid
because both are equally selected. And this tests "picked a better-replicating
variant", which proxies causality only when the causal variant has the
strongest true marginal association — true under one causal, false under
multiple causals in LD.

### 10.5 Negative control

Regions with a genome-wide significant signal in only one ancestry have no
second source of LD contrast at the causal variant, so the resolution gain
should be ≈ 0.

Fit the §5.1 model restricted to these regions. Here you *want* to fail to
reject — which means the CI is the result, not the p-value. "We did not reject"
with a CI of [0.30, 1.80] says nothing. **Pre-specify what counts as
absence of effect**, e.g. the CI excludes a 25% reduction.

This is the control that separates a real mechanism from a pipeline artefact
such as different variant filtering shrinking sets for reasons unrelated to
ancestry.

### 10.6 Failure accounting

Report non-convergence rate, regions where the joint set drops the
single-ancestry lead variant, and sets failing the purity filter. A benchmark
that reports only wins is not a benchmark, and this section is what earns
trust.

---

## 11. Effect modification by ancestry balance — the decisive feature of this manifest

The gain from joint analysis cannot exceed what the second ancestry
contributes. An arm with negligible sample size contributes negligible
information while still contributing LD-panel noise. So the effect is
*expected* to be modified by how balanced the ancestry arms are, and that
modifier should be modelled rather than discovered.

**Balance measure.** Use the inverse-Simpson / participation ratio over
effective sample sizes:

```
A_eff = (Σ_a N_a)² / Σ_a N_a²
```

`A_eff` equals the number of arms when they are equal-sized and tends to 1 when
one arm dominates. It is the "how many ancestries do you effectively have"
statistic, and unlike the raw arm count it does not treat an N = 1,419 arm as a
whole ancestry.

### 11.1 What `manifest.test3.tsv` actually contains

19 runs (10 with two ancestry arms, 9 with three); 19 NFE, 17 AFR, 11 EAS arms;
effective sample size from 1,419 to 437,621.

| A_eff | arms | maxN/minN | composition (ancestry:effective N) |
|---:|---:|---:|---|
| 1.03 | 2 | 71.1 | nfe:407,746 eas:5,737 |
| 1.03 | 2 | 70.7 | nfe:419,463 eas:5,934 |
| 1.04 | 3 | 308.4 | nfe:437,621 afr:6,298 eas:1,419 |
| 1.06 | 3 | 63.5 | nfe:412,791 afr:6,670 eas:6,505 |
| 1.16 | 2 | 12.5 | nfe:134,567 afr:10,772 |
| 1.34 | 2 | 5.8 | nfe:22,955 afr:3,978 |
| 1.34 | 3 | 190.0 | nfe:360,249 afr:60,105 eas:1,896 |
| 1.35 | 3 | 94.6 | nfe:437,137 afr:73,911 eas:4,620 |
| 1.45 | 2 | 4.2 | nfe:167,710 afr:39,469 |
| 1.46 | 2 | 4.1 | nfe:98,404 afr:24,264 |
| 1.50 | 2 | 3.8 | nfe:218,230 afr:57,987 |
| 1.50 | 2 | 3.7 | nfe:238,498 afr:63,835 |
| 1.50 | 3 | 106.9 | nfe:204,018 afr:52,355 eas:1,909 |
| 1.51 | 2 | 3.7 | nfe:75,196 afr:20,538 |
| 1.56 | 2 | 3.3 | nfe:149,855 afr:45,778 |
| 1.57 | 3 | 73.2 | nfe:329,504 afr:96,994 eas:4,503 |
| 1.58 | 3 | 69.9 | nfe:266,373 afr:79,575 eas:3,813 |
| 1.69 | 3 | 93.8 | nfe:207,771 afr:78,389 eas:2,216 |
| 1.69 | 3 | 62.2 | nfe:210,734 afr:78,442 eas:3,389 |

Two facts follow, and both are consequential.

**(i) `A_eff` never exceeds 1.69.** Even the most balanced run in this manifest
has less than 1.7 effective ancestries; most three-arm runs are effectively
two-arm because the EAS arm carries 1,419–6,505 samples against an NFE arm of
200,000–437,000. **This manifest cannot test the balanced-ancestry regime,
because it does not contain it.** Whatever the benchmark shows, it is a
statement about a strongly EUR-dominated design. Say that explicitly, and if a
stronger claim is wanted, select a manifest that includes runs with comparable
arm sizes.

**(ii) The manifest is effectively two experiments.** Splitting at
`A_eff ≥ 1.40` gives 11 runs (1.45–1.69, mostly AFR-supported) and 8 runs
(1.03–1.35, containing a token arm). Pooling them dilutes the effect toward
whatever the token-arm runs do.

### 11.2 How to handle it — continuous, not a subgroup

Do not present this as a subgroup analysis; the threshold 1.40 is arbitrary and
a reviewer will treat it as post-hoc. Model the modifier continuously as a
pre-specified interaction:

```
(|S| − 1) ~ NegBin,  log μ = β₀ + β₁·1[joint] + β₂·log A_eff
                             + β₃·1[joint]·log A_eff + u_r + v_t
```

`β₃` is the mechanistic prediction: the resolution gain grows with ancestry
balance. This turns "it works when the ancestries are balanced" from a
subgroup claim into a directional hypothesis test.

**Honest limitation.** `A_eff` varies only between traits, so the interaction
has 19 effective observations and will be badly underpowered. At this manifest
size, `β₃` is exploratory and hypothesis-generating; report the estimate and CI
without an inferential claim, present the stratified medians descriptively, and
**estimate the `A_eff` dependence properly in the simulation arm** (§9.2 axis
3), where it can be powered by construction.

---

## 12. Power floors — compute these before running, not after

Exact two-sided sign test at trait-level pairing (one summary number per
trait), with k of n traits favouring joint analysis:

| n traits | floor (all favour joint) | smallest k reaching p < 0.05 | next k down |
|---:|---|---|---|
| 19 (all runs) | p = 3.8 × 10⁻⁶ | k = 15 → p = 0.0192 | k = 14 → p = 0.0636 |
| 11 (A_eff ≥ 1.40) | p = 9.8 × 10⁻⁴ | k = 10 → p = 0.0117 | k = 9 → p = 0.0654 |
| 8 (A_eff < 1.40) | p = 7.8 × 10⁻³ | k = 8 → p = 0.0078 | k = 7 → p = 0.0703 |

Read these carefully:

- **Trait-level analysis is viable overall** — 15 of 19 traits favouring joint
  analysis gives p = 0.019 from an exact, assumption-free test. That is a
  respectable primary result and it requires no modelling choices at all.
- **The balanced stratum alone needs 10 of 11.** Anything less cannot reach
  p < 0.05 at trait level, so a stratum-specific claim requires the
  region-level analysis with the trait clustering handled as in §4.2 — the
  trait-level test cannot deliver it.
- Minimum attainable exact signed-rank p is `2/2^n`: 3.8 × 10⁻⁶ at n = 19,
  7.8 × 10⁻³ at n = 8.

For the region-level NB GLMM there is no closed-form power calculation worth
trusting. Simulate it: take the 26.06 single-ancestry set-size distribution as
the baseline, assume a rate ratio of 0.6 (a 40% reduction in excess variants)
and a region random-effect SD of ≈0.8 on the log scale, generate R regions
across 19 traits, and count rejections over 1000 draws. That gives power as a
direct function of R in an afternoon, and it tells you whether the region count
this manifest yields is sufficient before you spend compute on the run.

---

## 13. Multiplicity

The full plan generates on the order of thousands of p-values (3 contrasts × 3
metric families × up to 540 simulation cells). Options, most to least rigorous:

1. **One pre-specified primary endpoint**, one pre-specified primary simulation
   cell; everything else reported as secondary or exploratory without
   inferential claims. This is what a clinical trial does and it is the only
   fully defensible approach.
2. **Fixed-sequence (gatekeeping) testing**: test the primary, then the
   secondaries in a pre-specified order, each at α = 0.05, stopping at the
   first non-rejection. Controls FWER exactly with no α penalty, at the cost of
   committing to the order in advance.
3. **Benjamini–Hochberg within an explicitly defined family** — e.g. "the 12
   cells of the ρ × N-ratio grid for the coverage comparison". Sort
   `p₁ ≤ … ≤ p_m`, reject `1..k` for the largest k with `p_k ≤ kα/m`, and
   report `q_i = min_{j ≥ i} (m p_j / j)`. BH requires independence or positive
   regression dependence; across cells sharing simulated regions the dependence
   is positive, so BH is valid. Benjamini–Yekutieli holds under arbitrary
   dependence at the cost of a `log m` factor — usually too conservative to be
   worth it here.
4. **Not acceptable**: Bonferroni across thousands of dependent tests, or
   reporting the whole grid unadjusted and highlighting the significant cells.

**Recommended primary endpoint.** Co-primary, both fixed now:

- *Simulation:* median credible-set size at matched empirical FDR = 0.05,
  contrast C vs B, in the primary cell of §9.2.
- *Real data:* `exp(β₁)` from the §5.1 model, contrast C vs B, pooled over all
  19 traits, with the trait-level exact sign test of §12 as the assumption-free
  companion.

---

## 14. Invariants — any difference here invalidates the contrast

Each of these has broken a published multi-ancestry comparison.

1. **Region boundaries** — identical intervals, arm-blind definition (§6).
2. **Variant set within region** — identical after QC. If the joint arm uses
   the union across ancestries and the comparator uses one ancestry's variants,
   the candidate sets differ in cardinality *before* fine-mapping and set sizes
   are not comparable. Restrict both arms to the same variant set, and state
   whether it is the intersection or the union with explicit missing-data
   handling.
3. **MAF and INFO filters** — identical thresholds; state whether applied
   per-ancestry or pooled.
4. **`L`, maximum number of single effects** — identical, and reported.
5. **Coverage target** — identical (0.95).
6. **Purity / `min_abs_corr` filter** — identical. This one is a frequent
   hidden difference between implementations: the purity filter *drops* sets,
   changing both `k` and the size distribution.
7. **Convergence tolerance and iteration cap** — identical; record
   non-convergence rather than silently dropping the region.
8. **LD reference family** — the comparator must use the same panel family. If
   the joint arm uses Pan-UKBB per-ancestry panels and the comparator uses
   gnomAD, the benchmark is comparing panels, not methods.
9. **Sample size in the summary-statistic model** — per-variant N vs
   study-level N changes the z-score scaling.
10. **Effect-size prior** — this one *cannot* be matched, because it is part of
    the model difference. It must therefore be reported as a limitation: rung B
    removes LD diversity *and* changes the prior. The only clean isolation of
    LD contrast is rung B′, in simulation (§3.2).

---

## 15. Reporting template

For every contrast, report all of:

- number of regions; number of matched set pairs; number of discordant pairs
- median (IQR) credible-set size, **both arms, absolute**
- effect estimate with 95% CI, the test used, the p-value
- pre-specification status: primary / secondary / exploratory
- failure counts (§10.6)

Absolute numbers before ratios, always. A ratio without the denominators is not
a result.

---

## 16. Poster and abstract framing

- Report **both arms' absolute medians and IQRs**. A ratio alone reads as a
  scoreboard and invites the objection in §8.
- Frame the claim as *"adding a second ancestry to the same trait improves
  resolution"* — a statement about data diversity, not about anyone's software.
- Keep 26.06 as a **labelled resource baseline**, never as a comparator arm
  (§2).
- State the `A_eff ≤ 1.69` limitation (§11.1). It costs one sentence and it
  pre-empts the strongest available criticism.
- Do not write "FDR-controlled" (§7.2), and do not equate summed PIP with
  credible-set coverage.

---

## 17. Implementation — where the comparator arm belongs in the pipeline

The obvious construction is a parallel pipeline: IVW meta-analyse the
genome-wide summary statistics, run Locus Breaker on the result, annotate LD,
fine-map with SuSiE. **Do not build the primary comparator that way.** It
re-derives regions from the comparator's own statistics (§6 circularity), gives
the two arms different variant sets, requires a fresh decision about which LD
panel the meta arm gets (§3.1 trap), and swaps the fine-mapping software, which
silently un-matches `L`, the coverage target, the purity filter and the
convergence criteria (§14 items 4–7).

### 17.1 Collapse to the meta arm *after* LD annotation

Every invariant is satisfied for free if the meta arm is constructed **inside
the already-annotated locus set**, downstream of `Hailing Ducks LD Annotation`
and upstream of the method:

```
Locus Breaker
    ↓
Canonical Region Collection          ← regions defined ONCE, from per-study
    ↓                                  marginal statistics; the meta arm never
Hailing Ducks LD Annotation            influences them
    ↓
AnnotatedLocusSet  (per-ancestry z, per-ancestry R, per-ancestry N)
    ↓
    ├── arm = joint    : pass through unchanged        → MultiSuSiE, K arms
    ├── arm = meta      : MetaCollapse (§17.2)          → MultiSuSiE, K = 1
    └── arm = single    : keep largest arm only         → MultiSuSiE, K = 1
```

By construction all three arms share: identical region intervals, identical
variant sets, identical LD source and retrieval, identical sample-size
bookkeeping, and — because all three run the *same process* — identical `L`,
coverage target, purity filter, convergence tolerance and output contract. The
only difference between `joint` and `meta` is `K` ancestry-specific `(z, R)`
pairs versus one collapsed pair. That is the contrast in §3, isolated.

It is also cheap: no additional LD retrieval (the matrices are already in the
annotated locus set), no additional Locus Breaker pass, no second pipeline.

### 17.2 `MetaCollapse` — the exact transformation

Per canonical region, over the union variant set:

**Weights.** For variant *i* observed in ancestries `O(i)`:

```
u_{a,i} = (1/se_{a,i}) / sqrt( Σ_{b ∈ O(i)} 1/se_{b,i}² )     for a ∈ O(i)
u_{a,i} = 0                                                    otherwise
```

so `Σ_a u_{a,i}² = 1` for every variant, and variants present in only one arm
pass through with that arm's statistic. The union variant set is therefore
handled natively — no restriction to the intersection is needed.

**Statistics.** `z_meta,i = Σ_a u_{a,i} · z_{a,i}`, with
`n_meta = Σ_a N_a`.

**LD.** With independent samples across ancestries,

```
R_meta = Σ_a D_a R_a D_a ,        D_a = diag(u_{a,·})
```

and `diag(R_meta) = Σ_a u_{a,i}² = 1` exactly, so `R_meta` is a proper
correlation matrix requiring no rescaling.

**Verified numerically** (`analysis` scratch script, 3 ancestries with
N = 400,000 / 80,000 / 4,000, distinct AR(1)-plus-noise LD per ancestry,
distinct per-ancestry MAF, 400,000 Monte Carlo draws):

| assumed LD for the meta statistics | mean abs. error vs empirical, off-diagonal | max abs. error |
|---|---|---|
| `Σ_a D_a R_a D_a` (exact) | 0.00124 = **0.8 × Monte Carlo SE** | 0.0062 |
| `Σ_a (N_a/N) R_a` (√N approximation) | 0.01351 = **8.5 × Monte Carlo SE** | 0.1012 |

The exact form is correct to within Monte Carlo noise. The simpler
sample-size-weighted form — which is what the literature generally uses — is
off by up to 0.10 in correlation, because `se_{a,i}` depends on MAF and MAF
differs across ancestries. **That is the very mechanism this pipeline
exploits, so the approximation degrades precisely in the cases of interest.**
Use the per-variant form.

Practical note: `R_meta` inherits any non-PSD-ness of the per-ancestry
reference-panel matrices. Clip eigenvalues at zero and record the clipped
spectral mass (§9.1).

### 17.3 Use MultiSuSiE with K = 1, not susieR

Running the comparator through the same `MultiSuSiE` process with a single
collapsed arm removes four invariants from the "must be matched by hand" list,
because they become the same code.

> **Verify first:** that MultiSuSiE with one ancestry reduces to standard
> SuSiE — its cross-ancestry effect-size prior must degenerate correctly at
> K = 1. If it does not, the residual difference has to be reported as part of
> the C-vs-B limitation in §14 item 10.

### 17.4 The genome-wide meta pass is still needed — but only for discovery

The region-level collapse above cannot find regions that *only* the
meta-analysis would reach significance in, because Locus Breaker never sees the
meta statistics. For the discovery endpoint (§6) that is a real gap, and it
biases discovery against the meta arm.

So run the genome-wide construction as a **second, secondary path**: IVW meta
over full summary statistics → Locus Breaker → Canonical Region Collection,
then union those regions with the per-study ones. Every canonical region in the
union is fine-mapped by all three arms regardless of which source flagged it,
which is the arm-blind definition §6 asks for.

Split of duties:

| endpoint | region source | path |
|---|---|---|
| resolution (**primary**) | per-study Locus Breaker → canonical regions | §17.1 region-level collapse |
| discovery (secondary) | union of per-study **and** meta-analysis loci | §17.4 genome-wide pass |

### 17.5 Repository shape

- `modules/meta/` — new process `MetaCollapse`, consuming and emitting the
  `AnnotatedLocusSet` contract (per-locus Parquet +
  `MultiAncestryPairwiseLD` + JSONL metadata), so nothing upstream or
  downstream changes shape.
- `main.nf` — a `fan_out_arms()` channel helper after `annotate_with_ld()`,
  emitting the three configurations with `arm` added to the `meta` map.
- publish to `${params.output_dir}/finemapping/arm=${arm}/` so the three arms
  are partitions of one dataset rather than three runs to reconcile later.
- a join step keyed on `(runId, regionId)` emitting the paired table the
  analysis in §4–§6 consumes directly: one row per region per arm, with set
  sizes, set membership, PIPs, convergence flag and purity.
- `params.benchmark_arms = ['joint', 'meta', 'single']`, defaulting to
  `['joint']` so production runs are unaffected.

Estimated work: one process, one channel helper, one join. The costly parts —
region definition and LD retrieval — are reused, not duplicated.

---

## 18. Open questions to resolve before this plan is executable

1. Which LD reference the 26.06 SuSiE-inf credible sets were computed against.
2. Whether MultiSuSiE includes an infinitesimal component (§2).
3. How Locus Breaker regions are defined relative to the arms — the circularity
   check in §6.
4. Whether Canonical Region Collection emits strictly disjoint intervals.
5. How many canonical regions the 19 runs yield in total, which determines
   whether the region-level analysis is powered (§12).
6. Whether any ancestry arm has an independent held-out cohort available
   (§10.4).
