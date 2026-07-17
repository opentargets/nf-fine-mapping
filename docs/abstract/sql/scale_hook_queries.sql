-- Scale-hook queries backing the abstract's Background section.
-- Run against Open Targets release 26.06 via otai (see run_otai_queries.sh in this folder).
-- Source project: /home/mindos/Documents/Knowledge/Wiki/Projects/multi-ancestry-fine-mapping
-- (originally scoped in Research/ImplementationStatus 2026-07-07-study-sets-feasibility.md;
-- re-derived live here against 26.06 rather than the earlier snapshot).

-- 1. Confirm project IDs behind "GWAS Catalog" and "UKB-PPP" in the study index.
SELECT projectId, count(*) n
FROM study
WHERE studyType = 'gwas' OR studyType = 'pqtl'
GROUP BY projectId
ORDER BY n DESC;

-- 2. Credible sets and studies already fine-mapped for GWAS Catalog + UKB-PPP (single-ancestry today).
SELECT s.projectId, count(*) n_credible_sets, count(DISTINCT cs.studyId) n_studies
FROM credible_set cs
JOIN study s ON cs.studyId = s.studyId
WHERE s.projectId IN ('GCST', 'UKB_PPP_EUR')
GROUP BY s.projectId
ORDER BY n_credible_sets DESC;

-- 3. Breakdown of those credible sets by fine-mapping method (PICS vs SuSiE-inf road).
SELECT cs.finemappingMethod, s.projectId, count(*) n
FROM credible_set cs
JOIN study s ON cs.studyId = s.studyId
WHERE s.projectId IN ('GCST', 'UKB_PPP_EUR')
GROUP BY cs.finemappingMethod, s.projectId
ORDER BY n DESC;

-- 4. Multi-ancestry trait-set feasibility: group GWAS studies by shared trait (disease-id set),
--    count how many span >=2 ancestries, and find the largest set by study count / ancestry count.
WITH eligible AS (
  SELECT studyId, diseaseIds, ldPopulationStructure
  FROM study
  WHERE studyType = 'gwas'
    AND hasSumstats = true
    AND ldPopulationStructure IS NOT NULL
    AND len(ldPopulationStructure) >= 1
),
keyed AS (
  SELECT studyId,
         array_to_string(list_sort(list_distinct(diseaseIds)), '|') AS trait_key,
         list_transform(ldPopulationStructure, x -> x.ldPopulation) AS pops
  FROM eligible
),
unnested AS (
  SELECT trait_key, studyId, unnest(pops) AS pop
  FROM keyed
),
sets AS (
  SELECT trait_key,
         count(DISTINCT studyId) AS n_studies,
         count(DISTINCT pop) AS n_ancestries
  FROM unnested
  GROUP BY trait_key
)
SELECT count(*) AS total_sets,
       count(*) FILTER (WHERE n_ancestries >= 2) AS cross_ancestry_sets,
       count(*) FILTER (WHERE n_ancestries = 1) AS single_ancestry_sets,
       max(n_studies) AS max_studies_in_a_set,
       max(n_ancestries) AS max_ancestries_in_a_set
FROM sets;

-- 5. Post-collapse ancestry-count distribution: keep one representative study per (trait, ancestry),
--    picking the largest nSamples, then count how many ancestries survive per trait-set.
WITH eligible AS (
  SELECT studyId, diseaseIds, ldPopulationStructure, nSamples
  FROM study
  WHERE studyType = 'gwas'
    AND hasSumstats = true
    AND ldPopulationStructure IS NOT NULL
    AND len(ldPopulationStructure) >= 1
),
keyed AS (
  SELECT studyId, nSamples,
         array_to_string(list_sort(list_distinct(diseaseIds)), '|') AS trait_key,
         list_transform(ldPopulationStructure, x -> x.ldPopulation) AS pops
  FROM eligible
),
unnested AS (
  SELECT trait_key, studyId, nSamples, unnest(pops) AS pop
  FROM keyed
),
ranked AS (
  SELECT trait_key, pop, studyId, nSamples,
         row_number() OVER (PARTITION BY trait_key, pop ORDER BY nSamples DESC) AS rk
  FROM unnested
),
reps AS (
  SELECT trait_key, pop, studyId FROM ranked WHERE rk = 1
),
collapsed_sets AS (
  SELECT trait_key, count(DISTINCT pop) AS n_ancestries
  FROM reps
  GROUP BY trait_key
)
SELECT n_ancestries, count(*) AS n_sets
FROM collapsed_sets
WHERE n_ancestries >= 2
GROUP BY n_ancestries
ORDER BY n_ancestries;
