#!/usr/bin/env bash
# Re-run the scale-hook queries in scale_hook_queries.sql against Open Targets release 26.06
# via otai, to verify the numbers cited in abstract.qmd's Background section.
#
# Usage: ./run_otai_queries.sh
#
# Requires: uv / uvx (https://docs.astral.sh/uv/). No local otai install needed —
# uvx fetches and runs it straight from the Open Targets GitHub repo.

set -euo pipefail

OTAI="uvx --from git+https://github.com/opentargets/otai.git otai"

run() {
  local label="$1"
  local sql="$2"
  echo "== ${label} =="
  $OTAI run-sql "${sql}" --timeout 90
  echo
}

run "1. Project IDs behind GWAS Catalog / UKB-PPP" "
SELECT projectId, count(*) n
FROM study
WHERE studyType = 'gwas' OR studyType = 'pqtl'
GROUP BY projectId
ORDER BY n DESC"

run "2. Credible sets + studies already fine-mapped (GCST, UKB_PPP_EUR)" "
SELECT s.projectId, count(*) n_credible_sets, count(DISTINCT cs.studyId) n_studies
FROM credible_set cs
JOIN study s ON cs.studyId = s.studyId
WHERE s.projectId IN ('GCST', 'UKB_PPP_EUR')
GROUP BY s.projectId
ORDER BY n_credible_sets DESC"

run "3. Breakdown by fine-mapping method" "
SELECT cs.finemappingMethod, s.projectId, count(*) n
FROM credible_set cs
JOIN study s ON cs.studyId = s.studyId
WHERE s.projectId IN ('GCST', 'UKB_PPP_EUR')
GROUP BY cs.finemappingMethod, s.projectId
ORDER BY n DESC"

run "4. Multi-ancestry trait-set feasibility" "
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
FROM sets"

run "5. Post-collapse ancestry-count distribution" "
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
ORDER BY n_ancestries"
