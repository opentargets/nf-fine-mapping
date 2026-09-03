# Test manifest selection query

This DuckDB script rebuilds a fine-mapping test manifest from an Open Targets
release and a full manifest stored in GCS.

Set these three parameters at the top of the script:

1. `n_studies`: number of ranked studies to select; `20` reproduces
   `testdata/manifest.test3.tsv`.
2. `manifest_path`: full GCS path to the manifest.
3. `ot_release`: Open Targets release, for example `26.06`.

The query requires DuckDB's `httpfs` extension and credentials that can read
the public Open Targets S3 parquet files and the selected GCS object. Run it
with:

```bash
duckdb < query.sql
```

The script writes `manifest.test3.tsv` in the current directory. The final
diagnostic query should return zero rows; any returned row identifies a
component study referenced by a selected `runId` but absent from the manifest
`studyId` column.

## query.sql

```sql
-- Three parameters.
SET VARIABLE n_studies = 20;
SET VARIABLE manifest_path =
    'gs://gwas_catalog_multi_ancestry_fine_mapping/manifest.tsv';
SET VARIABLE ot_release = '26.06';

LOAD httpfs;

-- OTAI's release parquet layout.
CREATE OR REPLACE TEMP TABLE ot_study AS
SELECT *
FROM read_parquet(
    's3://open-targets-public-data-releases/platform/'
    || getvariable('ot_release')
    || '/output/study/*.parquet'
);

CREATE OR REPLACE TEMP TABLE ot_disease AS
SELECT *
FROM read_parquet(
    's3://open-targets-public-data-releases/platform/'
    || getvariable('ot_release')
    || '/output/disease/*.parquet'
);

CREATE OR REPLACE TEMP TABLE ot_credible_set AS
SELECT *
FROM read_parquet(
    's3://open-targets-public-data-releases/platform/'
    || getvariable('ot_release')
    || '/output/credible_set/*.parquet'
);

CREATE OR REPLACE TEMP TABLE full_manifest AS
SELECT *
FROM read_csv(
    getvariable('manifest_path'),
    delim = '\t',
    header = true,
    all_varchar = true
);

-- GWAS studies with at least one disease that is not descended from the
-- Open Targets measurement root EFO_0001444.
CREATE OR REPLACE TEMP TABLE eligible_studies AS
SELECT DISTINCT s.studyId
FROM ot_study AS s
CROSS JOIN UNNEST(s.diseaseIds) AS disease_id(disease_id)
INNER JOIN ot_disease AS d
    ON d.id = disease_id.disease_id
WHERE lower(s.studyType) = 'gwas'
  AND NOT list_contains(d.ancestors, 'EFO_0001444')
  AND d.id <> 'EFO_0001444';

-- Count variants in each credible set's 95% and 99% credible sets.
CREATE OR REPLACE TEMP TABLE filtered_credible_sets AS
SELECT
    c.studyId,
    c.studyLocusId,
    list_count(
        list_filter(c.locus, x -> coalesce(x.is95CredibleSet, false))
    ) AS locus_size_95,
    list_count(
        list_filter(c.locus, x -> coalesce(x.is99CredibleSet, false))
    ) AS locus_size_99
FROM ot_credible_set AS c
INNER JOIN eligible_studies AS e
    ON e.studyId = c.studyId
WHERE c.finemappingMethod = 'SuSiE-inf';

-- Inner join to manifest study IDs before ranking. DISTINCT prevents the
-- manifest's ancestry rows from weighting a credible set more than once.
CREATE OR REPLACE TEMP TABLE ranked_studies AS
SELECT
    c.studyId,
    count(*) AS credible_set_count,
    avg(c.locus_size_95) AS mean_locus_size_95,
    avg(c.locus_size_99) AS mean_locus_size_99
FROM filtered_credible_sets AS c
INNER JOIN (
    SELECT DISTINCT studyId
    FROM full_manifest
) AS m
    ON m.studyId = c.studyId
GROUP BY c.studyId
ORDER BY mean_locus_size_99 DESC, c.studyId
LIMIT getvariable('n_studies');

-- Select complete run groups belonging to the ranked studies.
CREATE OR REPLACE TEMP TABLE selected_run_ids AS
SELECT DISTINCT m.runId
FROM full_manifest AS m
INNER JOIN ranked_studies AS r
    ON r.studyId = m.studyId;

CREATE OR REPLACE TEMP TABLE run_components AS
SELECT DISTINCT
    m.runId,
    trim(component.studyId) AS studyId
FROM full_manifest AS m
CROSS JOIN UNNEST(string_split(m.runId, ',')) AS component(studyId);

CREATE OR REPLACE TEMP TABLE complete_run_ids AS
SELECT rc.runId
FROM run_components AS rc
GROUP BY rc.runId
HAVING count(*) = count(
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM full_manifest AS m
            WHERE m.studyId = rc.studyId
        )
        THEN 1
    END
);

-- Output every manifest row for each selected runId, retaining all component
-- studies and all available ancestry rows.
COPY (
    SELECT m.*
    FROM full_manifest AS m
    INNER JOIN selected_run_ids AS selected
        ON selected.runId = m.runId
    INNER JOIN complete_run_ids AS complete
        ON complete.runId = m.runId
    ORDER BY m.runId, m.studyId
) TO 'manifest.test3.tsv' (
    HEADER,
    DELIMITER '\t'
);

-- Review the ranking and verify the selected run groups.
SELECT *
FROM ranked_studies
ORDER BY mean_locus_size_99 DESC, studyId;

-- This must return zero rows.
SELECT
    rc.runId,
    rc.studyId AS missing_component_studyId
FROM run_components AS rc
INNER JOIN selected_run_ids AS selected
    ON selected.runId = rc.runId
WHERE NOT EXISTS (
    SELECT 1
    FROM full_manifest AS m
    WHERE m.studyId = rc.studyId
)
ORDER BY rc.runId, rc.studyId;
```
