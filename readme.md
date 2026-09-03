# nf-fine-mapping

**opentargets/nf-fine-mapping** is a workflow designed to perform the fine-mapping of Genome Wide Association Studies (GWAS) summary statistics. 

The workflow is designed to be an integral part of the Open Targets ecosystem and fits into the data model designed for post GWAS analysis in [Open Targets Gentropy](https://github.com/opentargets/gentropy). The workflow is designed to be run by default on the Google Cloud Platform (GCP) with support of [Nextflow](https://www.nextflow.io/) and contrainerization. 


## Rationale

The fine-mapping process can be run on two resolutions:

* **genotype resolution**: fine-mapping can be performed on the level of individual genotypes. This is the most accurate approach, although it is not suitable for large-scale post GWAS analysis as it requires access to individual-level genotype data that is most of the time not available due to privacy concerns.

* **summary statistics resolution**: fine-mapping can utilize open-access summary statistics in standardised and harmonised format. There are multiple resources that provide such data up to date (ex. GWAS Catalog). To perform the fine-mapping using summary statistics one need to have access to the Linkage Disequilibirum information. This approach can still utilize the LD information from within-sample (in-sample LD) if available, otherwise exteranl reference LD panels can be utilized. The external panels can produce less accurate results (out-of-sample LD).


* **multi-ancestry fine-mapping**: fine-mapping can be performed on the set of studies that test the same trait over many populations. 

### Workflow

Our workflow is designed to be run at scale (pilot run was performed on 26.06 OpenTargets study index that reflects all representative studies from GWAS Catalog up to May 2026, N~7k study pairs/triplets). Due to the scale of the data we can not rely on in-sample-ld.

nf-fine-mapping implements the **summary statistics resolution** fine-mapping approach with **out-of-sample LD**. Workflow utilizes **panUKBB LD Matrices** openly available to derive the LD information.

The workflow allows for running following fine-mapping methods via execution routes (subworkflows)

## Documentation

The full pipeline documentation covers execution profiles, locus processing,
workflow contracts, fine-mapping routes, and the collector API. Build it with:

```bash
make docs
```

The generated site is available at `docs/_build/html/index.html`.

## Summary Statistics Clumping

Summary statistics are clumped into study-locus datasets using the
collector implementation backed by DuckDB.

## Unified LD interface

Fine-mapping routes consume an ancestry-aware LD interface backed by Hailing
Ducks so that LD storage and retrieval remain independent from locus processing.

## Available fine-mapping routes

Routes assemble candidate loci, summary statistics, LD inputs, and
method-specific parameters for the selected fine-mapping method.

### multiSuSiE

MultiSuSiE consumes fully overlapping multi-study locus sets produced by the
locus-collection workflow.


