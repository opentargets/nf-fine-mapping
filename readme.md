# IDIC Infinite Diversity in Infinite Combinations fine-mapping

**opentargets/IDIC** is a workflow designed to perform the fine-mapping of Genome Wide Association Studies (GWAS) summary statistics. 

The workflow is designed to be an integral part of the Open Targets ecosystem and fits into the data model designed for post GWAS analysis in [Open Targets Gentropy](https://github.com/opentargets/gentropy). The workflow is designed to be run by default on the Google Cloud Platform (GCP) with support of [Nextflow](https://www.nextflow.io/) and contrainerization. 


## Rationale

The fine-mapping process can be run on two resolutions:

* **genotype resolution**: fine-mapping can be performed on the level of individual genotypes. This is the most accurate approach, although it is not suitable for large-scale post GWAS analysis as it requires access to individual-level genotype data that is most of the time not available due to privacy concerns.

* **summary statistics resolution**: fine-mapping can utilize open-access summary statistics in standardised and harmonised format. There are multiple resources that provide such data up to date (ex. GWAS Catalog). To perform the fine-mapping using summary statistics one need to have access to the Linkage Disequilibirum information. This approach can still utilize the LD information from within-sample (in-sample LD) if available, otherwise exteranl reference LD panels can be utilized. The external panels can produce less accurate results (out-of-sample LD).


* **multi-ancestry fine-mapping**: fine-mapping can be performed on the set of studies that test the same trait over many populations. 

### Workflow

Our workflow is designed to be run at scale (pilot run was performed on 26.06 OpenTargets study index that reflects all representative studies from GWAS Catalog up to May 2026, N~7k study pairs/triplets). Due to the scale of the data we can not rely on in-sample-ld.

IDIC workflow implements the **summary statistics resolution** fine-mappong approach with **out-of-sample LD**. Workflow utilizes **panUKBB LD Matrices** openly available to derive the LD information.

The workflow allows for running following fine-mapping methods via execution routes (subworkflows)


## Summary Statistics Clumping

TBA

## Unified LD interface

TBA

## Available fine-mapping routes

TBA

### multiSuSiE

TBA



