#!/usr/bin/env python3
"""
Usage:
    python extract_ld_matrix_gnomad.py \
        --ancestry afr \
        --chrom 1 --start 123456 --end 125000 \
        --lead_variant 1_123456_A_G \
        --output ld_matrix.npy
"""

import argparse
import numpy as np
from pyspark.sql import SparkSession, Row
from gentropy.common.session import Session
#from gentropy.method.ld_matrix_interface import LDMatrixInterface - cant use this function
from gentropy.datasource.gnomad.ld import GnomADLDMatrix
import os
import hail as hl

os.environ["JAVA_HOME"] = "/home/whittog/.sdkman/candidates/java/17.0.16-tem"
os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin:" + os.environ["PATH"]
gcs_connector = os.path.expanduser("~/hadoop-lib/gcs-connector-hadoop3-latest.jar")
os.environ["SPARK_CLASSPATH"] = f"{gcs_connector}:{os.environ.get('SPARK_CLASSPATH', '')}"
os.environ["HAIL_EXTRA_SPARK_ARGS"] = f"--jars {gcs_connector}"

hl.init(
    app_name="gnomad_ld_lookup",
    quiet=True,
    tmp_dir="file:///tmp",
    log="/tmp/hail.log",
    default_reference="GRCh38",
    spark_conf={
        "spark.driver.extraClassPath": gcs_connector,
        "spark.executor.extraClassPath": gcs_connector,
        "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "spark.jars": gcs_connector,
        "spark.hadoop.google.cloud.auth.service.account.enable": "true",
        "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        "spark.driver.memory": "16G",          # Increase driver memory
        "spark.executor.memory": "16G",        # Increase executor memory if needed
        "spark.driver.maxResultSize": "8G",
    }
)

def main():
    parser = argparse.ArgumentParser(description="Extract gnomAD LD matrix for a given ancestry")
    parser.add_argument("--ancestry", required=True, help="Ancestry string (e.g. afr, nfe, amr)")
    parser.add_argument("--ld_path", required=True, help="ld path not needed")
    parser.add_argument("--chrom", required=True, help="Chromosome (e.g. 1)")
    parser.add_argument("--start", type=int, required=True, help="Start position (bp)")
    parser.add_argument("--end", type=int, required=True, help="End position (bp)")
    parser.add_argument("--lead_variant", required=True, help="Lead variant ID (e.g. 1_123456_A_G)")
    parser.add_argument("--output", default="ld_matrix.npy", help="Output file (.npy or .csv)")
    args = parser.parse_args()

    # --- Spark session with GCS support ---
    spark = SparkSession.builder \
        .appName("gnomAD_LDMatrixExtraction") \
        .config("spark.jars", "/home/whittog/src/nf-fine-mapping/modules/LD-lookup/jars/gcs-connector-hadoop3-2.2.17-shaded.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .getOrCreate()

    session = Session(spark)

    pop = args.ancestry.lower()
    
    # force the function to accept gnoMAD paths
    # gnomad_paths = {
    # "bm_path": f"gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{pop}.common.ld.bm",
    # "ht_path": f"gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{pop}.common.ld.variant_indices.ht",
    # "liftover_ht_path": "gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/ht/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht",
    # }

    study_locus_row = Row(
        chromosome=str(args.chrom),
        locusStart=args.start,
        locusEnd=args.end,
        lead_variant=args.lead_variant
    )
    
    gnomad_ld = GnomADLDMatrix()

    locus_index = gnomad_ld.get_locus_index_boundaries(
        study_locus_row=study_locus_row,
        major_population=args.ancestry
    )

    ld_matrix = gnomad_ld.get_numpy_matrix(
        locus_index=locus_index,
        gnomad_ancestry=args.ancestry
    )

    print(ld_matrix.shape)
    # --- Save output ---
    if args.output.endswith(".csv"):
        np.savetxt(args.output, ld_matrix, delimiter=",")
    else:
        np.save(args.output, ld_matrix)

    print(f"LD matrix saved to {args.output}")

if __name__ == "__main__":
    main()
