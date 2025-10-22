import sys
from pyspark.sql import SparkSession

help = """
Example usage:
python convert_sushie_to_gentropy.py <sumstats_path> <sushie_cv_path> <study_locus_out>
python convert_sushie_to_gentropy.py \
    ../example_sumstats/leadvariantId=1_205247445_G_A/ldPopulation=afr/part-00000-34baccd9-028b-4908-9910-95a406f69548.c000.csv \
    ../outputs/example_sushie_out_cv.tsv
    ../outputs/example_credible_set.parquet
"""

def main ():
    spark = (
        SparkSession.builder.appName("sushie_output")
        .config("spark.driver.memory", "5g")
        .getOrCreate()
    )

    sumstats_path = sys.argv[1]
    sushie_cv_path = sys.argv[2]
    study_locus_out = sys.argv[3]


if __name__ == "__main__":
    main()