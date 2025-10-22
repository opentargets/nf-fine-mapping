import sys
from pyspark.sql import SparkSession
from gentropy.dataset.study_locus import StudyLocus
from pyspark.sql import functions as F


help = """
Example usage:
python convert_sushie_to_gentropy.py <sumstats_path> <sushie_cv_path> <study_locus_out>
python convert_sushie_to_gentropy.py \
    ../example_sumstats/leadvariantId=1_205247445_G_A/ldPopulation=afr/part-00000-34baccd9-028b-4908-9910-95a406f69548.c000.csv \
    ../outputs/sushie_output/test_output.sushie.cs.tsv
    ../outputs/example_credible_set.parquet
"""

def main ():
    spark = (
        SparkSession.builder.appName("sushie_output")
        .config("spark.driver.memory", "5g")
        .getOrCreate()
    )

    sumstats_path = sys.argv[1]
    sushie_cs_path = sys.argv[2]
    study_locus_out = sys.argv[3]

    # from sushie_cs.tsv get the variants present in the credsets and use pip_cs column for the pip
    # the mu is probably the ancestryX_sushie_weight column in the sushie_weights.tsv (ancestry specific), but for now we will just use the beta from the input sumstats_path
    sumstats_df = spark.read.csv(sumstats_path, sep="\t", header=True)
    sushie_cs_df = spark.read.csv(sushie_cs_path, sep="\t", header=True)

    # merge on variant id, add pip_cs column from sushie_cs_df
    merged_df = sumstats_df.join(
        sushie_cs_df.select("snp", "pip_cs", "CSIndex"),
        sumstats_df["variantId"] == sushie_cs_df["snp"],
        "inner"
        ).withColumnRenamed("pip_cs", "posteriorProbability"
        ).withColumn("is95CredibleSet", F.lit(True))
    merged_df.show()

    # for every variant with the same CSIndex, create a StudyLocus object with lead variant (lowest pval) and add the variants to it
    # get back the pvalue so we can find the lead variants
    merged_df = merged_df.withColumn(
            "computed_pvalue",
            F.col("pValueExponent") * F.pow(F.lit(10), F.col("pValueMantissa"))
        )
    cs_numbers = [r.CSIndex for r in merged_df.select("CSIndex").distinct().collect()]
    print(cs_numbers)
    for cs_number in cs_numbers:
        cs_variants = merged_df.filter(F.col("CSIndex") == cs_number)

        lead_variant = merged_df.orderBy("computed_pvalue").first()
        lead_variant = lead_variant.withColumn("finemappingMethod", "SuShiE")
        print(lead_variant)
        print(f"Lead variant: {lead_variant['variantId']} with pval {lead_variant['computed_pvalue']}")
        study_locus_df
#         study_locus = StudyLocus(_df=merged_df, _schema=StudyLocus.get_schema())
#         study_locus.show()


if __name__ == "__main__":
    main()