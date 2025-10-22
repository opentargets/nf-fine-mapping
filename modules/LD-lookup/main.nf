// modules 
test_input_file="/home/whittog/src/nf-fine-mapping/data_generation/outputs/example_sumstats/leadvariantId=1_205136853_A_G/ldPopulation=afr/part-00000-728dce75-dea7-497a-a7dc-2ce5ca8b2d44-c000.csv"
test_an="nfe"
test_gcs="gs://genetics-portal-dev-data/fine_mapping/ld_matrices/2024-06-03"
chrom=1
start=204_910_910
end=205_902_271
lead_variant="1_205136853_A_G"

workflow {
    GET_LD_MATRIX(test_an, test_gcs, chrom, start, end, lead_variant)
}


process GET_LD_MATRIX {
    input:
      val ancestry
      val gcs_path
      val chrom
      val start
      val end
      val lead_variant

    output:
      path "ld_matrix.npy"

    script:
      """
      source /home/whittog/src/nf-fine-mapping/.venv/bin/activate
      python ${workflow.projectDir}/ldlookup.py \
        --ancestry ${test_an} \
        --ld_path ${test_gcs} \
        --chrom ${chrom} \
        --start ${start} \
        --end ${end} \
        --lead_variant ${lead_variant} \
        --output ${lead_variant}_${test_an}_ld_matrix.npy
      """

}
