process SuShiE {
    container "sushie"

    input:
    path study_locus_files
    // LD files: only column names (variant ids), variant ids need to correspond to sumstats file
    path ld_files
    val sample_sizes
    val output_prefix

    output:
    path "*.sushie.corr.tsv", emit: corr
    path "*.sushie.cs.tsv", emit: cs
    path "*.sushie.weights.tsv", emit: weights

    script:
    args = task.ext.args ?: ''
    """
    sushie finemap \
        --summary \
        --gwas ${study_locus_files.join(' ')} \
        --ld ${ld_files.join(' ')} \
        --sample-size ${sample_sizes} \
        --output ${output_prefix} \
        --gwas-header chromosome variantId position referenceAllele alternateAllele zScore \
        ${args}
  """
}
