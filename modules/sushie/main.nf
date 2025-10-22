
workflow {
    test_sumstats = Channel
        .fromPath("/home/louwenjjr/nf-fine-mapping/data_generation/example_sumstats/leadvariantId=1_205247445_G_A/ldPopulation=nfe/part-00000-34baccd9-028b-4908-9910-95a406f69548.c000.csv")
        .collect()

    test_ld = Channel
        .fromPath("/home/louwenjjr/nf-fine-mapping/data_generation/example_sumstats/NFE.ld.bm")
        .collect()

    test_sample_sizes = Channel.value('408112')
    output            = Channel.value('1_205247445_G_A_NFE')
    
    SUSHIE(test_sumstats, test_ld, test_sample_sizes, output)
}

process SUSHIE {
    container "sushie"

    input:
    path study_locus_files
    // LD files: only column names (variant ids), variant ids need to correspond to sumstats file
    path ld_files
    val sample_sizes
    val output_prefix

    output:
    path "*.sushie.corr.tsv",    emit: corr
    path "*.sushie.cs.tsv",      emit: cs
    path "*.sushie.weights.tsv", emit: weights

  shell:
  args = task.ext.args ?: ''
  """
  sushie finemap \
    --summary \
    --gwas ${study_locus_files.join(' ')} \
    --ld ${ld_files.join(' ')} \
    --sample-size ${sample_sizes} \
    --output ${output_prefix} \
    --gwas-header chromosome variantId position referenceAllele alternateAllele zScore \
    $args
  """
}
