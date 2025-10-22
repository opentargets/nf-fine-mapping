
workflow {
    test_sumstats = Channel
        .fromPath('/workspaces/nf-fine-mapping/sushie/data/{EUR,AFR}.gwas')
        .collect()

    test_ld = Channel
        .fromPath('/workspaces/nf-fine-mapping/sushie/data/{EUR,AFR}.ld')
        .collect()

    test_sample_sizes = Channel.value('489 639')
    output            = Channel.value('test_output')
    
    SUSHIE(test_sumstats, test_ld, test_sample_sizes, output)
}

process SUSHIE {
    container "docker.io/cameronlloyd/sushie:latest"

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
    $args
  """
}
