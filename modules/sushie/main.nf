process SuShiE {
  label "sushie"

  publishDir "${params.output_dir}/sushie/corr/", mode: 'copy', overwrite: true, pattern: "*.sushie.corr.tsv"
  publishDir "${params.output_dir}/sushie/credible_sets", mode: 'copy', overwrite: true, pattern: "*.sushie.cs.tsv"
  publishDir "${params.output_dir}/sushie/weights", mode: 'copy', overwrite: true, pattern: "*.sushie.weights.tsv"
  publishDir "${params.output_dir}/sushie/logs", mode: 'copy', overwrite: true, pattern: "*.log"

  input:
  tuple val(trait), val(ancestries), val(sample_sizes), path(sumstats), path(ld)

  output:
  path "*.sushie.corr.tsv", emit: corr
  path "*.sushie.cs.tsv", emit: cs
  path "*.sushie.weights.tsv", emit: weights
  path "*.log", emit: log

  script:
  args = task.ext.args ?: ''
  sumstats = sumstats instanceof List ? sumstats : [sumstats]
  ld_files = ld instanceof List ? ld : [ld]
  sample_sizes = sample_sizes instanceof List ? sample_sizes : [sample_sizes]
  ancestries = ancestries instanceof List ? ancestries : [ancestries]
  ancestries_sorted = ancestries.sort()
  """
    sushie finemap \
        --summary \
        --gwas ${sumstats.join(' ')} \
        --ld ${ld_files.join(' ')} \
        --sample-size ${sample_sizes.join(' ')} \
        --output ${trait}-${ancestries_sorted.join('_')} \
        --gwas-header chromosome variantId position referenceAllele alternateAllele zScore \
        ${args}
      """

  stub:
  """
    touch "${trait}-${ancestries_sorted.join('_')}.sushie.corr.tsv"
    touch "${trait}-${ancestries_sorted.join('_')}.sushie.cs.tsv"
    touch "${trait}-${ancestries_sorted.join('_')}.sushie.weights.tsv"
    touch "${trait}-${ancestries_sorted.join('_')}.log"
    """
}
