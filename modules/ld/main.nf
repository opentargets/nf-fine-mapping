process SubsetLD {
  label "ld"

  input:
  tuple val(meta), path(intersection), val(hail_table), val(block_matrix)

  output:
  tuple val(meta), path("${meta.trait}_${meta.ancestry}.ld.bgzip"), emit: ld_output

  script:
  """
  subset_ld \
    --variants ${intersection} \
    --hail-table ${hail_table} \
    --block-matrix ${block_matrix} \
    --chain ${params.chain} \
    --liftover ${params.liftover} \
    --r2 ${params.r2} \
    --output "${meta.trait}_${meta.ancestry}.ld.bgzip" \
    --
  """

  stub:
  """
  touch "${meta.trait}_${meta.ancestry}.ld.bgzip"
  """
}
