process SubsetLD {
  container "ld"

  input:
  path variant_intersection_ch
  path ld_reference_ch

  output:
  path "*.ld.tsv", emit: ld

  script:
  """
  subset_ld \
    --variants ${variant_intersection_ch} \
    --ld-reference ${ld_reference_ch.join(' ')} \
    --output ${task.workDir}/${task.processName}.ld.tsv
  """
}
