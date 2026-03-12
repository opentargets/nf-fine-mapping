process Transform {
    label "transform"

    input:
    tuple val(trait), val(meta), path(sumstats), path(intersection)

    output:
    tuple val(trait), val(meta), path("transformed_${trait}_${meta.ancestry}.parquet"), emit: transformed

    script:
    """
    transform \
        --sumstat ${sumstats} \
        --intersection ${intersection} \
        --output "transformed_${trait}_${meta.ancestry}.parquet"
    """

    stub:
    """
    touch "transformed_${trait}_${meta.ancestry}.parquet"
    """
}
