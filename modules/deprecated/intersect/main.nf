process Intersect {
    label "intersect"

    input:
    tuple val(trait), val(meta), path(sumstats)

    output:
    tuple val(trait), path("intersection_${trait}.parquet"), emit: intersection

    script:
    def sumstat_list = sumstats instanceof List ? sumstats : [sumstats]
    def sumstat_flags = sumstat_list.collect { sumstat -> "--sumstat ${sumstat}" }.join(" ")
    """
    collector intersect \
        ${sumstat_flags} \
        --output "intersection_${trait}.parquet"
    """

    stub:
    """
    touch "intersection_${trait}.parquet"
    """
}
