process Collector {
    container "collector"

    input:
    tuple val(meta), path(parquet_directory)

    output:
    tuple val(meta), path("${meta.trait}_${meta.ancestry}.parquet"), emit: collected

    script:
    """
    collector \
        --input ${parquet_directory} \
        --output "${meta.trait}_${meta.ancestry}.parquet"
    """
}
