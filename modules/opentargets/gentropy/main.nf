process GENTROPY {
    tag "$meta.id"
    label 'process_single'

    // WARN: Version information not provided by tool on CLI. Please update version string below when bumping container versions.
    container "ghcr.io/opentargets/gentropy:2.4.2-internal.3"

    input:
    tuple val(meta), path(tsv)

    output:
    tuple val(meta), path("*.parquet"), emit: parquet
    path "versions.yml"               , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def VERSION = '2.4.2-internal.3' // WARN: Version information not provided by tool on CLI. Please update this string when bumping container versions.
    """
    gentropy \\
        $args \\
        step=gwas_catalog_sumstat_preprocess \\
        -o ${prefix}.parquet \\
        $tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        gentropy: $VERSION
    END_VERSIONS
    """

    stub:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def VERSION = '2.4.2-internal.3' // WARN: Version information not provided by tool on CLI. Please update this string when bumping container versions.
    """
    echo $args
    
    touch ${prefix}.parquet

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        gentropy: $VERSION
    END_VERSIONS
    """
}
