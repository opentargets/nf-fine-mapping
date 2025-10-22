process GENTROPY_LD_PREPARE {
    tag "${meta.id}"
    label 'process_single'

    // WARN: Version information not provided by tool on CLI. Please update version string below when bumping container versions.
    container "docker.io/library/gentropy:000"

    input:
    tuple val(meta), path(tsv), val(ancestry)

    output:
    tuple val(meta), path("*.parquet"), val(ancestry), path("*.ld"), emit: parquet
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def VERSION = '2.4.2-internal.3'
    // WARN: Version information not provided by tool on CLI. Please update this string when bumping container versions.
    // ${args} \
    // 
    """ 
    gentropy \\
        step=sushie_ld_input \\
        step.sushie_sumstat_input_path=${tsv} \\
        step.ld_block_matrix_path= \\
        step.ld_slice_output_path=${prefix}.ld.bm \\
        step.ancestry=${ancestry} \\
        ${args}

    cat <<-END_VERSIONS > versions.yml
        gentropy: "${VERSION}"
    END_VERSIONS
    """

    stub:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def VERSION = '2.4.2-internal.3'
    // WARN: Version information not provided by tool on CLI. Please update this string when bumping container versions.
    """
    echo ${args}
    
    touch ${prefix}.parquet

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        gentropy: ${VERSION}
    END_VERSIONS
    """
}



workflow {
    ch = Channel.fromPath(params.input)
    // Extract metadata
    metaCh = ch.map { path ->
        def m = path =~ /leadvariantId=([^\/]+)\/ldPopulation=([^\/]+)/
        if (m) {
            def leadVariantId = m[0][1]
            def pop = m[0][2]
            def meta = [id: leadVariantId]
            tuple(meta, path, pop)
        }
        else {
            tuple(null, path, null)
        }
    }

    // Test output
    metaCh.subscribe { println(it) }
    g = GENTROPY_LD_PREPARE(metaCh)
}
