nextflow.enable.dsl = 2
nextflow.enable.types = true


process COLLECT_FINEMAPPING_LOCI {
    tag "${runId}"

    label "collector"

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'collected_loci/**/*'

    input:
    tuple(runId: String, metas: List, study_locus_paths: List<Path>)

    output:
    full_overlap = tuple(runId, metas, file("collected_loci/full_overlaps/*.parquet", optional: true))
    partial_overlap = tuple(runId, metas, file("collected_loci/partial_overlaps/*.parquet"))
    non_overlap = tuple(runId, metas, file("collected_loci/non_overlaps/*.parquet"))
    stats = tuple(runId, metas, file("collected_loci/stats/*.json"))

    topic:
    tuple("${task.process}", "collector", "1.0.0") >> "versions"

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: runId
    def input_args = study_locus_paths.collect { path_item -> "--input ${path_item}" }.join(' ')
    """
    mkdir -p collected_loci/full_overlaps collected_loci/partial_overlaps collected_loci/non_overlaps collected_loci/stats

    collector collect_finemapping_loci \
        ${input_args} \
        --full_output collected_loci/full_overlaps/${prefix}.parquet \
        --partial_output collected_loci/partial_overlaps/${prefix}.parquet \
        --non_overlap_output collected_loci/non_overlaps/${prefix}.parquet \
        --stats_output collected_loci/stats/${prefix}.json \
        ${args}
    """

    stub:
    def prefix = task.ext.prefix ?: runId
    """
    mkdir -p collected_loci/partial_overlaps collected_loci/non_overlaps collected_loci/stats
    touch collected_loci/partial_overlaps/${prefix}.parquet
    touch collected_loci/non_overlaps/${prefix}.parquet
    touch collected_loci/stats/${prefix}.json
    """
}
