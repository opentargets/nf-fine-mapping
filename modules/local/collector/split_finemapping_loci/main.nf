nextflow.enable.dsl = 2
nextflow.enable.types = true


process SPLIT_FINEMAPPING_LOCI {
    tag "${runId}"

    label "collector"
    label "locus_collection"

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'fine_mapping_locus_sets'

    input:
    tuple(runId: String, metas: List, full_overlap_path: Path)

    output:
    fine_mapping_locus_sets = record(
        runId: runId,
        metas: metas,
        fine_mapping_locus_set_dir: file("fine_mapping_locus_sets", type: 'dir'),
    )

    topic:
    tuple("${task.process}", "collector", "1.0.0") >> "versions"

    script:
    def args = task.ext.args ?: ''
    """
    mkdir -p fine_mapping_locus_sets

    collector split_finemapping_loci \\
        --input ${full_overlap_path} \\
        --output fine_mapping_locus_sets \\
        ${args}
    """

    stub:
    """
    mkdir -p fine_mapping_locus_sets
    touch fine_mapping_locus_sets/set-a.parquet
    touch fine_mapping_locus_sets/set-b.parquet
    """
}
