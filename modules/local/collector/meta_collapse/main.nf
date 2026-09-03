nextflow.enable.dsl = 2
nextflow.enable.types = true


process COLLECTOR_META_COLLAPSE {
    tag "${runId}:${fine_mapping_locus_set_id}:${arm}"

    label "collector"
    label "meta_collapse"

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'meta_collapse/**/stats.json'

    input:
    tuple(
        runId: String,
        fine_mapping_locus_set_id: String,
        arm: String,
        mode: String,
        target_ancestry: String,
        // every arm's studyId -> ancestry -> sampleSize, needed to weight the collapse
        metas: List,
        // the collapsed arm's metadata, echoed unchanged for the fine-mapping process
        collapsed_metas: List,
        fine_mapping_locus_set_path: Path,
        multi_ancestry_pairwise_ld_path: Path,
    )

    output:
    collapsed = record(
        runId: runId,
        fine_mapping_locus_set_id: fine_mapping_locus_set_id,
        arm: arm,
        metas: collapsed_metas,
        fine_mapping_locus_set_path: file("meta_collapse/${arm}/fine_mapping_locus_set.parquet"),
        multi_ancestry_pairwise_ld_path: file("meta_collapse/${arm}/multi_ancestry_pairwise_ld.parquet"),
        stats_path: file("meta_collapse/${runId}/${fine_mapping_locus_set_id}/${arm}/stats.json"),
    )

    topic:
    tuple("${task.process}", "collector", "1.1.0") >> "versions"

    script:
    def args = task.ext.args ?: ''
    def target_ancestry_arg = mode == 'single' ? "--target_ancestry '${target_ancestry}'" : ''
    def metadata_lines = metas
        .collect { meta ->
            groovy.json.JsonOutput.toJson(
                [
                    studyId: meta.studyId,
                    ancestry: meta.ancestry,
                    sampleSize: meta.sampleSize,
                ]
            )
        }
        .join('\n')
    """
    mkdir -p meta_collapse/${arm}
    mkdir -p meta_collapse/${runId}/${fine_mapping_locus_set_id}/${arm}

    printf '%s\\n' '${metadata_lines}' > metadata.jsonl

    collector meta_collapse \\
        --input ${fine_mapping_locus_set_path} \\
        --multi_ancestry_pairwise_ld ${multi_ancestry_pairwise_ld_path} \\
        --study_metadata metadata.jsonl \\
        --output meta_collapse/${arm}/fine_mapping_locus_set.parquet \\
        --ld_output meta_collapse/${arm}/multi_ancestry_pairwise_ld.parquet \\
        --stats_output meta_collapse/${runId}/${fine_mapping_locus_set_id}/${arm}/stats.json \\
        --run_id '${runId}' \\
        --fine_mapping_locus_set_id '${fine_mapping_locus_set_id}' \\
        --mode '${mode}' \\
        --diagonal_tolerance ${params.meta_collapse_diagonal_tolerance} \\
        --max_missing_pair_fraction ${params.meta_collapse_max_missing_pair_fraction} \\
        ${target_ancestry_arg} \\
        ${args}
    """

    stub:
    """
    mkdir -p meta_collapse/${arm}
    mkdir -p meta_collapse/${runId}/${fine_mapping_locus_set_id}/${arm}
    touch meta_collapse/${arm}/fine_mapping_locus_set.parquet
    touch meta_collapse/${arm}/multi_ancestry_pairwise_ld.parquet
    printf '%s\\n' '{"mode":"${mode}","maxAbsDiagonalDeviation":0.0}' \\
        > meta_collapse/${runId}/${fine_mapping_locus_set_id}/${arm}/stats.json
    """
}
