nextflow.enable.dsl = 2
nextflow.enable.types = true


process HAILING_DUCKS_LD_ANNOTATION {
    tag "${runId}:${fine_mapping_locus_set_id}"

    label "collector"
    label "ld_annotation"

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'hailing_ducks_ld_annotation/**/stats.jsonl'

    input:
    tuple(
        runId: String,
        metas: List,
        fine_mapping_locus_set_path: Path,
        fine_mapping_locus_set_id: String,
        ht_paths: List,
        bm_paths: List,
        ancestries: List,
    )

    output:
    annotation = tuple(
        runId,
        metas,
        fine_mapping_locus_set_id,
        file("fine_mapping_locus_sets/*.parquet", optional: true),
        file("hailing_ducks_ld_annotation/*/multi_ancestry_pairwise_ld.parquet", optional: true),
        file("hailing_ducks_ld_annotation/*/stats.jsonl"),
    )
    stats = tuple(
        runId,
        fine_mapping_locus_set_id,
        file("hailing_ducks_ld_annotation/*/stats.jsonl"),
    )

    topic:
    tuple("${task.process}", "collector", "1.1.0") >> "versions"

    script:
    def args = task.ext.args ?: ''
    def locus_set_id = fine_mapping_locus_set_path.baseName
    def prefix = task.ext.prefix ? "${task.ext.prefix}_${locus_set_id}" : "${runId}_${locus_set_id}"
    def registry_args = (0..<ancestries.size()).collect { index ->
        "--ancestry '${ancestries[index]}' --ht_path '${ht_paths[index]}' --bm_path '${bm_paths[index]}'"
    }.join(' \\\n        ')
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
    mkdir -p fine_mapping_locus_sets
    mkdir -p hailing_ducks_ld_annotation/${prefix}
    cp ${fine_mapping_locus_set_path} fine_mapping_locus_sets/${fine_mapping_locus_set_path.getName()}
    printf '%s\\n' '${metadata_lines}' > metadata.jsonl

    collector hailing_ld \\
        --input ${fine_mapping_locus_set_path} \\
        --study_metadata metadata.jsonl \\
        --output hailing_ducks_ld_annotation/${prefix}/multi_ancestry_pairwise_ld.parquet \\
        --stats_output hailing_ducks_ld_annotation/${prefix}/stats.jsonl \\
        --max_cached_blocks ${params.hailing_ducks_max_cached_blocks} \\
        ${registry_args} \\
        ${args}

    check_result=\$(collector check_ld_pair_stats \\
        --run_id '${runId}' \\
        --fine_mapping_locus_set_id '${fine_mapping_locus_set_id}' \\
        --path hailing_ducks_ld_annotation/${prefix}/stats.jsonl)
    if [[ -n "\$check_result" ]]; then
        rm -rf fine_mapping_locus_sets
        rm -f hailing_ducks_ld_annotation/${prefix}/multi_ancestry_pairwise_ld.parquet
    fi
    """

    stub:
    def locus_set_id = fine_mapping_locus_set_path.baseName
    def prefix = task.ext.prefix ? "${task.ext.prefix}_${locus_set_id}" : "${runId}_${locus_set_id}"
    """
    mkdir -p fine_mapping_locus_sets
    mkdir -p hailing_ducks_ld_annotation/${prefix}
    touch fine_mapping_locus_sets/${fine_mapping_locus_set_path.getName()}
    touch hailing_ducks_ld_annotation/${prefix}/multi_ancestry_pairwise_ld.parquet
    touch hailing_ducks_ld_annotation/${prefix}/stats.jsonl
    """
}
