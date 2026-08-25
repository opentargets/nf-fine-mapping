nextflow.enable.dsl = 2
nextflow.enable.types = true


process SUSIEX_FINE_MAPPING {
    tag "${runId}:${fine_mapping_locus_set_id}"

    label "susiex"
    container params.susiex_container ?: 'ghcr.io/project-defiant/susiex/susiex:sha256-bf751e492d4bb3b7267f58a0583a1e00c3a9fc8dca3c1e565758d9e4754698cc'
    containerOptions '--entrypoint=""'
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'susiex/*', saveAs: { filename -> "susiex/${runId}/${fine_mapping_locus_set_id}/${filename.replace('susiex/', '')}" }

    input:
    tuple(
        runId: String,
        fine_mapping_locus_set_id: String,
        metas: List,
        fine_mapping_locus_set_path: Path,
        multi_ancestry_pairwise_ld_path: Path,
    )

    output:
    results = record(
        runId: runId,
        fine_mapping_locus_set_id: fine_mapping_locus_set_id,
        metas: metas,
        study_locus_path: file("susiex/study_locus.parquet", optional: true),
        extended_results_path: file("susiex/fit.h5ad", optional: true),
        stats_path: file("susiex/stats.json"),
    )

    topic:
    tuple("${task.process}", "susiex", "0.1.0") >> "versions"

    script:
    def args = task.ext.args ?: ''
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
    mkdir -p susiex

    printf '%s\\n' '${metadata_lines}' > metadata.jsonl

    susiex pipeline \\
        --fine-mapping-locus-set ${fine_mapping_locus_set_path} \\
        --multi-ancestry-pairwise-ld ${multi_ancestry_pairwise_ld_path} \\
        --study-metadata metadata.jsonl \\
        --run-id ${runId} \\
        --fine-mapping-locus-set-id ${fine_mapping_locus_set_id} \\
        --study-locus-output susiex/study_locus.parquet \\
        --extended-results-output susiex/fit.h5ad \\
        --stats-output susiex/stats.json \\
        ${args}
    """

    stub:
    """
    mkdir -p susiex
    touch susiex/study_locus.parquet
    touch susiex/fit.h5ad
    printf '%s\n' '{"status":"SUCCESS"}' > susiex/stats.json
    """
}
