nextflow.enable.dsl = 2
nextflow.enable.types = true


process MULTISUSIE_FINE_MAPPING {
    tag "${runId}:${fine_mapping_locus_set_id}"

    label "multisusie"
    container params.multisusie_container ?: 'multisusie:latest'
    containerOptions '--entrypoint=""'
    maxForks 1

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
        study_locus_path: file("multisusie/study_locus.parquet", optional: true),
        extended_results_path: file("multisusie/fit.h5ad", optional: true),
        stats_path: file("multisusie/stats.json"),
    )

    topic:
    tuple("${task.process}", "multisusie", "1.0.0") >> "versions"

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
    mkdir -p multisusie

    printf '%s\\n' '${metadata_lines}' > metadata.jsonl

    multisusie \\
        --fine-mapping-locus-set ${fine_mapping_locus_set_path} \\
        --multi-ancestry-pairwise-ld ${multi_ancestry_pairwise_ld_path} \\
        --study-metadata metadata.jsonl \\
        --run-id ${runId} \\
        --fine-mapping-locus-set-id ${fine_mapping_locus_set_id} \\
        --study-locus-output multisusie/study_locus.parquet \\
        --extended-results-output multisusie/fit.h5ad \\
        --stats-output multisusie/stats.json \\
        ${args}
    """

    stub:
    """
    mkdir -p multisusie
    touch multisusie/study_locus.parquet
    touch multisusie/fit.h5ad
    printf '%s\n' '{"status":"SUCCESS"}' > multisusie/stats.json
    """
}
