nextflow.enable.dsl = 2
nextflow.enable.types = true


process SUSHIE_FINE_MAPPING {
    tag "${runId}:${fine_mapping_locus_set_id}"

    label "sushie"
    container params.sushie_container ?: 'ghcr.io/project-defiant/sushie/sushie:sha256-adedefcfe352f0ea80345654c6969e78a351ac333b10cd054721e32dd9b182d6'
    containerOptions '--entrypoint=""'
    publishDir "${params.output_dir}", mode: 'copy', pattern: 'sushie/*', saveAs: { filename -> "sushie/${runId}/${fine_mapping_locus_set_id}/${filename.replace('sushie/', '')}" }

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
        study_locus_path: file("sushie/study_locus.parquet", optional: true),
        extended_results_path: file("sushie/fit.h5ad", optional: true),
        stats_path: file("sushie/stats.json"),
    )

    topic:
    tuple("${task.process}", "sushie", "0.1.0") >> "versions"

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
    mkdir -p sushie

    printf '%s\\n' '${metadata_lines}' > metadata.jsonl

    sushie \\
        --fine-mapping-locus-set ${fine_mapping_locus_set_path} \\
        --multi-ancestry-pairwise-ld ${multi_ancestry_pairwise_ld_path} \\
        --study-metadata metadata.jsonl \\
        --run-id ${runId} \\
        --fine-mapping-locus-set-id ${fine_mapping_locus_set_id} \\
        --study-locus-output sushie/study_locus.parquet \\
        --extended-results-output sushie/fit.h5ad \\
        --stats-output sushie/stats.json \\
        ${args}
    """

    stub:
    """
    mkdir -p sushie
    touch sushie/study_locus.parquet
    touch sushie/fit.h5ad
    printf '%s\n' '{"status":"SUCCESS"}' > sushie/stats.json
    """
}
