nextflow.enable.dsl = 2
nextflow.enable.types = true


process STUDY_LOCUS_LD_ANNOTATION {
    tag "${runId}"

    label "collector"
    label "locus_annotation"

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'locus_annotation/**/*'

    input:
    tuple(runId: String, metas: List, collected_locus_path: Path, ld_index_path: Path, ld_pairs_input_path: Path)

    output:
    annotation = tuple(runId, file("locus_annotation/*/fine_mapping_loci.parquet"), file("locus_annotation/*/ld_pairs.parquet"))

    topic:
    tuple("${task.process}", "collector", "1.0.0") >> "versions"

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: runId
    def metadata_json = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(metas))
    """
    mkdir -p locus_annotation/${prefix}

    cat > metadata.json <<'EOF'
    ${metadata_json}
    EOF

    collector study_locus_ld_annotation \
        --input ${collected_locus_path} \
        --metadata_json metadata.json \
        --ld_index ${ld_index_path} \
        --ld_pairs_input ${ld_pairs_input_path} \
        --output_dir locus_annotation/${prefix} \
        ${args}
    """

    stub:
    def prefix = task.ext.prefix ?: runId
    """
    mkdir -p locus_annotation/${prefix}
    touch locus_annotation/${prefix}/fine_mapping_loci.parquet
    touch locus_annotation/${prefix}/ld_pairs.parquet
    """
}
