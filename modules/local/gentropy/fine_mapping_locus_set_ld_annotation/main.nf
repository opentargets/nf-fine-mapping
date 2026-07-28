nextflow.enable.dsl = 2


process GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION {
    tag "${runId}:${fine_mapping_locus_set_path.baseName}"

    label "gentropy"
    label "ld_annotation"
    maxForks 1

    publishDir "${params.output_dir}", mode: 'copy', pattern: 'gentropy_ld_annotation/**/*'

    input:
    tuple val(runId), val(metas), path(fine_mapping_locus_set_path), val(fine_mapping_locus_set_id), path(ld_variant_index_paths), val(ld_block_matrix_paths), val(ancestries)

    output:
    tuple val(runId), val(metas), val(fine_mapping_locus_set_id), path("fine_mapping_locus_sets/*.parquet"), path("gentropy_ld_annotation/*/multi_ancestry_pairwise_ld"), path("gentropy_ld_annotation/*/stats.jsonl"), emit: annotation
    tuple val(runId), val(fine_mapping_locus_set_id), path("gentropy_ld_annotation/*/stats.jsonl"), emit: stats

    script:
    def args = task.ext.args ?: ''
    def locus_set_id = fine_mapping_locus_set_path.baseName
    def prefix = task.ext.prefix ? "${task.ext.prefix}_${locus_set_id}" : "${runId}_${locus_set_id}"
    def gentropy_spark_uri = params.gentropy_spark_uri ?: 'local[*]'
    def gentropy_spark_conf = params.gentropy_ld_annotation_spark_conf ?: params.gentropy_spark_conf ?: '{}'
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
    def registry = (0..<ancestries.size()).collect { index ->
        [
            ancestry: ancestries[index],
            vi_path: ld_variant_index_paths[index].getName(),
            bm_path: ld_block_matrix_paths[index],
        ]
    }
    def registry_override = "[${registry.collect { entry ->
        "{ancestry:${entry.ancestry},vi_path:${entry.vi_path},bm_path:${entry.bm_path}}"
    }.join(',')}]"
    def bm_schemes = ld_block_matrix_paths.collect { path ->
        path.toString().split(':', 2)[0].toLowerCase()
    } as Set
    def connector_args = []
    if (bm_schemes.intersect(['s3', 's3a'])) {
        connector_args += [
            'step.session.add_s3_connector=true',
            '+step.session.s3_configuration.anonymous=true',
            '+step.session.s3_configuration.s3_host_url=s3.us-east-1.amazonaws.com',
        ]
    }
    if (bm_schemes.contains('gs') || bm_schemes.contains('gcs')) {
        connector_args << 'step.session.add_gcs_connector=true'
    }
    """
    mkdir -p gentropy_ld_annotation/${prefix}
    mkdir -p fine_mapping_locus_sets
    cp ${fine_mapping_locus_set_path} fine_mapping_locus_sets/${fine_mapping_locus_set_path.getName()}

    printf '%s\\n' '${metadata_lines}' > metadata.jsonl

    gentropy step=fine_mapping_locus_set_ld_annotation \\
        'step.session.spark_uri="${gentropy_spark_uri}"' \\
        '+step.session.extended_spark_conf=${gentropy_spark_conf}' \\
        step.session.write_mode=overwrite \\
        step.session.log_level=INFO \\
        step.session.start_hail=true \\
        step.session.output_partitions=1 \\
        step.fine_mapping_locus_set_input_path="'${fine_mapping_locus_set_path}'" \\
        step.fine_mapping_study_metadata_jsonl_input_path=metadata.jsonl \\
        step.multi_ancestry_pairwise_ld_output_path="'gentropy_ld_annotation/${prefix}/multi_ancestry_pairwise_ld'" \\
        step.stats_output_path="'gentropy_ld_annotation/${prefix}/stats.jsonl'" \\
        step.ld_registry='${registry_override}' \\
        ${connector_args.join(' \\\n        ')} \\
        ${args}
    """

    stub:
    def locus_set_id = fine_mapping_locus_set_path.baseName
    def prefix = task.ext.prefix ? "${task.ext.prefix}_${locus_set_id}" : "${runId}_${locus_set_id}"
    """
    mkdir -p gentropy_ld_annotation/${prefix}
    mkdir -p fine_mapping_locus_sets
    touch fine_mapping_locus_sets/${fine_mapping_locus_set_path.getName()}
    touch gentropy_ld_annotation/${prefix}/multi_ancestry_pairwise_ld
    touch gentropy_ld_annotation/${prefix}/stats.jsonl
    """
}
