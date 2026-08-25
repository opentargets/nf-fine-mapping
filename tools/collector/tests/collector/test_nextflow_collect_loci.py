"""Static contract tests for collect-loci Nextflow wiring."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
LOCUS_BREAKER_WORKFLOW = REPO_ROOT / "workflows" / "locus_breaker" / "main.nf"
LOCUS_COLLECTION_WORKFLOW = REPO_ROOT / "workflows" / "locus_collection" / "main.nf"
LOCUS_ANNOTATION_WORKFLOW = REPO_ROOT / "workflows" / "locus_annotation" / "main.nf"
COLLECTOR_LOCUS_BREAKER_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "locus_breaker" / "main.nf"
GENTROPY_LOCUS_BREAKER_MODULE = REPO_ROOT / "modules" / "local" / "gentropy" / "locus_breaker_clumping" / "main.nf"
COLLECT_CANONICAL_REGIONS_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "collect_canonical_regions" / "main.nf"
EMPTY_STATUS_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "empty_status" / "main.nf"
GENTROPY_LD_ANNOTATION_MODULE = REPO_ROOT / "modules" / "local" / "gentropy" / "fine_mapping_locus_set_ld_annotation" / "main.nf"
HAILING_DUCKS_LD_ANNOTATION_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "hailing_ld" / "main.nf"
MULTISUSIE_MODULE = REPO_ROOT / "modules" / "local" / "multisusie" / "fine_mapping" / "main.nf"
LD_PAIR_STATS_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "ld_pair_stats" / "main.nf"
MAIN_WORKFLOW = REPO_ROOT / "main.nf"
NEXTFLOW_CONFIG = REPO_ROOT / "nextflow.config"
NF_TEST_CONFIG = REPO_ROOT / "nf-test.config"
NF_TEST_PIPELINE = REPO_ROOT / "tests" / "default.nf.test"
NF_TEST_LOCUS_BREAKER_WORKFLOW = REPO_ROOT / "tests" / "workflows" / "locus_breaker.nf.test"
NF_TEST_LOCUS_COLLECTION_WORKFLOW = REPO_ROOT / "tests" / "workflows" / "locus_collection.nf.test"
NF_TEST_LOCUS_ANNOTATION_WORKFLOW = REPO_ROOT / "tests" / "workflows" / "locus_annotation.nf.test"
NF_TEST_NEXTFLOW_CONFIG = REPO_ROOT / "tests" / "nextflow.config"
FULL_COLLECTOR_HAILING_DUCKS_TEST_CONFIG = REPO_ROOT / "conf" / "test-full-collector-hailing-ducks.config"
FULL_MANIFEST = REPO_ROOT / "testdata" / "manifest.full.tsv"


def test_locus_breaker_workflow_does_not_collect_loci():
    workflow = LOCUS_BREAKER_WORKFLOW.read_text()
    collector_module = COLLECTOR_LOCUS_BREAKER_MODULE.read_text()
    gentropy_module = GENTROPY_LOCUS_BREAKER_MODULE.read_text()

    assert "workflow LOCUS_BREAKER" in workflow
    assert "include { COLLECTOR_LOCUS_BREAKER }" in workflow
    assert "include { GENTROPY_LOCUS_BREAKER_CLUMPING }" in workflow
    assert "locus_breaker_method = params.locus_breaker_method.toString().toLowerCase()" in workflow
    assert "locus_breaker_method == 'collector'" in workflow
    assert "locus_breaker_method == 'gentropy'" in workflow
    assert "ch_input = ch_sumstats.map" in workflow
    assert "tuple(" in workflow
    assert "COLLECTOR_LOCUS_BREAKER(ch_input)" in workflow
    assert "GENTROPY_LOCUS_BREAKER_CLUMPING(ch_input)" in workflow
    assert ".multiMap" not in workflow
    assert "process COLLECTOR_LOCUS_BREAKER" in collector_module
    assert "process GENTROPY_LOCUS_BREAKER_CLUMPING" in gentropy_module
    assert "tuple(meta: Map, summary_statistics_path: Path)" in collector_module
    assert "tuple(meta: Map, summary_statistics_path: Path)" in gentropy_module
    assert "gentropy step=locus_breaker_clumping" in gentropy_module
    assert "collector locus_breaker" in collector_module
    assert 'tuple("${task.process}", "collector", "1.1.0") >> "versions"' in collector_module
    assert "process COLLECTOR_LOCUS_BREAKER" not in workflow
    assert "process GENTROPY_LOCUS_BREAKER_CLUMPING" not in workflow
    assert "process collect_finemapping_loci" not in workflow
    assert "collector collect_finemapping_loci" not in workflow
    assert "ch_full_overlap_loci" not in workflow
    assert "ch_partial_overlap_loci" not in workflow
    assert "ch_non_overlap_loci" not in workflow
    assert "ch_collect_loci_stats" not in workflow
    assert "ch_locus2" not in workflow


def test_manifest_preserves_remote_summary_statistics_uris():
    workflow = MAIN_WORKFLOW.read_text()

    assert "summarystats_location.contains('://')" in workflow
    assert "summary_statistics_path = (summarystats_location.startsWith('/') || summarystats_location.contains('://'))" in workflow


def test_multisusie_process_uses_configured_purity_and_disables_low_memory():
    module = MULTISUSIE_MODULE.read_text()

    assert "--purity-min-r2 ${params.multisusie_purity_min_r2}" in module
    assert "--no-low-memory-mode" in module
    assert "do not override it in task.ext.args" in module
    assert "low-memory mode is disabled" in module


def test_locus_collection_workflow_wires_collect_canonical_regions_after_clumping():
    workflow = LOCUS_COLLECTION_WORKFLOW.read_text()
    module = COLLECT_CANONICAL_REGIONS_MODULE.read_text()
    validation_module = EMPTY_STATUS_MODULE.read_text()

    assert "workflow LOCUS_COLLECTION" in workflow
    assert "include { COLLECT_CANONICAL_REGIONS }" in workflow
    assert "include { COLLECTOR_EMPTY_STATUS }" in workflow
    assert "COLLECT_CANONICAL_REGIONS(ch_input)" in workflow
    assert "COLLECTOR_EMPTY_STATUS(ch_collection_status_input)" in workflow
    assert "process COLLECT_CANONICAL_REGIONS" in module
    assert "process COLLECTOR_EMPTY_STATUS" in validation_module
    assert ".groupTuple(by: 0)" in workflow
    assert "collector collect_canonical_regions" in module
    assert "collector empty_status" in validation_module
    assert 'tuple("${task.process}", "collector", "1.1.0") >> "versions"' in module
    assert 'tuple("${task.process}", "collector", "1.1.0") >> "versions"' in validation_module
    assert "--run_id '${runId}'" in module
    assert "--fine_mapping_locus_set_output_dir fine_mapping_locus_sets" in module
    assert "--stats_parquet_output stats.parquet" in module
    assert "--stats_json_output stats.json" in module
    assert "--run_id ${runId}" in validation_module
    assert "--path ${dataset_path}" in validation_module
    assert '"LOCUS_COLLECTION"' in workflow
    assert (
        "tuple(runId: String, metas: List, locus_breaker_paths: List<Path>, ancestries: List<String>, summary_statistics_paths: List<Path>)" in module
    )
    assert "tuple(runId: String, logical_path: String, validation_stage: String, dataset_path: Path)" in validation_module
    assert "loci = tuple(runId, metas, file(\"fine_mapping_locus_sets\", type: 'dir'))" in module
    assert 'stats = tuple(runId, metas, file("stats.parquet"), file("stats.json"))' in module
    assert 'file(("status/*.jsonl"), optional: true' in validation_module
    assert "ch_locus_collection_status = ch_collection_status" in workflow
    assert ".filter { status_path -> status_path != null }" in workflow


def test_locus_annotation_workflow_selects_gentropy_or_hailing_ducks_after_full_overlaps():
    workflow = LOCUS_ANNOTATION_WORKFLOW.read_text()
    module = GENTROPY_LD_ANNOTATION_MODULE.read_text()
    hailing_module = HAILING_DUCKS_LD_ANNOTATION_MODULE.read_text()
    status_module = LD_PAIR_STATS_MODULE.read_text()

    assert "workflow LOCUS_ANNOTATION" in workflow
    assert "include { GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION }" in workflow
    assert "include { HAILING_DUCKS_LD_ANNOTATION }" in workflow
    assert "GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION(ch_selected_ld_annotation_input)" in workflow
    assert "HAILING_DUCKS_LD_ANNOTATION(ch_selected_ld_annotation_input)" in workflow
    assert "params.ld_annotation_method" in workflow
    assert "params.ld_registry" in workflow
    assert "record(" in workflow
    assert "fine_mapping_locus_set_path" in workflow
    assert "multi_ancestry_pairwise_ld_path" in workflow
    assert "stats_path" in workflow
    assert "process GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION" in module
    assert 'label "ld_annotation"' in module
    assert "path(fine_mapping_locus_set_path), val(fine_mapping_locus_set_id), path(ld_variant_index_paths)" in module
    assert "step=fine_mapping_locus_set_ld_annotation" in module
    assert "step.fine_mapping_locus_set_input_path=\"'${fine_mapping_locus_set_path}'\"" in module
    assert "step.fine_mapping_study_metadata_jsonl_input_path=metadata.jsonl" in module
    assert "printf '%s\\\\n' '${metadata_lines}' > metadata.jsonl" in module
    assert "step.ld_registry='${registry_override}'" in module
    assert "vi_path: ld_variant_index_paths[index].getName()" in module
    assert "step.multi_ancestry_pairwise_ld_output_path=\"'gentropy_ld_annotation/${prefix}/multi_ancestry_pairwise_ld'\"" in module
    assert "step.stats_output_path=\"'gentropy_ld_annotation/${prefix}/stats.jsonl'\"" in module
    assert "step.session.start_hail=true" in module
    assert "step.session.spark_uri" in module
    assert "step.session.extended_spark_conf" in module
    assert "params.gentropy_ld_annotation_spark_conf" in module
    assert "bm_schemes.intersect(['s3', 's3a'])" in module
    assert "+step.session.s3_configuration.s3_host_url=s3.us-east-1.amazonaws.com" in module
    assert "step.session.output_partitions=1" in module
    assert 'path("gentropy_ld_annotation/*/multi_ancestry_pairwise_ld")' in module
    assert 'path("gentropy_ld_annotation/*/stats.jsonl")' in module
    assert "process HAILING_DUCKS_LD_ANNOTATION" in hailing_module
    assert "collector hailing_ld" in hailing_module
    assert "--study_metadata metadata.jsonl" in hailing_module
    assert "--max_cached_blocks ${params.hailing_ducks_max_cached_blocks}" in hailing_module
    assert "printf '%s\\\\n' '${metadata_lines}' > metadata.jsonl" in hailing_module
    assert "--ht_path" in hailing_module
    assert "--bm_path" in hailing_module
    assert "multi_ancestry_pairwise_ld.parquet" in hailing_module
    assert "process COLLECTOR_CHECK_LD_PAIR_STATS" in status_module
    assert "collector check_ld_pair_stats" in status_module
    assert "--fine_mapping_locus_set_id ${fine_mapping_locus_set_id}" in status_module


def test_main_workflow_publishes_collect_finemapping_loci_outputs():
    workflow = MAIN_WORKFLOW.read_text()

    assert "include { LOCUS_COLLECTION } from './workflows/locus_collection/main.nf'" in workflow
    assert "include { LOCUS_ANNOTATION } from './workflows/locus_annotation/main.nf'" in workflow
    assert "locus_collection_out = LOCUS_COLLECTION(locus_out)" in workflow
    assert "locus_annotation_out = LOCUS_ANNOTATION(full_overlap_loci)" in workflow
    assert "locus_breaker_out = LOCUS_BREAKER(supported_manifest_ch)" in workflow
    assert "locus_out2" not in workflow
    assert "loci2" not in workflow
    assert "full_overlap_loci = filter_rows_by_invalid_run_ids(" in workflow
    assert "partial_overlap_loci = filter_rows_by_invalid_run_ids(" in workflow
    assert "non_overlap_loci = filter_rows_by_invalid_run_ids(" in workflow
    assert "collect_loci_stats = filter_rows_by_invalid_run_ids(" in workflow
    assert "locus_collection_status = locus_collection_out.ch_locus_collection_status" in workflow
    assert "fine_mapping_locus_sets = locus_annotation.map" in workflow
    assert "multi_ancestry_pairwise_ld = locus_annotation.map" not in workflow
    assert "full_overlap_loci    = full_overlap_loci" in workflow
    assert "partial_overlap_loci = partial_overlap_loci" in workflow
    assert "non_overlap_loci     = non_overlap_loci" in workflow
    assert "collect_loci_stats   = collect_loci_stats" in workflow
    assert "locus_collection_status = locus_collection_status" in workflow
    assert "fine_mapping_locus_sets = fine_mapping_locus_sets" in workflow
    assert "locus_annotation     = locus_annotation" not in workflow
    assert "multi_ancestry_pairwise_ld = multi_ancestry_pairwise_ld" not in workflow
    assert "fine_mapping_locus_sets" in workflow
    assert "status/locus_collection" in workflow
    assert "fine_mapping_locus_sets {" in workflow
    assert "locus_annotation {" not in workflow
    assert "path 'locus_annotation/multi_ancestry_pairwise_ld'" not in workflow
    assert "validation/manifest" in workflow


def test_main_workflow_filters_invalid_runs_from_manifest_locus_and_collection_channels():
    workflow = MAIN_WORKFLOW.read_text()

    assert "def invalid_run_ids_from_status_channels(status_channels)" in workflow
    assert "def filter_rows_by_invalid_run_ids(rows, invalid_run_ids, extract_run_id)" in workflow
    assert "status_channels.findAll { status_channel -> status_channel != null }" in workflow
    assert "collect_invalid_run_ids = parsed_status_records" in workflow
    assert "manifest_invalid_run_ids = invalid_run_ids_from_status_channels([manifest_validation_status])" in workflow
    assert "supported_manifest_ch = filter_rows_by_invalid_run_ids(" in workflow
    assert "manifest_validation_out.supported_manifest_rows" in workflow
    assert "locus_invalid_run_ids = invalid_run_ids_from_status_channels([manifest_validation_status, locus_breaker_status])" in workflow
    assert "locus_out = filter_rows_by_invalid_run_ids(" in workflow
    assert "locus_breaker_out.ch_locus" in workflow
    assert "collection_invalid_run_ids = invalid_run_ids_from_status_channels([" in workflow
    assert "locus_collection_status" in workflow
    assert "published_locus_out = filter_rows_by_invalid_run_ids(" in workflow
    assert "loci                 = published_locus_out" in workflow
    assert "locus_collection_out.ch_full_overlap_loci" in workflow
    assert "locus_collection_out.ch_partial_overlap_loci" in workflow
    assert "locus_collection_out.ch_non_overlap_loci" in workflow
    assert "locus_collection_out.ch_collect_loci_stats" in workflow
    assert "invalid_run_ids_from_status_channels([manifest_validation_status])" in workflow
    assert "invalid_run_ids_from_status_channels([manifest_validation_status, locus_breaker_status])" in workflow


def test_full_data_profile_uses_a_single_explicit_collector_hailing_ducks_profile():
    nextflow_config = NEXTFLOW_CONFIG.read_text()
    full_config = FULL_COLLECTOR_HAILING_DUCKS_TEST_CONFIG.read_text()
    manifest = FULL_MANIFEST.read_text().splitlines()
    normalized_nextflow_config = " ".join(nextflow_config.split())
    normalized_full_config = " ".join(full_config.split())

    assert "testFullCollectorHailingDucks" in nextflow_config
    assert "includeConfig 'conf/test-full-collector-hailing-ducks.config'" in nextflow_config
    assert "fullTest" not in nextflow_config
    assert "testGentropyLocal" not in nextflow_config
    assert "includeConfig 'conf/full-test.config'" not in nextflow_config
    assert "includeConfig 'conf/test-gentropy-local.config'" not in nextflow_config
    assert 'locus_breaker_method = "collector"' in normalized_nextflow_config
    assert 'ld_annotation_method = "hailing_ducks"' in normalized_nextflow_config
    assert 'manifest = "${projectDir}/testdata/manifest.full.tsv"' in normalized_full_config
    assert 'output_dir = "${projectDir}/testdata/output_full"' in normalized_full_config
    assert 'workDir = "${projectDir}/testdata/work_full"' in full_config
    assert 'locus_breaker_method = "collector"' in normalized_full_config
    assert 'ld_annotation_method = "hailing_ducks"' in normalized_full_config
    assert "ghcr.io/opentargets/nf-fine-mapping/collector:sha256-" in full_config
    assert "vi_path" not in full_config
    assert "gentropy_spark_uri" not in full_config
    assert "gentropy_spark_conf" not in full_config
    assert "gentropy_ld_annotation_spark_conf" not in full_config
    assert "withLabel: gentropy" not in full_config
    assert "gentropy:3.4.0-dev.1-ld-pair-extraction-v2" not in full_config
    assert "memory = 8.GB" in normalized_full_config
    assert "maxForks = 2" in normalized_full_config
    assert "maxRetries = 2" in normalized_full_config
    assert "task.exitStatus == 137 ? 'retry' : 'terminate'" in normalized_full_config
    assert len(manifest) == 13
    assert all("\tdata/" in row for row in manifest[1:])
    assert all("\ttestdata/sumstats/" not in row for row in manifest[1:])


def test_full_integration_profile_keeps_remote_panukbb_block_matrices_without_local_variant_indexes():
    full_config = FULL_COLLECTOR_HAILING_DUCKS_TEST_CONFIG.read_text()
    normalized_full_config = " ".join(full_config.split())

    assert "ld_registry = [" in normalized_full_config
    assert "pan-ukb-us-east-1/ld_release/UKBB.EUR.ldadj.bm" in full_config
    assert "pan-ukb-us-east-1/ld_release/UKBB.CSA.ldadj.bm" in full_config
    assert "pan-ukb-us-east-1/ld_release/UKBB.AFR.ldadj.bm" in full_config
    assert "ldadj.variant.b38.ht" in full_config
    assert "ld_references" not in full_config
    assert "data/reference/panukbb/chr1/" not in full_config
    assert "data/reference/panukbb/full_test/" not in full_config
    assert "vi_path" not in full_config


def test_full_integration_profile_does_not_bind_a_fixed_spark_ui_host_port():
    full_config = FULL_COLLECTOR_HAILING_DUCKS_TEST_CONFIG.read_text()

    assert "--publish 4040:4040" not in full_config


def test_nf_test_pipeline_verifies_explicit_collector_hailing_and_gentropy_step_wiring():
    nf_test_config = NF_TEST_CONFIG.read_text()
    nf_test_pipeline = NF_TEST_PIPELINE.read_text()
    nf_test_nextflow_config = NF_TEST_NEXTFLOW_CONFIG.read_text()

    assert "conf/test-full-collector-hailing-ducks.config" in nf_test_config
    assert "conf/test-gentropy-local.config" not in nf_test_config
    assert "conf/full-test.config" not in nf_test_config
    assert 'profile "testFullCollectorHailingDucks"' in nf_test_config
    assert 'options "-stub-run"' in nf_test_pipeline
    assert "runs collector locus breaker, collection, and Hailing Ducks LD annotation on full test data" in nf_test_pipeline
    assert "runs gentropy locus breaker and Gentropy LD annotation on chr1 test data" in nf_test_pipeline
    assert 'output_dir = "$outputDir"' in nf_test_pipeline
    assert 'locus_breaker_method = "gentropy"' in nf_test_pipeline
    assert 'ld_annotation_method = "gentropy"' in nf_test_pipeline
    assert 'ld_annotation_method = "hailing_ducks"' in nf_test_pipeline
    assert 'manifest = new File("testdata/manifest.full.tsv").canonicalPath' in nf_test_pipeline
    assert 'manifest = new File("testdata/manifest.tsv").canonicalPath' in nf_test_pipeline
    assert 'manifest_base_dir = new File(".").canonicalPath' in nf_test_pipeline
    assert "workflow.trace.succeeded().size() == 57" in nf_test_pipeline
    assert 'task.startsWith("LOCUS_BREAKER:COLLECTOR_LOCUS_BREAKER") } == 12' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_BREAKER:GENTROPY_LOCUS_BREAKER_CLUMPING") } == 12' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_COLLECTION:COLLECT_CANONICAL_REGIONS") } == 4' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_COLLECTION:COLLECTOR_EMPTY_STATUS") } == 4' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_ANNOTATION:HAILING_DUCKS_LD_ANNOTATION") } == 8' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_ANNOTATION:HAILING_DUCKS_LD_ANNOTATION") } == 0' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_ANNOTATION:GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION") } == 8' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_ANNOTATION:GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION") } == 0' in nf_test_pipeline
    assert "docker.enabled = false" in nf_test_nextflow_config


def test_nf_test_workflows_verify_locus_breaker_and_collection_contracts():
    locus_breaker_test = NF_TEST_LOCUS_BREAKER_WORKFLOW.read_text()
    locus_collection_test = NF_TEST_LOCUS_COLLECTION_WORKFLOW.read_text()
    locus_annotation_test = NF_TEST_LOCUS_ANNOTATION_WORKFLOW.read_text()

    assert 'workflow "LOCUS_BREAKER"' in locus_breaker_test
    assert 'workflow "LOCUS_COLLECTION"' in locus_collection_test
    assert 'workflow "LOCUS_ANNOTATION"' in locus_annotation_test
    assert 'options "-stub-run"' in locus_breaker_test
    assert 'options "-stub-run"' in locus_collection_test
    assert 'options "-stub-run"' in locus_annotation_test
    assert 'locus_breaker_method = "collector"' in locus_breaker_test
    assert 'locus_breaker_method = "gentropy"' in locus_breaker_test
    assert "input[0] = channel.of(" in locus_breaker_test
    assert "input[0] = channel.of(" in locus_collection_test
    assert "input[0] = channel.of(" in locus_annotation_test
    assert "${projectDir}/testdata/sumstats/GCST90002351" in locus_breaker_test
    assert "${projectDir}/testdata/sumstats/GCST90002351/GCST90002351.parquet" in locus_collection_test
    assert 'task.startsWith("LOCUS_BREAKER:COLLECTOR_LOCUS_BREAKER") } == 2' in locus_breaker_test
    assert 'task.startsWith("LOCUS_BREAKER:GENTROPY_LOCUS_BREAKER_CLUMPING") } == 2' in locus_breaker_test
    assert 'task.startsWith("LOCUS_COLLECTION:COLLECT_CANONICAL_REGIONS") } == 1' in locus_collection_test
    assert 'task.startsWith("LOCUS_COLLECTION:COLLECTOR_EMPTY_STATUS") } == 1' in locus_collection_test
    assert 'task.startsWith("LOCUS_ANNOTATION:GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION") } == 2' in locus_annotation_test
    assert 'task.startsWith("LOCUS_ANNOTATION:HAILING_DUCKS_LD_ANNOTATION") } == 1' in locus_annotation_test
    assert "workflow.out.ch_locus.size() == 2" in locus_breaker_test
    assert 'topics "versions"' in locus_breaker_test
    assert 'row[0].startsWith("LOCUS_BREAKER:COLLECTOR_LOCUS_BREAKER")' in locus_breaker_test
    assert 'row[0].startsWith("LOCUS_BREAKER:GENTROPY_LOCUS_BREAKER_CLUMPING")' in locus_breaker_test
    assert 'row[1] == "collector"' in locus_breaker_test
    assert 'row[1] == "gentropy"' in locus_breaker_test
    assert 'row[2] == "3.4.0-dev.1-ld-pair-extraction-v2"' in locus_breaker_test
    assert 'topics "versions"' in locus_collection_test
    assert 'topics "versions"' in locus_annotation_test
    assert (
        "topics.versions.collect { row -> row[0] }.any { value -> "
        'value.startsWith("LOCUS_COLLECTION:COLLECT_CANONICAL_REGIONS") }' in locus_collection_test
    )
    assert (
        "topics.versions.collect { row -> row[0] }.any { value -> "
        'value.startsWith("LOCUS_COLLECTION:COLLECTOR_EMPTY_STATUS") }' in locus_collection_test
    )
    assert 'task.startsWith("LOCUS_ANNOTATION:GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION")' in locus_annotation_test
    assert 'task.startsWith("LOCUS_ANNOTATION:HAILING_DUCKS_LD_ANNOTATION")' in locus_annotation_test
    assert "workflow.out.ch_full_overlap_loci.size() == 2" in locus_collection_test
    assert "workflow.out.ch_partial_overlap_loci.size() == 0" in locus_collection_test
    assert "workflow.out.ch_non_overlap_loci.size() == 0" in locus_collection_test
    assert "workflow.out.ch_collect_loci_stats.size() == 1" in locus_collection_test
    assert "workflow.out.ch_locus_collection_status.size() == 1" in locus_collection_test
    assert (
        "workflow.out.ch_full_overlap_loci.collect { r -> r.metas.collect { meta -> meta.studyId } }.flatten() "
        '== ["STUDY_A", "STUDY_B", "STUDY_C", "STUDY_A", "STUDY_B", "STUDY_C"]' in locus_collection_test
    )
    assert "workflow.out.ch_locus_annotation.size() == 2" in locus_annotation_test
    assert 'workflow.out.ch_locus_annotation[0].runId == "RUN_A"' in locus_annotation_test


def test_locus_breaker_clumping_passes_profile_spark_settings_to_gentropy():
    workflow = GENTROPY_LOCUS_BREAKER_MODULE.read_text()

    assert "def gentropy_spark_uri = params.gentropy_spark_uri ?: 'local[*]'" in workflow
    assert "def gentropy_spark_conf = params.gentropy_spark_conf ?: '{}'" in workflow
    assert "'step.session.spark_uri=\"${gentropy_spark_uri}\"'" in workflow
    assert "'+step.session.extended_spark_conf=${gentropy_spark_conf}'" in workflow


def test_local_modules_support_process_ext_args():
    collector_locus_breaker = COLLECTOR_LOCUS_BREAKER_MODULE.read_text()
    gentropy_locus_breaker = GENTROPY_LOCUS_BREAKER_MODULE.read_text()
    collect_canonical_regions = COLLECT_CANONICAL_REGIONS_MODULE.read_text()
    gentropy_ld_annotation = GENTROPY_LD_ANNOTATION_MODULE.read_text()

    assert "def args = task.ext.args ?: ''" in collector_locus_breaker
    assert "def args = task.ext.args ?: ''" in gentropy_locus_breaker
    assert "def args = task.ext.args ?: ''" in collect_canonical_regions
    assert "def args = task.ext.args ?: ''" in gentropy_ld_annotation
    assert "        ${args}" in collector_locus_breaker
    assert "        ${args}" in gentropy_locus_breaker
    assert "        ${args}" in collect_canonical_regions
    assert "        ${args}" in gentropy_ld_annotation


def test_local_modules_support_process_ext_prefix():
    collector_locus_breaker = COLLECTOR_LOCUS_BREAKER_MODULE.read_text()
    gentropy_locus_breaker = GENTROPY_LOCUS_BREAKER_MODULE.read_text()
    collect_canonical_regions = COLLECT_CANONICAL_REGIONS_MODULE.read_text()
    gentropy_ld_annotation = GENTROPY_LD_ANNOTATION_MODULE.read_text()

    assert "def prefix = task.ext.prefix ?: meta.studyId" in collector_locus_breaker
    assert "def prefix = task.ext.prefix ?: meta.studyId" in gentropy_locus_breaker
    assert "--run_id '${runId}'" in collect_canonical_regions
    assert 'def prefix = task.ext.prefix ? "${task.ext.prefix}_${locus_set_id}" : "${runId}_${locus_set_id}"' in gentropy_ld_annotation
    assert "${prefix}.parquet" in collector_locus_breaker
    assert "${prefix}" in gentropy_locus_breaker
    assert "stats.parquet" in collect_canonical_regions
    assert "stats.json" in collect_canonical_regions
    assert "gentropy_ld_annotation/${prefix}" in gentropy_ld_annotation
