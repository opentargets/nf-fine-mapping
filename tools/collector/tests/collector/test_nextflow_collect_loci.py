"""Static contract tests for collect-loci Nextflow wiring."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
LOCUS_BREAKER_WORKFLOW = REPO_ROOT / "workflows" / "locus_breaker" / "main.nf"
LOCUS_COLLECTION_WORKFLOW = REPO_ROOT / "workflows" / "locus_collection" / "main.nf"
LOCUS_ANNOTATION_WORKFLOW = REPO_ROOT / "workflows" / "locus_annotation" / "main.nf"
COLLECTOR_LOCUS_BREAKER_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "locus_breaker" / "main.nf"
GENTROPY_LOCUS_BREAKER_MODULE = REPO_ROOT / "modules" / "local" / "gentropy" / "locus_breaker_clumping" / "main.nf"
COLLECT_FINEMAPPING_LOCI_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "collect_finemapping_loci" / "main.nf"
EMPTY_STATUS_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "empty_status" / "main.nf"
GENTROPY_LD_ANNOTATION_MODULE = REPO_ROOT / "modules" / "local" / "gentropy" / "fine_mapping_locus_set_ld_annotation" / "main.nf"
HAILING_DUCKS_LD_ANNOTATION_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "hailing_ld" / "main.nf"
LD_PAIR_STATS_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "ld_pair_stats" / "main.nf"
MAIN_WORKFLOW = REPO_ROOT / "main.nf"
NEXTFLOW_CONFIG = REPO_ROOT / "nextflow.config"
NF_TEST_CONFIG = REPO_ROOT / "nf-test.config"
NF_TEST_PIPELINE = REPO_ROOT / "tests" / "default.nf.test"
NF_TEST_LOCUS_BREAKER_WORKFLOW = REPO_ROOT / "tests" / "workflows" / "locus_breaker.nf.test"
NF_TEST_LOCUS_COLLECTION_WORKFLOW = REPO_ROOT / "tests" / "workflows" / "locus_collection.nf.test"
NF_TEST_LOCUS_ANNOTATION_WORKFLOW = REPO_ROOT / "tests" / "workflows" / "locus_annotation.nf.test"
NF_TEST_NEXTFLOW_CONFIG = REPO_ROOT / "tests" / "nextflow.config"
FULL_TEST_CONFIG = REPO_ROOT / "conf" / "full-test.config"
TEST_GENTROPY_LOCAL_CONFIG = REPO_ROOT / "conf" / "test-gentropy-local.config"
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
    assert 'tuple("${task.process}", "collector", "1.0.0") >> "versions"' in collector_module
    assert "process COLLECTOR_LOCUS_BREAKER" not in workflow
    assert "process GENTROPY_LOCUS_BREAKER_CLUMPING" not in workflow
    assert "process collect_finemapping_loci" not in workflow
    assert "collector collect_finemapping_loci" not in workflow
    assert "ch_full_overlap_loci" not in workflow
    assert "ch_partial_overlap_loci" not in workflow
    assert "ch_non_overlap_loci" not in workflow
    assert "ch_collect_loci_stats" not in workflow
    assert "ch_locus2" not in workflow


def test_locus_collection_workflow_wires_collect_finemapping_loci_after_clumping():
    workflow = LOCUS_COLLECTION_WORKFLOW.read_text()
    module = COLLECT_FINEMAPPING_LOCI_MODULE.read_text()
    validation_module = EMPTY_STATUS_MODULE.read_text()

    assert "workflow LOCUS_COLLECTION" in workflow
    assert "include { COLLECT_FINEMAPPING_LOCI }" in workflow
    assert "include { COLLECTOR_EMPTY_STATUS }" in workflow
    assert "COLLECT_FINEMAPPING_LOCI(ch_collect_finemapping_loci_input)" in workflow
    assert "COLLECTOR_EMPTY_STATUS(ch_collection_status_input)" in workflow
    assert "process COLLECT_FINEMAPPING_LOCI" in module
    assert "process COLLECTOR_EMPTY_STATUS" in validation_module
    assert ".groupTuple(by: 0)" in workflow
    assert "collector collect_finemapping_loci" in module
    assert "collector empty_status" in validation_module
    assert 'tuple("${task.process}", "collector", "1.0.0") >> "versions"' in module
    assert 'tuple("${task.process}", "collector", "1.0.0") >> "versions"' in validation_module
    assert "--full_output collected_loci/full_overlaps/${prefix}.parquet" in module
    assert "--partial_output collected_loci/partial_overlaps/${prefix}.parquet" in module
    assert "--non_overlap_output collected_loci/non_overlaps/${prefix}.parquet" in module
    assert "--stats_output collected_loci/stats/${prefix}.json" in module
    assert "--run_id ${runId}" in validation_module
    assert "--path ${dataset_path}" in validation_module
    assert '"LOCUS_COLLECTION"' in workflow
    assert "tuple(runId: String, metas: List, study_locus_paths: List<Path>)" in module
    assert "tuple(runId: String, logical_path: String, validation_stage: String, dataset_path: Path)" in validation_module
    assert 'full_overlap = tuple(runId, metas, file("collected_loci/full_overlaps/*.parquet"))' in module
    assert 'partial_overlap = tuple(runId, metas, file("collected_loci/partial_overlaps/*.parquet"))' in module
    assert 'non_overlap = tuple(runId, metas, file("collected_loci/non_overlaps/*.parquet"))' in module
    assert 'stats = tuple(runId, metas, file("collected_loci/stats/*.json"))' in module
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
    assert "multi_ancestry_pairwise_ld = locus_annotation.map" in workflow
    assert "full_overlap_loci    = full_overlap_loci" in workflow
    assert "partial_overlap_loci = partial_overlap_loci" in workflow
    assert "non_overlap_loci     = non_overlap_loci" in workflow
    assert "collect_loci_stats   = collect_loci_stats" in workflow
    assert "locus_collection_status = locus_collection_status" in workflow
    assert "fine_mapping_locus_sets = fine_mapping_locus_sets" in workflow
    assert "multi_ancestry_pairwise_ld = multi_ancestry_pairwise_ld" in workflow
    assert "collected_loci/full_overlaps" in workflow
    assert "collected_loci/partial_overlaps" in workflow
    assert "collected_loci/non_overlaps" in workflow
    assert "collected_loci/stats" in workflow
    assert "status/locus_collection" in workflow
    assert "locus_annotation/fine_mapping_locus_sets" in workflow
    assert "locus_annotation/multi_ancestry_pairwise_ld" in workflow
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


def test_full_data_profile_uses_separate_manifest_work_and_output_locations():
    nextflow_config = NEXTFLOW_CONFIG.read_text()
    full_config = FULL_TEST_CONFIG.read_text()
    manifest = FULL_MANIFEST.read_text().splitlines()

    assert "fullTest" in nextflow_config
    assert "includeConfig 'conf/full-test.config'" in nextflow_config
    assert 'locus_breaker_method = "collector"' in nextflow_config
    assert 'manifest   = "${projectDir}/testdata/manifest.full.tsv"' in full_config
    assert 'output_dir = "${projectDir}/testdata/output_full"' in full_config
    assert 'workDir = "${projectDir}/testdata/work_full"' in full_config
    assert 'gentropy_spark_uri = "local[2]"' in full_config
    assert "spark.driver.memory:24g" in full_config
    assert "spark.executor.memory:24g" in full_config
    assert len(manifest) == 13
    assert all("\tdata/" in row for row in manifest[1:])
    assert all("\ttestdata/sumstats/" not in row for row in manifest[1:])


def test_chr1_local_profile_uses_hailing_ducks_ld_annotation():
    nextflow_config = NEXTFLOW_CONFIG.read_text()
    local_config = TEST_GENTROPY_LOCAL_CONFIG.read_text()

    assert "testGentropyLocal" in nextflow_config
    assert "includeConfig 'conf/test-gentropy-local.config'" in nextflow_config
    assert 'executor.name = "local"' in local_config
    assert "manifest" in local_config
    assert '"${projectDir}/testdata/manifest.tsv"' in local_config
    assert '"${projectDir}/testdata/output_gentropy"' in local_config
    assert 'workDir = "${projectDir}/testdata/work_gentropy"' in local_config
    assert 'locus_breaker_method              = "collector"' in local_config
    assert 'ld_annotation_method              = "hailing_ducks"' in local_config
    assert "ghcr.io/project-defiant/hailing-ducks:v1.1.0" in local_config
    assert "container  = 'collector:hailing-ducks-dev'" in local_config
    assert 'ht_path: "s3://pan-ukb-us-east-1/ld_release/UKBB.EUR.ldadj.variant.b38.ht"' in local_config
    assert 'bm_path: "s3://pan-ukb-us-east-1/ld_release/UKBB.EUR.ldadj.bm"' in local_config


def test_real_profiles_use_local_variant_indexes_and_remote_panukbb_block_matrices():
    chr1_config = (REPO_ROOT / "conf" / "test.config").read_text()
    full_config = (REPO_ROOT / "conf" / "full-test.config").read_text()

    for config in (chr1_config, full_config):
        assert "ld_registry = [" in config
        assert "pan-ukb-us-east-1/ld_release/UKBB.EUR.ldadj.bm" in config
        assert "pan-ukb-us-east-1/ld_release/UKBB.CSA.ldadj.bm" in config
        assert "pan-ukb-us-east-1/ld_release/UKBB.AFR.ldadj.bm" in config
        assert "ldadj.variant.b38.ht" in config
        assert "ld_references" not in config

    assert "data/reference/panukbb/chr1/UKBB.CSA.aligned.parquet" in chr1_config
    assert "data/reference/panukbb/full_test/UKBB.CSA.aligned.parquet" in full_config


def test_nf_test_pipeline_verifies_collector_and_gentropy_step_wiring():
    nf_test_config = NF_TEST_CONFIG.read_text()
    nf_test_pipeline = NF_TEST_PIPELINE.read_text()
    nf_test_nextflow_config = NF_TEST_NEXTFLOW_CONFIG.read_text()

    assert "conf/test-gentropy-local.config" in nf_test_config
    assert 'options "-stub-run"' in nf_test_pipeline
    assert "runs collector locus breaker and locus collection on chr1 test data" in nf_test_pipeline
    assert "runs gentropy locus breaker and locus collection on chr1 test data" in nf_test_pipeline
    assert 'output_dir = "$outputDir"' in nf_test_pipeline
    assert 'locus_breaker_method = "gentropy"' in nf_test_pipeline
    assert 'manifest = new File("testdata/manifest.tsv").canonicalPath' in nf_test_pipeline
    assert 'manifest_base_dir = new File(".").canonicalPath' in nf_test_pipeline
    assert "workflow.trace.succeeded().size() == 60" in nf_test_pipeline
    assert 'task.startsWith("LOCUS_BREAKER:COLLECTOR_LOCUS_BREAKER") } == 12' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_BREAKER:GENTROPY_LOCUS_BREAKER_CLUMPING") } == 12' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_COLLECTION:COLLECT_FINEMAPPING_LOCI") } == 4' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_COLLECTION:COLLECTOR_EMPTY_STATUS") } == 4' in nf_test_pipeline
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
    assert 'task.startsWith("LOCUS_COLLECTION:COLLECT_FINEMAPPING_LOCI") } == 1' in locus_collection_test
    assert 'task.startsWith("LOCUS_COLLECTION:COLLECTOR_EMPTY_STATUS") } == 1' in locus_collection_test
    assert 'task.startsWith("LOCUS_ANNOTATION:GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION") } == 2' in locus_annotation_test
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
        'value.startsWith("LOCUS_COLLECTION:COLLECT_FINEMAPPING_LOCI") }' in locus_collection_test
    )
    assert (
        "topics.versions.collect { row -> row[0] }.any { value -> "
        'value.startsWith("LOCUS_COLLECTION:COLLECTOR_EMPTY_STATUS") }' in locus_collection_test
    )
    assert 'task.startsWith("LOCUS_ANNOTATION:GENTROPY_FINE_MAPPING_LOCUS_SET_LD_ANNOTATION")' in locus_annotation_test
    assert "workflow.out.ch_full_overlap_loci.size() == 2" in locus_collection_test
    assert "workflow.out.ch_partial_overlap_loci.size() == 1" in locus_collection_test
    assert "workflow.out.ch_non_overlap_loci.size() == 1" in locus_collection_test
    assert "workflow.out.ch_collect_loci_stats.size() == 1" in locus_collection_test
    assert "workflow.out.ch_locus_collection_status.size() == 1" in locus_collection_test
    assert 'meta.studyId } == ["STUDY_A", "STUDY_B", "STUDY_C"]' in locus_collection_test
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
    collect_finemapping_loci = COLLECT_FINEMAPPING_LOCI_MODULE.read_text()
    gentropy_ld_annotation = GENTROPY_LD_ANNOTATION_MODULE.read_text()

    assert "def args = task.ext.args ?: ''" in collector_locus_breaker
    assert "def args = task.ext.args ?: ''" in gentropy_locus_breaker
    assert "def args = task.ext.args ?: ''" in collect_finemapping_loci
    assert "def args = task.ext.args ?: ''" in gentropy_ld_annotation
    assert "        ${args}" in collector_locus_breaker
    assert "        ${args}" in gentropy_locus_breaker
    assert "        ${args}" in collect_finemapping_loci
    assert "        ${args}" in gentropy_ld_annotation


def test_local_modules_support_process_ext_prefix():
    collector_locus_breaker = COLLECTOR_LOCUS_BREAKER_MODULE.read_text()
    gentropy_locus_breaker = GENTROPY_LOCUS_BREAKER_MODULE.read_text()
    collect_finemapping_loci = COLLECT_FINEMAPPING_LOCI_MODULE.read_text()
    gentropy_ld_annotation = GENTROPY_LD_ANNOTATION_MODULE.read_text()

    assert "def prefix = task.ext.prefix ?: meta.studyId" in collector_locus_breaker
    assert "def prefix = task.ext.prefix ?: meta.studyId" in gentropy_locus_breaker
    assert "def prefix = task.ext.prefix ?: runId" in collect_finemapping_loci
    assert 'def prefix = task.ext.prefix ? "${task.ext.prefix}_${locus_set_id}" : "${runId}_${locus_set_id}"' in gentropy_ld_annotation
    assert "${prefix}.parquet" in collector_locus_breaker
    assert "${prefix}" in gentropy_locus_breaker
    assert "${prefix}.parquet" in collect_finemapping_loci
    assert "${prefix}.json" in collect_finemapping_loci
    assert "gentropy_ld_annotation/${prefix}" in gentropy_ld_annotation
