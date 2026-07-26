"""Static contract tests for collect-loci Nextflow wiring."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
LOCUS_BREAKER_WORKFLOW = REPO_ROOT / "workflows" / "locus_breaker" / "main.nf"
LOCUS_COLLECTION_WORKFLOW = REPO_ROOT / "workflows" / "locus_collection" / "main.nf"
LOCUS_ANNOTATION_WORKFLOW = REPO_ROOT / "workflows" / "locus_annotation" / "main.nf"
COLLECTOR_LOCUS_BREAKER_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "locus_breaker" / "main.nf"
GENTROPY_LOCUS_BREAKER_MODULE = REPO_ROOT / "modules" / "local" / "gentropy" / "locus_breaker_clumping" / "main.nf"
COLLECT_FINEMAPPING_LOCI_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "collect_finemapping_loci" / "main.nf"
STUDY_LOCUS_LD_ANNOTATION_MODULE = REPO_ROOT / "modules" / "local" / "collector" / "study_locus_ld_annotation" / "main.nf"
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
    assert 'tuple("${task.process}", "gentropy", "3.3.0-dev.64") >> "versions"' in gentropy_module
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

    assert "workflow LOCUS_COLLECTION" in workflow
    assert "include { COLLECT_FINEMAPPING_LOCI }" in workflow
    assert "COLLECT_FINEMAPPING_LOCI(ch_collect_finemapping_loci_input)" in workflow
    assert "process COLLECT_FINEMAPPING_LOCI" in module
    assert ".groupTuple(by: 0)" in workflow
    assert "collector collect_finemapping_loci" in module
    assert 'tuple("${task.process}", "collector", "1.0.0") >> "versions"' in module
    assert "--full_output collected_loci/full_overlaps/${prefix}.parquet" in module
    assert "--partial_output collected_loci/partial_overlaps/${prefix}.parquet" in module
    assert "--non_overlap_output collected_loci/non_overlaps/${prefix}.parquet" in module
    assert "--stats_output collected_loci/stats/${prefix}.json" in module
    assert "tuple(runId: String, metas: List, study_locus_paths: List<Path>)" in module
    assert 'full_overlap = tuple(runId, metas, file("collected_loci/full_overlaps/*.parquet"))' in module
    assert 'partial_overlap = tuple(runId, metas, file("collected_loci/partial_overlaps/*.parquet"))' in module
    assert 'non_overlap = tuple(runId, metas, file("collected_loci/non_overlaps/*.parquet"))' in module
    assert 'stats = tuple(runId, metas, file("collected_loci/stats/*.json"))' in module
    assert ".filter { _runId, _metas, collected_locus_path -> collected_locus_path != null }" not in workflow


def test_locus_annotation_workflow_wires_study_locus_ld_annotation_after_full_overlaps():
    workflow = LOCUS_ANNOTATION_WORKFLOW.read_text()
    module = STUDY_LOCUS_LD_ANNOTATION_MODULE.read_text()

    assert "workflow LOCUS_ANNOTATION" in workflow
    assert "include { STUDY_LOCUS_LD_ANNOTATION }" in workflow
    assert "STUDY_LOCUS_LD_ANNOTATION(ch_study_locus_ld_annotation_input)" in workflow
    assert "params.ld_index" in workflow
    assert "params.ld_pairs_input" in workflow
    assert "record(" in workflow
    assert "fine_mapping_loci_path" in workflow
    assert "ld_pairs_path" in workflow
    assert "process STUDY_LOCUS_LD_ANNOTATION" in module
    assert "tuple(runId: String, metas: List, collected_locus_path: Path, ld_index_path: Path, ld_pairs_input_path: Path)" in module
    assert "collector study_locus_ld_annotation" in module
    assert "--input ${collected_locus_path}" in module
    assert "--metadata_json metadata.json" in module
    assert "--ld_index ${ld_index_path}" in module
    assert "--ld_pairs_input ${ld_pairs_input_path}" in module
    assert "--output_dir locus_annotation/${prefix}" in module
    assert 'annotation = tuple(runId, file("locus_annotation/*/fine_mapping_loci.parquet"), file("locus_annotation/*/ld_pairs.parquet"))' in module
    assert 'tuple("${task.process}", "collector", "1.0.0") >> "versions"' in module


def test_main_workflow_publishes_collect_finemapping_loci_outputs():
    workflow = MAIN_WORKFLOW.read_text()

    assert "include { LOCUS_COLLECTION } from './workflows/locus_collection/main.nf'" in workflow
    assert "include { LOCUS_ANNOTATION } from './workflows/locus_annotation/main.nf'" in workflow
    assert "locus_collection_out = LOCUS_COLLECTION(locus_out)" in workflow
    assert "locus_annotation_out = LOCUS_ANNOTATION(full_overlap_loci)" in workflow
    assert "manifest_validation_out = MANIFEST_VALIDATION(filtered_ch)" in workflow
    assert "locus_breaker_out = LOCUS_BREAKER(supported_manifest_ch)" in workflow
    assert "locus_out = locus_breaker_out.ch_locus" in workflow
    assert "locus_breaker_status = locus_breaker_out.ch_status" in workflow
    assert "locus_out2" not in workflow
    assert "loci2" not in workflow
    assert "full_overlap_loci = locus_collection_out.ch_full_overlap_loci" in workflow
    assert "partial_overlap_loci = locus_collection_out.ch_partial_overlap_loci" in workflow
    assert "non_overlap_loci = locus_collection_out.ch_non_overlap_loci" in workflow
    assert "collect_loci_stats = locus_collection_out.ch_collect_loci_stats" in workflow
    assert "fine_mapping_loci = locus_annotation_out.ch_fine_mapping_loci" in workflow
    assert "ld_pairs = locus_annotation_out.ch_ld_pairs" in workflow
    assert "full_overlap_loci    = full_overlap_loci" in workflow
    assert "partial_overlap_loci = partial_overlap_loci" in workflow
    assert "non_overlap_loci     = non_overlap_loci" in workflow
    assert "collect_loci_stats   = collect_loci_stats" in workflow
    assert "fine_mapping_loci    = fine_mapping_loci" in workflow
    assert "ld_pairs             = ld_pairs" in workflow
    assert "manifest_validation_status = manifest_validation_status" in workflow
    assert "collected_loci/full_overlaps" in workflow
    assert "collected_loci/partial_overlaps" in workflow
    assert "collected_loci/non_overlaps" in workflow
    assert "collected_loci/stats" in workflow
    assert "locus_annotation/fine_mapping_loci" in workflow
    assert "locus_annotation/ld_pairs" in workflow
    assert "validation/manifest" in workflow


def test_main_workflow_validates_manifest_ancestry_against_exact_ld_references():
    workflow = MAIN_WORKFLOW.read_text()
    nextflow_config = NEXTFLOW_CONFIG.read_text()
    test_config = (REPO_ROOT / "conf" / "test.config").read_text()
    gentropy_config = TEST_GENTROPY_LOCAL_CONFIG.read_text()
    full_config = FULL_TEST_CONFIG.read_text()
    nf_test_pipeline = NF_TEST_PIPELINE.read_text()

    assert "params.ld_references" in workflow
    assert "UNREGISTERED_ANCESTRY" in workflow
    assert "validationStage: 'MANIFEST'" in workflow
    assert "MANIFEST_VALIDATION_REPORT" in workflow
    assert "Duplicate ld_references ancestry labels" in workflow
    assert "ld_references = []" in nextflow_config
    assert "ld_references = [" in test_config
    assert "ld_references = [" in gentropy_config
    assert "ld_references = [" in full_config
    assert (REPO_ROOT / "tests" / "workflows" / "manifest_validation.nf.test").exists()


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


def test_chr1_gentropy_local_profile_uses_gentropy_locus_breaker():
    nextflow_config = NEXTFLOW_CONFIG.read_text()
    gentropy_config = TEST_GENTROPY_LOCAL_CONFIG.read_text()

    assert "testGentropyLocal" in nextflow_config
    assert "includeConfig 'conf/test-gentropy-local.config'" in nextflow_config
    assert 'executor.name = "local"' in gentropy_config
    assert 'manifest   = "${projectDir}/testdata/manifest.tsv"' in gentropy_config
    assert 'output_dir = "${projectDir}/testdata/output_gentropy"' in gentropy_config
    assert 'workDir = "${projectDir}/testdata/work_gentropy"' in gentropy_config
    assert 'locus_breaker_method = "gentropy"' in gentropy_config
    assert 'gentropy_spark_uri   = "local[2]"' in gentropy_config
    assert "spark.driver.memory:8g" in gentropy_config
    assert "spark.executor.memory:8g" in gentropy_config
    assert "ghcr.io/opentargets/gentropy:3.3.0-dev.64" in gentropy_config


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
    assert 'ld_index = "${projectDir}/testdata/sumstats/GCST90002351/chr1.parquet"' in nf_test_pipeline
    assert 'ld_pairs_input = "${projectDir}/testdata/sumstats/GCST90018748/chr1.parquet"' in nf_test_pipeline
    assert "workflow.trace.succeeded().size() == 20" in nf_test_pipeline
    assert 'task.startsWith("LOCUS_BREAKER:COLLECTOR_LOCUS_BREAKER") } == 12' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_BREAKER:GENTROPY_LOCUS_BREAKER_CLUMPING") } == 12' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_COLLECTION:COLLECT_FINEMAPPING_LOCI") } == 4' in nf_test_pipeline
    assert 'task.startsWith("LOCUS_ANNOTATION:STUDY_LOCUS_LD_ANNOTATION") } == 4' in nf_test_pipeline
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
    assert "${projectDir}/testdata/sumstats/GCST90002351/chr1.parquet" in locus_collection_test
    assert 'task.startsWith("LOCUS_BREAKER:COLLECTOR_LOCUS_BREAKER") } == 2' in locus_breaker_test
    assert 'task.startsWith("LOCUS_BREAKER:GENTROPY_LOCUS_BREAKER_CLUMPING") } == 2' in locus_breaker_test
    assert 'task.startsWith("LOCUS_COLLECTION:COLLECT_FINEMAPPING_LOCI") } == 1' in locus_collection_test
    assert 'task.startsWith("LOCUS_ANNOTATION:STUDY_LOCUS_LD_ANNOTATION") } == 1' in locus_annotation_test
    assert "workflow.out[0].size() == 2" in locus_breaker_test
    assert 'topics "versions"' in locus_breaker_test
    assert 'row[0].startsWith("LOCUS_BREAKER:COLLECTOR_LOCUS_BREAKER")' in locus_breaker_test
    assert 'row[0].startsWith("LOCUS_BREAKER:GENTROPY_LOCUS_BREAKER_CLUMPING")' in locus_breaker_test
    assert 'row[1] == "collector"' in locus_breaker_test
    assert 'row[1] == "gentropy"' in locus_breaker_test
    assert 'row[2] == "3.3.0-dev.64"' in locus_breaker_test
    assert 'topics "versions"' in locus_collection_test
    assert 'topics "versions"' in locus_annotation_test
    assert 'topics.versions[0][0].startsWith("LOCUS_COLLECTION:COLLECT_FINEMAPPING_LOCI")' in locus_collection_test
    assert 'topics.versions[0][0].startsWith("LOCUS_ANNOTATION:STUDY_LOCUS_LD_ANNOTATION")' in locus_annotation_test
    assert "workflow.out.ch_full_overlap_loci.size() == 0" in locus_collection_test
    assert "workflow.out.ch_partial_overlap_loci.size() == 1" in locus_collection_test
    assert "workflow.out.ch_non_overlap_loci.size() == 1" in locus_collection_test
    assert "workflow.out.ch_collect_loci_stats.size() == 1" in locus_collection_test
    assert 'meta.studyId } == ["STUDY_A", "STUDY_B", "STUDY_C"]' in locus_collection_test
    assert "workflow.out.ch_locus_annotation.size() == 1" in locus_annotation_test
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
    study_locus_ld_annotation = STUDY_LOCUS_LD_ANNOTATION_MODULE.read_text()

    assert "def args = task.ext.args ?: ''" in collector_locus_breaker
    assert "def args = task.ext.args ?: ''" in gentropy_locus_breaker
    assert "def args = task.ext.args ?: ''" in collect_finemapping_loci
    assert "def args = task.ext.args ?: ''" in study_locus_ld_annotation
    assert "        ${args}" in collector_locus_breaker
    assert "        ${args}" in gentropy_locus_breaker
    assert "        ${args}" in collect_finemapping_loci
    assert "        ${args}" in study_locus_ld_annotation


def test_local_modules_support_process_ext_prefix():
    collector_locus_breaker = COLLECTOR_LOCUS_BREAKER_MODULE.read_text()
    gentropy_locus_breaker = GENTROPY_LOCUS_BREAKER_MODULE.read_text()
    collect_finemapping_loci = COLLECT_FINEMAPPING_LOCI_MODULE.read_text()
    study_locus_ld_annotation = STUDY_LOCUS_LD_ANNOTATION_MODULE.read_text()

    assert "def prefix = task.ext.prefix ?: meta.studyId" in collector_locus_breaker
    assert "def prefix = task.ext.prefix ?: meta.studyId" in gentropy_locus_breaker
    assert "def prefix = task.ext.prefix ?: runId" in collect_finemapping_loci
    assert "def prefix = task.ext.prefix ?: runId" in study_locus_ld_annotation
    assert "${prefix}.parquet" in collector_locus_breaker
    assert "${prefix}" in gentropy_locus_breaker
    assert "${prefix}.parquet" in collect_finemapping_loci
    assert "${prefix}.json" in collect_finemapping_loci
    assert "locus_annotation/${prefix}" in study_locus_ld_annotation
