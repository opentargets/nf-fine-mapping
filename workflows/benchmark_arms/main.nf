nextflow.enable.dsl = 2
nextflow.enable.types = true

// Comparator arms for the multi-ancestry resolution benchmark.
//
// See docs/prd/meta-collapse.md and docs/benchmarks/multi-ancestry-resolution.md.
//
// The whole benchmark concern lives in this one file, so it is reviewable in
// isolation and removable without touching the production path.
//
// MULTISUSIE_FINE_MAPPING is *aliased*, never modified. Each arm gets its own
// process invocation, which buys two things: `L`, the purity threshold, the
// convergence criteria and the output contract are identical across arms by
// construction rather than by discipline; and each arm's results arrive on its
// own channel, so they can be published separately without joining on
// (runId, fineMappingLocusSetId) -- keys that are equal across arms and would
// therefore collide.
//
// This workflow is a no-op unless params.benchmark_arms names an arm beyond
// 'joint', so default runs are unaffected.

include { COLLECTOR_META_COLLAPSE as COLLECTOR_META_COLLAPSE_META } from '../../modules/local/collector/meta_collapse/main.nf'
include { COLLECTOR_META_COLLAPSE as COLLECTOR_META_COLLAPSE_SINGLE } from '../../modules/local/collector/meta_collapse/main.nf'
include { MULTISUSIE_FINE_MAPPING as MULTISUSIE_FINE_MAPPING_META } from '../../modules/local/multisusie/fine_mapping/main.nf'
include { MULTISUSIE_FINE_MAPPING as MULTISUSIE_FINE_MAPPING_SINGLE } from '../../modules/local/multisusie/fine_mapping/main.nf'

def resolve_benchmark_arms() {
    // Declared inside the function: with nextflow.enable.types, a top-level
    // `def X = ...` is a statement, and statements cannot be mixed with script
    // declarations. workflows/fine_mapping/main.nf scopes supported_methods the
    // same way.
    //
    // 'joint' is the production path and is not re-run here; it is accepted in
    // the list so params.benchmark_arms reads as the full set of arms in play.
    def supported_arms = ['joint', 'meta', 'single'] as Set
    def configured = params.benchmark_arms ?: ['joint']
    // A value from a config file arrives as a List, but `--benchmark_arms
    // joint,meta,single` on the command line arrives as a String. Accept both,
    // so the documented CLI invocation works without a params file.
    if (configured instanceof CharSequence) {
        configured = configured.toString().split(',').collect { arm -> arm.trim() }.findAll { arm -> arm }
    }
    if (!(configured instanceof List) || configured.isEmpty()) {
        error "params.benchmark_arms must be a non-empty list, or a comma-separated string."
    }
    def arms = configured.collect { arm -> arm.toString().toLowerCase() }
    def duplicates = arms.countBy { arm -> arm }.findAll { _arm, count -> count > 1 }.keySet().toList().sort()
    if (duplicates) {
        error "Duplicate benchmark arms: ${duplicates.join(', ')}"
    }
    def unsupported = arms.findAll { arm -> !supported_arms.contains(arm) }.sort()
    if (unsupported) {
        error "Unsupported benchmark arms: ${unsupported.join(', ')}. Supported: ${supported_arms.sort().join(', ')}"
    }
    return arms
}


// The collapsed arm's metadata is a deterministic function of the joint arm's,
// so it is derived here rather than round-tripped through a file. sampleSize is
// the sum because inverse-variance meta-analysis of independent samples has the
// summed effective sample size.
def meta_arm_metas(runId, metas) {
    def total = metas.collect { meta -> meta.sampleSize as Double }.sum()
    return [[studyId: "${runId}__meta".toString(), ancestry: 'meta', sampleSize: total]]
}


// The single arm is the largest-effective-sample-size ancestry. Ties break on
// the ancestry label so the choice is reproducible across runs.
def single_arm_meta(metas) {
    return metas
        .toSorted { left, right ->
            def bySize = (right.sampleSize as Double) <=> (left.sampleSize as Double)
            bySize != 0 ? bySize : (left.ancestry.toString() <=> right.ancestry.toString())
        }
        .first()
}


def to_fine_mapping_input(ch_collapsed) {
    return ch_collapsed.map { r ->
        tuple(
            r.runId,
            r.fine_mapping_locus_set_id,
            r.metas,
            r.fine_mapping_locus_set_path,
            r.multi_ancestry_pairwise_ld_path,
        )
    }
}


workflow BENCHMARK_ARMS {
    take:
    ch_locus_annotation: Channel<Map>

    main:
    def arms = resolve_benchmark_arms()

    if (arms.contains('meta')) {
        ch_meta_collapse_out = COLLECTOR_META_COLLAPSE_META(
            ch_locus_annotation.map { r ->
                tuple(
                    r.runId,
                    r.fine_mapping_locus_set_id,
                    'meta',
                    'meta',
                    '',
                    r.metas,
                    meta_arm_metas(r.runId, r.metas),
                    r.fine_mapping_locus_set_path,
                    r.multi_ancestry_pairwise_ld_path,
                )
            }
        )
        ch_meta_results = MULTISUSIE_FINE_MAPPING_META(to_fine_mapping_input(ch_meta_collapse_out.collapsed))
        ch_meta_stats = ch_meta_collapse_out.stats
    }
    else {
        ch_meta_results = channel.empty()
        ch_meta_stats = channel.empty()
    }

    if (arms.contains('single')) {
        ch_single_collapse_out = COLLECTOR_META_COLLAPSE_SINGLE(
            ch_locus_annotation.map { r ->
                def selected = single_arm_meta(r.metas)
                tuple(
                    r.runId,
                    r.fine_mapping_locus_set_id,
                    'single',
                    'single',
                    selected.ancestry.toString(),
                    r.metas,
                    [selected],
                    r.fine_mapping_locus_set_path,
                    r.multi_ancestry_pairwise_ld_path,
                )
            }
        )
        ch_single_results = MULTISUSIE_FINE_MAPPING_SINGLE(to_fine_mapping_input(ch_single_collapse_out.collapsed))
        ch_single_stats = ch_single_collapse_out.stats
    }
    else {
        ch_single_results = channel.empty()
        ch_single_stats = channel.empty()
    }

    emit:
    ch_meta_results = ch_meta_results
    ch_single_results = ch_single_results
    ch_meta_collapse_stats = ch_meta_stats.mix(ch_single_stats)
}
