nextflow.enable.dsl = 2
nextflow.enable.types = true

include { COLLECT_FINEMAPPING_LOCI } from '../../modules/local/collector/collect_finemapping_loci/main.nf'


workflow LOCUS_COLLECTION {
    take:
    ch_locus: Channel<Map>

    main:
    ch_collect_finemapping_loci_input = ch_locus
        .map { r ->
            tuple(r.meta.runId, r.meta, r.study_locus_path)
        }
        .groupTuple(by: 0)
        .map { runId, metas, study_locus_paths ->
            def ordered = (0..<metas.size())
                .collect { idx -> [meta: metas[idx], path: study_locus_paths[idx]] }
                .sort { item -> item.meta.studyId }

            tuple(
                runId,
                ordered.collect { item -> item.meta },
                ordered.collect { item -> item.path },
            )
        }

    ch_collected_loci = COLLECT_FINEMAPPING_LOCI(ch_collect_finemapping_loci_input)

    emit:
    ch_full_overlap_loci      = ch_collected_loci.full_overlap
        .filter { _runId, _metas, collected_locus_path -> collected_locus_path != null }
        .map { runId, metas, collected_locus_path ->
            record(runId: runId, metas: metas, collected_locus_path: collected_locus_path)
        }
    ch_partial_overlap_loci   = ch_collected_loci.partial_overlap.map { runId, metas, collected_locus_path ->
        record(runId: runId, metas: metas, collected_locus_path: collected_locus_path)
    }
    ch_non_overlap_loci       = ch_collected_loci.non_overlap.map { runId, metas, collected_locus_path ->
        record(runId: runId, metas: metas, collected_locus_path: collected_locus_path)
    }
    ch_collect_loci_stats     = ch_collected_loci.stats.map { runId, metas, stats_path ->
        record(runId: runId, metas: metas, stats_path: stats_path)
    }
}
