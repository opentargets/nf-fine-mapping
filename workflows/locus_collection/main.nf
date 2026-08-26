nextflow.enable.dsl = 2
nextflow.enable.types = true

include { COLLECT_CANONICAL_REGIONS } from '../../modules/local/collector/collect_canonical_regions/main.nf'


workflow LOCUS_COLLECTION {
    take:
    ch_locus: Channel<Map>

    main:
    ch_input = ch_locus
        .map { r -> tuple(r.meta.runId, r.meta, r.study_locus_path, r.summary_statistics_path) }
        .groupTuple(by: 0)
        .map { runId, metas, locus_paths, sumstat_paths ->
            def ordered = (0..<metas.size())
                .collect { idx -> [meta: metas[idx], locus: locus_paths[idx], sumstats: sumstat_paths[idx]] }
                .sort { item -> item.meta.studyId }

            tuple(
                runId,
                ordered.collect { item -> item.meta },
                ordered.collect { item -> item.locus },
                ordered.collect { item -> item.meta.ancestry },
                ordered.collect { item -> item.sumstats },
            )
        }

    collected = COLLECT_CANONICAL_REGIONS(ch_input)

    emit:
    ch_full_overlap_loci = collected.loci.flatMap { runId, metas, locus_dir ->
        locus_dir.listFiles().findAll { it.name.endsWith('.parquet') }.sort { it.name }.collect { path ->
            record(runId: runId, metas: metas, collected_locus_path: path)
        }
    }
    ch_partial_overlap_loci = channel.empty()
    ch_non_overlap_loci = channel.empty()
    ch_collect_loci_stats = collected.stats.map { runId, metas, stats_parquet, stats_json ->
        record(
            runId: runId,
            metas: metas,
            stats_path: stats_json,
            stats_parquet_path: stats_parquet,
        )
    }
    ch_locus_collection_status = collected.status.filter { path -> path }
}
