nextflow.enable.dsl = 2
nextflow.enable.types = true

include { COLLECT_FINEMAPPING_LOCI } from '../../modules/local/collector/collect_finemapping_loci/main.nf'
include { SPLIT_FINEMAPPING_LOCI } from '../../modules/local/collector/split_finemapping_loci/main.nf'
include { COLLECTOR_EMPTY_STATUS } from '../../modules/local/collector/empty_status/main.nf'


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
    ch_split_finemapping_loci = SPLIT_FINEMAPPING_LOCI(
        ch_collected_loci.full_overlap.map { runId, metas, collected_locus_path ->
            tuple(runId, metas, collected_locus_path)
        }
    )
    ch_collection_status_input = ch_collected_loci.full_overlap.map { runId, metas, collected_locus_path ->
        tuple(
            runId,
            "collected_loci/full_overlaps/${runId}.parquet",
            "LOCUS_COLLECTION",
            collected_locus_path,
        )
    }
    ch_collection_status = COLLECTOR_EMPTY_STATUS(ch_collection_status_input)
        .filter { status_path -> status_path != null }

    emit:
    ch_full_overlap_loci      = ch_split_finemapping_loci
        .flatMap { r ->
            def paths = r.fine_mapping_locus_set_dir.listFiles()
                .findAll { path -> path.name.endsWith('.parquet') }
                .sort { path -> path.name }
            paths.collect { path ->
                record(runId: r.runId, metas: r.metas, collected_locus_path: path)
            }
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
    ch_locus_collection_status = ch_collection_status
}
