nextflow.enable.dsl = 2
nextflow.enable.types = true

include { STUDY_LOCUS_LD_ANNOTATION } from '../../modules/local/collector/study_locus_ld_annotation/main.nf'


workflow LOCUS_ANNOTATION {
    take:
    ch_full_overlap_loci: Channel<Map>

    main:
    ch_study_locus_ld_annotation_input = ch_full_overlap_loci.map { r ->
        if (!params.ld_index || !params.ld_pairs_input) {
            error "LD annotation requires params.ld_index and params.ld_pairs_input when full-overlap loci are present."
        }

        tuple(
            r.runId,
            r.metas,
            r.collected_locus_path,
            file(params.ld_index),
            file(params.ld_pairs_input),
        )
    }

    ch_study_locus_ld_annotation = STUDY_LOCUS_LD_ANNOTATION(ch_study_locus_ld_annotation_input)

    ch_locus_annotation = ch_study_locus_ld_annotation.map { runId, fine_mapping_loci_path, ld_pairs_path ->
        record(runId: runId, fine_mapping_loci_path: fine_mapping_loci_path, ld_pairs_path: ld_pairs_path)
    }

    emit:
    ch_locus_annotation = ch_locus_annotation
    ch_fine_mapping_loci = ch_locus_annotation.map { r -> r.fine_mapping_loci_path }
    ch_ld_pairs = ch_locus_annotation.map { r -> r.ld_pairs_path }
}
