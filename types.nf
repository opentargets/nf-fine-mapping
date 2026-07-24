nextflow.enable.dsl = 2
nextflow.enable.types = true

enum Route {
    multi_susie_route,
}

record MetaRecord {
    runId: String
    studyId: String
    route: String
    ancestry: String
    traitSet: List<String>
    sampleSize: Integer
}


record ManifestRecord {
    summary_statistics_path: Path
    meta: MetaRecord
}


record LocusRecord {
    study_locus_path: Path
    meta: MetaRecord
}
