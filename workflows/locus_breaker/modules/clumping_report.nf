nextflow.enable.types = true

process clumping_report {

    label "collector"

    input:
    array_of_locus_paths: List<Path>

    output:
    file("clumping_report.parquet")

    script:
    """
    collector clumping_report \
        ${array_of_locus_paths.join(' ')} \
        --output clumping_report.parquet
    """

    stub:
    """
    touch clumping_report.parquet
    """
}
