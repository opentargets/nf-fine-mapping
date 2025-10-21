
test_sumstats = channel.fromPath("/home/louwenjjr/sushie/data/AFR.gwas")
test_LD = channel.fromPath("/home/louwenjjr/sushie/data/AFR.ld")


workflow {
    run_sushie(test_sumstats, test_LD, channel.fromPath("test_output"), 639)
}


process run_sushie {
    input:
    path sumstats_file
    path ld // LD file, with column names, variant ids need to correspond to sumstats file
    path output_prefix
    val sample_size

    shell:
    """
    # need to somehow explode the other columns based on number of ancestries
    sushie finemap --summary --gwas $sumstats_file --ld $ld --sample-size $sample_size --output $output_prefix
    """
}