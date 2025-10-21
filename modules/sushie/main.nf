
test_sumstats = "/home/louwenjjr/sushie/data/EUR.gwas /home/louwenjjr/sushie/data/AFR.gwas"
test_ld = "/home/louwenjjr/sushie/data/EUR.ld /home/louwenjjr/sushie/data/AFR.ld"
test_sample_sizes = "489 639"
output = "test_output"


workflow {
    run_sushie(test_sumstats, test_ld, test_sample_sizes, output)
}

process run_sushie {
    input:
    val study_locus_files
    // LD files: with column names (variant ids), variant ids need to correspond to sumstats file
    val ld_files
    val sample_sizes
    val output_prefix

    output:
    val "*.sushie.corr.tsv", emit: corr
    val "*.sushie.cs.tsv", emit: cs
    val "*.sushie.weights.tsv", emit: weights

    shell:
    """
    sushie finemap --summary --gwas $study_locus_files --ld $ld_files --sample-size $sample_sizes --output $output_prefix
    """
}