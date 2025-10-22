
test_sumstats = "/home/louwenjjr/sushie/data/EUR.gwas /home/louwenjjr/sushie/data/AFR.gwas"
test_ld = "/home/louwenjjr/sushie/data/EUR.ld /home/louwenjjr/sushie/data/AFR.ld"
test_sample_sizes = "489 639"
output = "test_output"


workflow {
    SUSHIE(test_sumstats, test_ld, test_sample_sizes, output)
}

process SUSHIE {
    // todo: push the image to a registry. for now build the Dockerfile in this dir like docker build . -t sushie
    container "sushie"

    input:
    path study_locus_files
    // LD files: only column names (variant ids), variant ids need to correspond to sumstats file
    path ld_files
    val sample_sizes
    val output_prefix

    output:
    path "*.sushie.corr.tsv", emit: corr
    path "*.sushie.cs.tsv", emit: cs
    path "*.sushie.weights.tsv", emit: weights

    shell:
    """
    sushie finemap --summary --gwas $study_locus_files --ld $ld_files --sample-size $sample_sizes --output $output_prefix --gwas-header chromosome variantId position referenceAllele alternateAllele zScore
    """
}
