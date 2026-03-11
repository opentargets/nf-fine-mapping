process Intersection {
    container "intersection"

    input:
    tuple path(sumstats_files), val(ancestry), val(trait), val(sample_size)

    output:
    path "${trait}_${ancestry}.intersection.sumstats.tsv", emit: filtered_sumstats
    path "${trait}_${ancestry}.intersection.variants.tsv", emit: intersection

    script:
    """
    intersection 
    --sumstats ${sumstats_files.join(' ')}
    
    """
}
