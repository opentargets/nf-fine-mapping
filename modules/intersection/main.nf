process Intersection {
    container "intersection"

    input:
    tuple val(trait), val(meta), path(sumstats_files)

    script:
    """
    echo "Running intersection for trait ${trait}"
    
    """
}
