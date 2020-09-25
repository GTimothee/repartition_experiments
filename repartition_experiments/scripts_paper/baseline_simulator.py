import numpy as np

from repartition_experiments.algorithms.utils import get_blocks_shape, get_named_volumes, hypercubes_overlap, get_overlap_subarray, Volume


def get_overlap_volume(v1, v2):
    pair = get_overlap_subarray(v1, v2)  # overlap coordinates in basis of R
    p1, p2 = tuple(pair[0]), tuple(pair[1])
    return Volume(0, p1, p2)


def write_buffer(involume, outvolume, O):
    overlap_shape = get_overlap_volume(involume, outvolume).get_shape()

    nb_outfile_seeks_tmp = 0
    s = overlap_shape
    if s[2] != O[2]:
        nb_outfile_seeks_tmp += s[0]*s[1]
    elif s[1] != O[1]:
        nb_outfile_seeks_tmp += s[0]
    elif s[0] != O[0]:
        nb_outfile_seeks_tmp += 1
    else:
        pass

    return nb_outfile_seeks_tmp


def baseline_rechunk(O, I, R):
    """
    Arguments: 
    ----------
        O, I, R: tuples
    """

    infiles_partition = get_blocks_shape(R, I)
    inblocks = get_named_volumes(infiles_partition, I)
    
    outfiles_partition = get_blocks_shape(R, O)
    outblocks = get_named_volumes(outfiles_partition, O)

    nb_infile_openings = 0
    nb_infile_seeks = 0
    nb_outfile_openings = 0
    nb_outfile_seeks = 0

    for buffer_index in sorted(inblocks.keys()):
        read_buffer = inblocks[buffer_index]
        nb_infile_openings += 1

        for outblock in outblocks.values():
            if hypercubes_overlap(read_buffer, outblock):
                nb_outfile_seeks_tmp = write_buffer(read_buffer, outblock, O)
                nb_outfile_seeks += nb_outfile_seeks_tmp
                #nb_outfile_openings += 1

    return nb_outfile_openings + nb_outfile_seeks + nb_infile_openings + nb_infile_seeks