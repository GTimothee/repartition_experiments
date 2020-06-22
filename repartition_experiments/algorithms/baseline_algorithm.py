import os, h5py, time, logging
import numpy as np

from .utils import get_blocks_shape, get_named_volumes, hypercubes_overlap, _3d_to_numeric_pos, numeric_to_3d_pos, Volume, get_file_manager

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__ + 'baseline')

DEBUG_LOCAL = False

def write_to_outfile(involume, outvolume, data, outfiles_partition, outdir_path, O, file_manager):
    """ Write intersection of input file and output file into output file.

    Arguments:
    ----------
        involume: input volume representing the input file
        outvolume: input volume representing the output file
        data: input chunk read from input file
        outfiles_partition: partition tuple of R by O
        outdir_path: output directory to store the output files
        O: output file shape
        file_manager: object to read/write into files using a specific file format
    """
    lowcorner, upcorner = get_overlap_subarray(involume, outvolume)  # find subarray crossing both files in the basis of the original image

    slices = [(lowcorner[0], upcorner[0]), (lowcorner[1], upcorner[1]), (lowcorner[2], upcorner[2])]
    offset_in = involume.get_corners()[0]  # lower corner
    offset_out = outvolume.get_corners()[0]

    slices_in_infile = [ # convert corners in the basis of input file
        (lowcorner[0]-offset_in[0], upcorner[0]-offset_in[0]), 
        (lowcorner[1]-offset_in[1], upcorner[1]-offset_in[1]), 
        (lowcorner[2]-offset_in[2], upcorner[2]-offset_in[2])]
    
    slices_in_outfile = [ # convert corners in the basis of output file
        (lowcorner[0]-offset_out[0], upcorner[0]-offset_out[0]), 
        (lowcorner[1]-offset_out[1], upcorner[1]-offset_out[1]), 
        (lowcorner[2]-offset_out[2], upcorner[2]-offset_out[2])]

    if DEBUG_LOCAL:
        logger.debug(f"[debug] extracting {s[0][0]}:{s[0][1]}, {s[1][0]}:{s[1][1]}, {s[2][0]}:{s[2][1]} from input file")
        logger.debug(f"[debug] inserting {s2[0][0]}:{s2[0][1]}, {s2[1][0]}:{s2[1][1]}, {s2[2][0]}:{s2[2][1]} into output file {out_filename}")

    s = slices_in_infile
    subarr_data = data[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]]  # extract subarr from input file's data 

    _3d_pos = numeric_to_3d_pos(outvolume.index, outfiles_partition, order='C')
    i, j, k = _3d_pos
    file_manager.write_data(i, j, k, outdir_path, subarr_data, slices_in_outfile, O)
    
    if DEBUG_LOCAL: 
        file_manager.test_write(outfile_path, slices_in_outfile, subarr_data)


def get_volume(infilepath, infiles_volumes, infiles_partition):
    """ Get Volume object associated to a file.

    Arguments: 
    ----------
        infilepath: path to input file. 
        infiles_volumes: list of all volumes, each one represents an input file
        infiles_partition: partition tuple of original array by input files
    """
    filename = infilepath.split('/')[-1]
    pos = filename.split('_')
    pos[-1] = pos[-1].split('.')[0]
    pos = tuple(list(map(lambda s: int(s), pos)))
    numeric_pos = _3d_to_numeric_pos(pos, infiles_partition, order='C')
    return infiles_volumes[numeric_pos]


def get_overlap_subarray(hypercube1, hypercube2):
    """ Find the intersection of both files.
    Refactor of hypercubes_overlap to return the overlap subarray

    Returns: 
    --------
        pair of corners of the subarray
    See also:
    ---------
        utils.hypercubes_overlap
    """

    if not isinstance(hypercube1, Volume) or \
        not isinstance(hypercube2, Volume):
        raise TypeError()

    lowercorner1, uppercorner1 = hypercube1.get_corners()
    lowercorner2, uppercorner2 = hypercube2.get_corners()
    nb_dims = len(uppercorner1)
    
    subarray_lowercorner = list()
    subarray_uppercorner = list()
    for i in range(nb_dims):
        subarray_lowercorner.append(max(lowercorner1[i], lowercorner2[i]))
        subarray_uppercorner.append(min(uppercorner1[i], uppercorner2[i]))

    print(f"Overlap subarray : {subarray_lowercorner[0]}:{subarray_uppercorner[0]}, {subarray_lowercorner[1]}:{subarray_uppercorner[1]}, {subarray_lowercorner[2]}:{subarray_uppercorner[2]}")
    return (subarray_lowercorner, subarray_uppercorner)


def baseline_rechunk(indir_path, outdir_path, O, I, R, file_format, debug_mode=False, clean_out_dir=False):
    """ Naive rechunk implementation in plain python.
    The input directory is supposed to contain the input files (output of the split process).
    WARNING: Does not clean the output directory after use by default.

    Returns: 
    --------
        processing time
    """
    DEBUG_LOCAL = True if debug_mode else False

    file_manager = get_file_manager(file_format)

    infiles_partition = get_blocks_shape(R, I)
    infiles_volumes = get_named_volumes(infiles_partition, I)
    outfiles_partition = get_blocks_shape(R, O)
    outfiles_volumes = get_named_volumes(outfiles_partition, O)
    outfiles_volumes = outfiles_volumes.values()
    input_files = file_manager.get_input_files(indir_path)

    try:
        t = time.time()
        for input_file in input_files:
            involume = get_volume(input_file, infiles_volumes, infiles_partition)
            
            data = file_manager.read(input_file)
            
            for outvolume in outfiles_volumes:
                if hypercubes_overlap(involume, outvolume):
                    write_to_outfile(involume, outvolume, data, outfiles_partition, outdir_path, O, file_manager)
            
            file_manager.close_infiles()

        t = time.time() - t

        if clean_out_dir:
            file_manager.clean_directory(outdir_path)
        return t

    except Exception as e:
        print(e)
        return None