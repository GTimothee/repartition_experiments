import os, h5py, time, logging, csv, json
import numpy as np

from repartition_experiments.algorithms.utils import _3d_to_numeric_pos, get_file_manager, get_blocks_shape, get_named_volumes, hypercubes_overlap, get_overlap_subarray, numeric_to_3d_pos, Volume
from repartition_experiments.algorithms.utils import get_opened_files
from repartition_experiments.algorithms.tracker import Tracker

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__ + 'baseline')

DEBUG_LOCAL = False
DONT_WRITE = False
tracker = None

def get_overlap_volume(v1, v2):
    pair = get_overlap_subarray(v1, v2)  # overlap coordinates in basis of R
    p1, p2 = tuple(pair[0]), tuple(pair[1])
    return Volume(0, p1, p2)


def write_to_outfile(involume, outvolume, data, outfiles_partition, outdir_path, O, file_manager, addition, tracker):
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
    overlap_vol = get_overlap_volume(involume, outvolume)
    overlap_shape = overlap_vol.get_shape()
    if DONT_WRITE:
        tracker.add_volume(overlap_vol)

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

    if DONT_WRITE:
        print(f"Overlap shape: {overlap_shape}")
        print(f"Outfile shape: {O}")
        print(f"Number seeks: {nb_outfile_seeks_tmp}")
        return overlap_shape, 0, nb_outfile_seeks_tmp

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

    if addition:
        subarr_data = subarr_data + 1

    global outdirs_dict, outdir_index

    if (i, j, k) in outdirs_dict.keys():
        outdir_path = outdirs_dict[(i, j, k)]
        print(f"Writing at: {outdir_path}")
    else:
        outdir_path = '/disk' + str(outdir_index) + '/gtimothee/output'
        outdirs_dict[(i, j, k)] = outdir_path
        outdir_index += 1
        if outdir_index == 6:
            outdir_index = 0

        print(f"Writing at: {outdir_path}")
        print(f"Increasing writing index: {outdir_index}")

    t2 = time.time()
    if not DONT_WRITE:
        file_manager.write_data(i, j, k, outdir_path, subarr_data, slices_in_outfile, O)
    t2 = time.time() - t2
    
    if DEBUG_LOCAL: 
        file_manager.test_write(outfile_path, slices_in_outfile, subarr_data)

    return overlap_shape, t2, nb_outfile_seeks_tmp


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


def baseline_rechunk(indir_path, outdir_path, O, I, R, file_format, addition, distributed, debug_mode=False, clean_out_dir=False, dont_write=False):
    """ Naive rechunk implementation in plain python.
    The input directory is supposed to contain the input files (output of the split process).
    WARNING: Does not clean the output directory after use by default.
    """

    print(f"Setting arguments...")
    global DEBUG_LOCAL
    global DONT_WRITE
    global tracker
    global outdirs_dict, outdir_index
    outdirs_dict = dict()
    outdir_index = 0
    tracker = Tracker()
    DEBUG_LOCAL = True if debug_mode else False
    DONT_WRITE = True if dont_write else False

    print("Addition mode:", addition)
    print("DONT_WRITE: ", DONT_WRITE)

    O, I, R = tuple(O), tuple(I), tuple(R)

    file_manager = get_file_manager(file_format)

    infiles_partition = get_blocks_shape(R, I)
    infiles_volumes = get_named_volumes(infiles_partition, I)
    outfiles_partition = get_blocks_shape(R, O)
    outfiles_volumes = get_named_volumes(outfiles_partition, O)
    outfiles_volumes = outfiles_volumes.values()

    if distributed:
        repartition_dict = None
        
        json_filename = '/disk0/gtimothee/repartition_dict.json'
        if not os.path.isfile(json_filename):
            # print("cannot find association dict json file")
            sys.exit(1)
        else:
            pass # print(f"json file found")

        try: 
            with open(json_filename) as f:
                repartition_dict = json.load(f)
        except Exception as e: 
            print(e)
            # print("error (1)")
            sys.exit(1)

        if repartition_dict == None:
            # print("error (2)")
            sys.exit(1)
        else:
            pass # print(f"Found reparition dict: {repartition_dict}")

        input_files = repartition_dict.values()
    else:
        input_files = file_manager.get_input_files(indir_path)

    t_read = 0
    t_write = 0

    vols_written = list()
    nb_infile_openings = 0
    nb_infile_seeks = 0
    nb_outfile_openings = 0
    nb_outfile_seeks = 0
    buffer_index = 1
    for input_file in input_files:
        print(f"Treating buffer: {buffer_index}...")
        buffer_index += 1
        nb_infile_openings += 1

        involume = get_volume(input_file, infiles_volumes, infiles_partition)
        t1 = time.time()
        if not DONT_WRITE:
            data = file_manager.read_data_from_fp(input_file, slices=None)
        else:
            data = None
        t1 = time.time() - t1
        t_read += t1
        
        for outvolume in outfiles_volumes:
            if hypercubes_overlap(involume, outvolume):
                shape, t2, nb_outfile_seeks_tmp = write_to_outfile(involume, outvolume, data, outfiles_partition, outdir_path, O, file_manager, addition, tracker)
                t_write += t2
                vols_written.append(shape)
                # nb_outfile_openings += 1 already included in nb_outfile_seeks
                nb_outfile_seeks += nb_outfile_seeks_tmp
        
        file_manager.close_infiles()

    if DONT_WRITE:
        assert tracker.is_complete(((0,0,0), R))

    # print("\nShapes written:")
    # for row in vols_written: 
    #     print(row)

    if clean_out_dir:
        print("Cleaning output directory")
        file_manager.clean_directory(outdir_path)

    get_opened_files()

    return t_read, t_write, [nb_outfile_openings, nb_outfile_seeks, nb_infile_openings, nb_infile_seeks]
