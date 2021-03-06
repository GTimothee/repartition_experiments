import argparse
import dask.array as da
import numpy as np
import os, sys, json

""" File that creates the input blocks for the experiment.
To be run before the experiment.
"""

def create_input_file(shape, dirname, file_manager):
    """ Creating the original array.
    """
    filename = f'{shape[0]}_{shape[1]}_{shape[2]}_original.hdf5'
    filepath = os.path.join(dirname, filename)

    if not os.path.isfile(filepath):
        arr = da.random.random(size=shape)
        arr = arr.astype(np.float16)
        da.to_hdf5(filepath, '/data', arr, chunks=None, compression=None)

    return filepath


def get_arguments():
    """ Get arguments from console command.
    """
    parser = argparse.ArgumentParser(description="This experiment is referenced as experiment 3 in Guédon et al.")
    
    parser.add_argument('paths_config', 
        action='store', 
        type=str, 
        help='Path to configuration file containing paths of data directories.')

    parser.add_argument('R',
        action='store',
        help='')
    
    parser.add_argument('I',
        action='store',
        help='')

    parser.add_argument('-f', '--file_format',
        action='store',
        type=str,
        dest='file_format',
        default='HDF5',
        help='File format of arrays manipulated.')

    parser.add_argument('-s', '--splits_only', 
        action='store_true', 
        dest='splits_only',
        default=False,
        help='do not create original array and split, just creates the splits directly')


    parser.add_argument('-d', '--distributed', 
        action='store_true', 
        dest='distributed',
        default=False,
        help='if array is bigger than size of one disk, store parts on several disks and generate a summary file to be used by keep algorithm')

    return parser.parse_args()


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)
                

def create_case(args):
    paths = load_json(args.paths_config)

    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    from repartition_experiments.scripts_exp.exp_utils import create_empty_dir, create_input_chunks, create_input_chunks_distributed
    from repartition_experiments.algorithms.clustered_writes import clustered_writes
    from repartition_experiments.algorithms.utils import get_file_manager, get_blocks_shape

    # preprocessing
    fm = get_file_manager(args.file_format)
    R_stringlist, I_stringlist = args.R.split('_'), args.I.split('_')
    R, I = tuple(map(lambda e: int(e), R_stringlist)), tuple(map(lambda e: int(e), I_stringlist))
    print(R, I)
    indir_path, outdir_path = os.path.join(paths["ssd_path"], 'indir'), os.path.join(paths["ssd_path"], 'outdir')
    partition = get_blocks_shape(R, I)

    if args.distributed:  # only creates the input blocks, without creating the big image first and splitting it, and stores each chunk in a rounding fashion on the different disks of the cluster
        create_input_chunks_distributed(I, partition, indir_path, args.file_format)
        return

    if not args.splits_only: # creating input image and then splitting it.
        origarr_filepath = create_input_file(R, paths["ssd_path"], fm)
        print("creating input file...", origarr_filepath)
        bpv = 2
        R_size = R[0]*R[1]*R[2]*bpv
        create_empty_dir(indir_path)
        create_empty_dir(outdir_path)
        clustered_writes(origarr_filepath, R, I, bpv, R_size, args.file_format, indir_path)
    else:  # only creates the input blocks, without creating the big image first and splitting it
        create_input_chunks(I, partition, indir_path, args.file_format)


if __name__ == "__main__":

    args = get_arguments()
    create_case(args)