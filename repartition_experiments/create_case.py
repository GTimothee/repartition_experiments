import argparse
import dask.array as da
import numpy as np
import os, sys, json

def create_input_file(shape, dirname, file_manager):
    filename = f'{shape[0]}_{shape[1]}_{shape[2]}_original.hdf5'
    filepath = os.path.join(dirname, filename)

    # if not os.path.isfile(filepath):
    #     data = np.random.default_rng().random(size=shape, dtype='f')
    #     file_manager.write(filepath, data, shape, _slices=None)

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


# TODO: refactor
def create_input_chunks_distributed(cs, partition, data_dir, file_format):
    """ for HDF5 only for now
        cs: chunk shape
        file_format: file format
        data_dir: to store the file
    """
    if not file_format == "HDF5":
        print("File format not supported yet. Aborting...")
        sys.exit(1)

    create_empty_dir(data_dir)
    print(f"Creating input chunks...")

    stored = 0 # in bytes
    one_chunk_size = cs[0] * cs[1] * cs[2] * 2 # 2 = nb bytes per voxel
    disk_index = 0
    one_disk_size = 440000000000 # 440GB
    repartition_dict = dict()

    for i in range(partition[0]):
        for j in range(partition[1]):
            for k in range(partition[2]):
                if stored + one_chunk_size > one_disk_size:
                    disk_index += 1

                print(f"Creating random array... shape: {cs}")
                arr = da.random.uniform(size=cs)
                print(f"Done, converting to float16...")
                arr = arr.astype(np.float16)
                out_filename = f'{i}_{j}_{k}.hdf5'
                print(f"Building {out_filename} with shape {cs}")
                data_dirpath = os.path.join('/disk' + str(disk_index), 'gtimothee')
                outfilepath = os.path.join(data_dirpath, out_filename)
                print(f"Storing on {data_dirpath}...")
                da.to_hdf5(outfilepath, '/data', arr, chunks=None, compression=None)

                stored += one_chunk_size
                repartition_dict[(i,j,k)] = outfilepath

    print(f"Writing repartition file...")
    json_file = os.path.join('disk0', 'gtimothee', 'repartition_dict.json')
    if os.path.isfile(json_file):
        os.remove(json_file)

    with open(json_file, 'w+') as outfile:
        json.dump(repartition_dict, outfile)
                

def create_case(args):
    paths = load_json(args.paths_config)

    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    from repartition_experiments.exp_utils import create_empty_dir, create_input_chunks
    from repartition_experiments.algorithms.clustered_writes import clustered_writes
    from repartition_experiments.algorithms.utils import get_file_manager, get_blocks_shape

    fm = get_file_manager(args.file_format)
    R_stringlist, I_stringlist = args.R.split('_'), args.I.split('_')
    R, I = tuple(map(lambda e: int(e), R_stringlist)), tuple(map(lambda e: int(e), I_stringlist))
    print(R, I)

    indir_path, outdir_path = os.path.join(paths["ssd_path"], 'indir'), os.path.join(paths["ssd_path"], 'outdir')
    partition = get_blocks_shape(R, I)
    
    if args.distributed:
        create_input_chunks_distributed(I, partition, indir_path, args.file_format)
        return

    if not args.splits_only:
        origarr_filepath = create_input_file(R, paths["ssd_path"], fm)
        print("creating input file...", origarr_filepath)
        bpv = 2
        R_size = R[0]*R[1]*R[2]*bpv
        create_empty_dir(indir_path)
        create_empty_dir(outdir_path)
        clustered_writes(origarr_filepath, R, I, bpv, R_size, args.file_format, indir_path)
    else:
        create_input_chunks(I, partition, indir_path, args.file_format)


if __name__ == "__main__":

    args = get_arguments()
    create_case(args)