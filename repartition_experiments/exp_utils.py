import sys, os, shutil, json
import numpy as np
import dask.array as da

from repartition_experiments.file_formats.hdf5 import HDF5_manager
from repartition_experiments.algorithms.utils import get_blocks_shape


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)


def create_input_chunks(cs, partition, data_dir, file_format):
    """
        cs: chunk shape
        file_format: file format
        data_dir: to store the file
    """
    if file_format == "HDF5":
        file_manager = HDF5_manager()
    else:
        print("File format not supported yet. Aborting...")
        sys.exit(1)

    print(f"Creating input chunks at {data_dir}")

    create_empty_dir(data_dir)

    _slices = ((0,cs[0]), (0,cs[1]), (0,cs[2]))
    for i in range(partition[0]):
        for j in range(partition[1]):
            for k in range(partition[2]):
                arr = da.random.random(size=cs)
                arr = arr.astype(np.float16)
                out_filename = f'{i}_{j}_{k}.hdf5'
                print(f"Building {out_filename} with shape {cs}")
                outfilepath = os.path.join(data_dir, out_filename)
                da.to_hdf5(outfilepath, '/data', arr, chunks=None, compression=None)
                # data = np.random.uniform(size=cs)
                # file_manager.write_data(i, j, k, data_dir, data, _slices, cs)


def create_empty_dir(dir_path):
    """
    dir exists => erase content
    dir does not exist => creates dir
    """
    if os.path.isdir(dir_path):
        shutil.rmtree(dir_path)

    os.mkdir(dir_path)
    if not os.path.isdir(dir_path):
        raise OSError()


def verify_results(outdir_path, original_array_path, R, O, file_format, addition, split_merge=False):
    """ Compare content of each output file against expected subarrays from original array.
    WARNING: this function opens all output files + the original array
    """

    if file_format == "HDF5":
        file_manager = HDF5_manager()
    else:
        print("File format not supported yet. Aborting...")
        sys.exit(1)

    partition = get_blocks_shape(R, O)
    orig_arr_data = file_manager.read_all(original_array_path)
    all_true = True

    if split_merge:
        result_arrpath = os.path.join(outdir_path, "0_0_0.hdf5")
        return file_manager.check_split_merge(original_array_path, result_arrpath)

    for i in range(partition[0]):
        for j in range(partition[1]):
            for k in range(partition[2]):
                outfilepath = os.path.join(outdir_path, str(i) + "_" + str(j) + "_" + str(k) + ".hdf5")
                data_stored = file_manager.read_all(outfilepath)
                ground_truth = orig_arr_data[i*O[0]:(i+1)*O[0],j*O[1]:(j+1)*O[1],k*O[2]:(k+1)*O[2]]
                
                if addition:
                    ground_truth = ground_truth +1

                try:
                    assert np.allclose(data_stored, ground_truth, rtol=1e-02)
                    # print(f"Good output file {outfilepath}")
                except:
                    print(f"Error: bad rechunking {outfilepath}")
                    print(f"Slices from ground truth {i*O[0]}:{(i+1)*O[0]}, {j*O[1]}:{(j+1)*O[1]}, {k*O[2]}:{(k+1)*O[2]}")
                    print("data_stored", data_stored)
                    print("ground_truth", ground_truth)
                    all_true = False  # do not return here to see all failures

    file_manager.close_infiles()  # close all files
    return all_true


def get_case_arguments():
    if case == 1:
        R, O, I = tuple(run["R"]), tuple(run["O"]), tuple(run["I"])
        lambd = get_input_aggregate(O, I)
        B, volumestokeep = (lambd[0], lambd[1], lambd[2]), list(range(1, 8))
    elif case == 2:
        R, O, I, B, volumestokeep = tuple(run["R"]), tuple(run["O"]), tuple(run["I"]), tuple(run["B"]), run["volumestokeep"]
    else:
        raise ValueError("Case index does not exist")

    return R, O, I, B, volumestokeep