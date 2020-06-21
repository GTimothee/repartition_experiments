import sys, os, shutil
import numpy as np

from .file_formats.hdf5 import HDF5_manager
from .algorithms.utils import get_blocks_shape


def create_input_chunks(cs, data_dir, file_format):
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

    _slices = ((0,cs[0]), (0,cs[1]), (0,cs[2]))
    for i in range(cs[0]):
        for j in range(cs[1]):
            for k in range(cs[2]):
                data = np.random.normal(size=cs)
                file_manager.write_data(i, j, k, data_dir, data, _slices, cs)


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


def verify_results(outdir_path, original_array_path, R, O):
    """ Compare content of each output file against expected subarrays from original array
    """
    outfiles_partition = get_blocks_shape(R, O)

    all_true = True
    with h5py.File(original_array_path, 'r') as f:
        orig_arr = f["/data"]

        for i in range(outfiles_partition[0]):
            for j in range(outfiles_partition[1]):
                for k in range(outfiles_partition[2]):
                    outfilepath = os.path.join(outdir_path, str(i) + "_" + str(j) + "_" + str(k)  + ".hdf5")

                    with h5py.File(outfilepath, 'r') as f:
                        data_stored = f["/data"]
                        ground_truth = orig_arr[i*O[0]:(i+1)*O[0],j*O[1]:(j+1)*O[1],k*O[2]:(k+1)*O[2]]

                        try:
                            assert np.allclose(data_stored[()], ground_truth)
                            print(f"Good output file {outfilename}")
                        except:
                            print(f"Slices from ground truth {i*O[0]}:{(i+1)*O[0]}, {j*O[1]}:{(j+1)*O[1]}, {k*O[2]}:{(k+1)*O[2]}")
                            print("data_stored", data_stored[()])
                            print("ground_truth", ground_truth)
                            print(f"Error: bad rechunking {outfilename}")
                            all_true = False  # do not return here to see all failures
    return all_true