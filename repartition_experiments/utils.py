import sys
import numpy as np

from .file_formats.hdf5 import HDF5_manager


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