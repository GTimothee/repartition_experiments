import sys
import numpy as np

from .file_formats.hdf5 import HDF5_manager


def create_input_chunks(I, O, indir_path, format):
    if file_format == "HDF5":
        file_manager = HDF5_manager()
    else:
        print("File format not supported yet. Aborting...")
        sys.exit(1)

    chunk_slices = ((0,I[0]), (0,I[1]), (0,I[2]))
    for i in range(I[0]):
        for j in range(I[1]):
            for k in range(I[2]):
                data = np.random.normal(size=I)
                file_manager.write_data(i, j, k, indir_path, data, chunk_slices, I)