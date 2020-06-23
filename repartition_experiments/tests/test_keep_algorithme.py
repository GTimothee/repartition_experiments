import numpy as np
import os 

from ..algorithms.keep_algorithm import *
from ..algorithms.utils import Volume, get_file_manager
from ..exp_utils import create_empty_dir

def test_remove_from_cache():
    cache = {
        2: [(Volume(0, (0,0,0), (5,5,5)), np.zeros((5,5,5)))]
    }
    outfile_index = 2
    volume_to_write = Volume(0, (0,0,0), (5,5,5))
    remove_from_cache(cache, outfile_index, volume_to_write)

    assert len(cache[outfile_index]) == 0


def test_write_in_outfile():
    R = (30,30,30)
    O = (15,15,15)
    data_part = np.random.normal()
    vol_to_write = Volume(0, (5,20,5), (15,30,15))
    file_manager = get_file_manager('HDF5')
    outdir_path = './outdir'
    create_empty_dir(outdir_path)
    outvolume = Volume(2, (0,15,0), (15,30,15))
    cache = {
        2: [(Volume(0, (5,20,5), (15,30,15)), data_part)]
    }

    write_in_outfile(data_part, vol_to_write, file_manager, outdir_path, outvolume, O, (2,2,2), cache, False)
    arr = file_manager.read_all(os.path.join('./outdir', "0_1_0.hdf5"))
    written = arr[5:15, 5:15, 5:15]
    assert np.allclose(written, data_part)
    assert np.allclose(arr[0:5, 0:15, 0:15], np.zeros((5,15,15)))
    assert np.allclose(arr[5:15, 0:15, 0:5], np.zeros((10,15,5)))
    assert np.allclose(arr[0:15, 0:5, 0:15], np.zeros((15,5,15)))

    create_empty_dir(outdir_path)
    write_in_outfile(data_part, vol_to_write, file_manager, outdir_path, outvolume, O, (2,2,2), cache, True)
    assert len(cache[2]) == 0

    arr = file_manager.read_all(os.path.join('./outdir', "0_1_0.hdf5"))
    written = arr[5:15, 5:15, 5:15]
    assert np.allclose(written, data_part)
    assert np.allclose(arr[0:5, 0:15, 0:15], np.zeros((5,15,15)))
    assert np.allclose(arr[5:15, 0:15, 0:5], np.zeros((10,15,5)))
    assert np.allclose(arr[0:15, 0:5, 0:15], np.zeros((15,5,15)))


# def test_read_buffer():
#     read_buffer(buffer, buffers_to_infiles, involumes, file_manager, input_dirpath)


# def test_equals():
#     equals(vol_to_write, buff_volume)


# def test_add_to_cache():
#     add_to_cache(cache, vol_to_write, buff_volume, data_part, outvolume_index)


# def test_keep_algorithm():
#     keep_algorithm(R, O, I, B, volumestokeep, file_format)