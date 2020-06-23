import numpy as np
import os 

from ..algorithms.keep_algorithm import *
from ..algorithms.utils import Volume, get_file_manager, get_named_volumes
from ..exp_utils import create_empty_dir, create_input_chunks


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
    vol_to_write = Volume(0, (5,20,5), (15,30,15))
    file_manager = get_file_manager('HDF5')

    outdir_path = './outdir'
    create_empty_dir(outdir_path)
    data_part = np.random.uniform(size=(10,10,10))
    file_manager.write('./origarr.hdf5', data_part, data_part.shape, _slices=None)
    data_part = file_manager.read_all('./origarr.hdf5')

    outvolume = Volume(2, (0,15,0), (15,30,15))
    cache = {
        2: [(Volume(0, (5,20,5), (15,30,15)), data_part)]
    }

    write_in_outfile(data_part, vol_to_write, file_manager, outdir_path, outvolume, O, (2,2,2), cache, False)
    arr = file_manager.read_all(os.path.join('./outdir', "0_1_0.hdf5"))
    written = arr[5:15, 5:15, 5:15]
    assert np.allclose(arr[0:5, 0:15, 0:15], np.zeros((5,15,15)))
    assert np.allclose(arr[5:15, 0:15, 0:5], np.zeros((10,15,5)))
    assert np.allclose(arr[0:15, 0:5, 0:15], np.zeros((15,5,15)))
    print("shape", written[0][0])
    print("shape", data_part[0][0])
    assert np.allclose(written, data_part)

    create_empty_dir(outdir_path)
    write_in_outfile(data_part, vol_to_write, file_manager, outdir_path, outvolume, O, (2,2,2), cache, True)
    assert len(cache[2]) == 0

    arr = file_manager.read_all(os.path.join('./outdir', "0_1_0.hdf5"))
    written = arr[5:15, 5:15, 5:15]
    assert np.allclose(arr[0:5, 0:15, 0:15], np.zeros((5,15,15)))
    assert np.allclose(arr[5:15, 0:15, 0:5], np.zeros((10,15,5)))
    assert np.allclose(arr[0:15, 0:5, 0:15], np.zeros((15,5,15)))
    assert np.allclose(written, data_part)


def test_read_buffer():
    file_manager = get_file_manager('HDF5')

    # create 4 input chunks
    indir = './indir'
    create_empty_dir(indir)
    partition = (1,2,2)
    cs = (10,10,10)
    create_input_chunks(cs, partition, indir, 'HDF5')

    buffer = Volume(0, (0,5,5), (10,15,15))
    buffers_to_infiles = {
        0: [0,1,2,3]
    }
    involumes = get_named_volumes(partition, cs)
    data = read_buffer(buffer, buffers_to_infiles, involumes, file_manager, indir, (20,20,20), cs)

    # verification
    arr0 = file_manager.read_all(os.path.join(indir, "0_0_0.hdf5"))
    arr1 = file_manager.read_all(os.path.join(indir, "0_0_1.hdf5"))
    arr2 = file_manager.read_all(os.path.join(indir, "0_1_0.hdf5"))
    arr3 = file_manager.read_all(os.path.join(indir, "0_1_1.hdf5"))

    d0 = arr0[0:10, 5:10, 5:10]
    d1 = arr1[0:10, 5:10, 0:5]
    d2 = arr2[0:10, 0:5, 5:10]
    d3 = arr3[0:10, 0:5, 0:5]

    verif = dict()
    for k, v in data.items():
        verif[k.index] = v
    assert np.allclose(verif[0], d0)
    assert np.allclose(verif[1], d1)
    assert np.allclose(verif[2], d2)
    assert np.allclose(verif[3], d3)


def test_equals():
    v1 = Volume(0, (5,5,5), (10,10,10))
    v2 = Volume(5, (5,5,5), (10,10,10))
    v3 = Volume(5, (5,5,5), (10,9,10))
    assert equals(v1, v2)
    assert not equals(v1, v3)


# def test_add_to_cache():
#     add_to_cache(cache, vol_to_write, buff_volume, data_part, outvolume_index)


# def test_keep_algorithm():
#     keep_algorithm(R, O, I, B, volumestokeep, file_format)