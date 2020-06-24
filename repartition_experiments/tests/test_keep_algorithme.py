import numpy as np
import os, pytest

from ..algorithms.keep_algorithm import *
from ..algorithms.utils import Volume, get_file_manager, get_named_volumes
from ..exp_utils import create_empty_dir, create_input_chunks, verify_results
from ..algorithms.clustered_writes import clustered_writes

def test_remove_from_cache():
    cache = {
        2: [(Volume(0, (0,0,0), (5,5,5)), np.zeros((5,5,5)), dict())]
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
        2: [(Volume(0, (5,20,5), (15,30,15)), data_part, dict())]
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


def test_add_to_cache():
    cache = {
        2: [(Volume(0, (0,0,0), (5,5,5)), np.zeros((5,5,5)), dict())]
    }
    buff_volume = Volume(0, (5,5,5), (15,15,15))
    data_part = np.random.uniform(size=(10,10,10))

    vol_to_write = Volume(0, (0,0,0), (20,20,20))

    outvolume_index = 3
    add_to_cache(cache, vol_to_write, buff_volume, data_part, outvolume_index)
    assert len(cache.keys()) == 2
    l = cache[3]
    assert len(l) == 1
    e = l[0]
    vol, data, tracker = e
    assert equals(vol, vol_to_write)
    assert data.shape == (20,20,20)
    assert np.allclose(data[5:15, 5:15, 5:15], data_part)
    assert np.allclose(data[0:5, 0:5, 0:5], np.zeros(shape=(5,5,5)))

    cache = {
        2: [(Volume(0, (0,0,0), (5,5,5)), np.zeros((5,5,5)), dict())]
    }
    outvolume_index = 2
    add_to_cache(cache, vol_to_write, buff_volume, data_part, outvolume_index)
    assert len(cache.keys()) == 1
    l = cache[2]
    assert len(l) == 2


# different test cases 
@pytest.fixture(params=[
{
    "R": (1,12,12),
    "O": (1,4,4),
    "I": (1,6,6),
    "B": (1,6,6),
    "volumestokeep": [1]
},{  
    "R": (1,12,12),
    "O": (1,4,4),
    "I": (1,3,3),
    "B": (1,6,6),
    "volumestokeep": [1]
},{  
    "R": (1,12,12),
    "O": (1,4,4),
    "I": (1,3,3),
    "B": (1,6,6),
    "volumestokeep": [1,2,3]
},{
    "R": (12,12,12),
    "O": (4,4,4),
    "I": (3,3,3),
    "B": (6,6,6),
    "volumestokeep": [1,2,3,4,5,6,7]    
},{
    "R": (390,300,350),
    "O": (65,50,50),
    "I": (78,60,70),
    "B": (78,60,70),
    "volumestokeep": [1,2,3,4,5,6,7]  
},{
    "R": [390,300,350],
    "I": [78,60,70],
    "O": [65,50,50],
    "B": [1,30,70],
    "volumestokeep": [1]
},{
    "R": [390,300,350],
    "I": [78,60,70],
    "O": [65,50,50],
    "B": [39,60,70],
    "volumestokeep": [1,2,3]
},{
    "R": [390,300,350],
    "I": [39,30,35],
    "O": [65,50,50],
    "B": [39,60,70],
    "volumestokeep": [1,2,3]
},{
    "R": [390,300,350],
    "I": [78,60,70],
    "O": [78,300,70],
    "B": [39,300,70],
    "volumestokeep": [1,2,3]
}
])
def case(request):
    return request.param 


def test_keep_algorithm(case):
    # R, I, O = case
    # lambd = get_input_aggregate(O, I)
    # B = (lambd[0],lambd[1],lambd[2])
    # volumestokeep = [1,2,3]

    R, O, I, B, volumestokeep = case["R"], case["O"], case["I"], case["B"], case["volumestokeep"]

    indir_path, outdir_path, file_format = './input_dir', './output_dir', 'HDF5'
    create_empty_dir(indir_path)
    create_empty_dir(outdir_path)

    # create input array
    origarr_filepath = './original_array.hdf5'
    if os.path.isfile(origarr_filepath):
        os.remove(origarr_filepath)
    data = np.random.normal(size=R)
    fm = get_file_manager(file_format)
    fm.write(origarr_filepath, data, R, _slices=None)

    # split before resplit
    bpv = 2 # bytes per voxel
    R_size = R[0]*R[1]*R[2]*bpv
    clustered_writes(origarr_filepath, R, I, bpv, R_size, file_format, indir_path)

    keep_algorithm(R, O, I, B, volumestokeep, file_format, outdir_path, indir_path)
    assert verify_results(outdir_path, origarr_filepath, R, O, file_format)