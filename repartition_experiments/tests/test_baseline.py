import pytest, os
import numpy as np

from ..algorithms.baseline_algorithm import baseline_rechunk
from ..exp_utils import create_input_chunks, create_empty_dir, verify_results
from ..algorithms.utils import get_file_manager
from ..algorithms.clustered_writes import clustered_writes


# different test cases 
@pytest.fixture(params=[
    ((1,12,12), (1,6,6), (1,4,4)), 
    ((1,12,12), (1,3,3), (1,4,4)), 
    ((12,12,12), (6,6,6), (4,4,4)),
    ((12,12,12), (3,3,3), (4,4,4)),
    ((390,300,350), (78,60,70), (65,50,50)),
    ((390,300,350), (65,50,50), (78,60,70)),
    ((390,300,350), (78,60,70), (78,300,70)),
])
def case(request):
    return request.param 


def test_baseline(case):
    ff  = 'HDF5'  # file format
    bpv = 2 # bytes per voxel
    R, I, O = case
    indir_path, outdir_path, file_format = './input_dir', './output_dir', 'HDF5'

    create_empty_dir(indir_path)
    create_empty_dir(outdir_path)

    # create input array
    origarr_filepath = './original_array.hdf5'
    if os.path.isfile(origarr_filepath):
        os.remove(origarr_filepath)
    data = np.random.normal(size=R)
    fm = get_file_manager(ff)
    fm.write(origarr_filepath, data, R, _slices=None)

    m = R[0]*R[1]*R[2]*bpv
    clustered_writes(origarr_filepath, R, I, bpv, m, ff, indir_path)  # split
    baseline_rechunk(indir_path, outdir_path, O, I, R, file_format, True, clean_out_dir=False)  # rechunk

    assert verify_results(outdir_path, origarr_filepath, R, O, file_format)