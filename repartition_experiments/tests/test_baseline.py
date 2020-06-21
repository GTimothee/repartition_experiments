import pytest

from ..algorithms.baseline_algorithm import baseline_rechunk
from ..exp_utils import create_input_chunks, create_empty_dir


# different test cases 
@pytest.fixture(params=[
    ((1,12,12), (1,6,6), (1,4,4)), 
    ((12,12,12), (6,6,6), (4,4,4))
])
def case(request):
    return request.param 


def test_baseline(case):
    R, I, O = case
    indir_path, outdir_path, file_format = './input_dir', './output_dir', 'HDF5'

    create_empty_dir(indir_path)
    create_empty_dir(outdir_path)

    create_input_chunks(I, indir_path, file_format)
    baseline_rechunk(indir_path, outdir_path, O, I, R, file_format, True)