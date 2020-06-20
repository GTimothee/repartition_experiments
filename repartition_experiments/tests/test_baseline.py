import os, pytest

from ..algorithms.baseline_algorithm import baseline_rechunk
from ..utils import create_input_chunks


@pytest.fixture(params=[
    ((1,120,120), (1,60,60), (1,40,40)), 
    ((120,120,120), (60,60,60), (40,40,40))
])
def case(request):
    return request.param 


def test_baseline(case):
    R, I, O = (1,120,120), (1,60,60), (1,40,40)
    indir_path, outdir_path, file_format = './input_dir', './output_dir', 'HDF5'

    if not os.path.isdir(indir_path):
        os.mkdir(indir_path)
    if not os.path.isdir(outdir_path):
        os.mkdir(outdir_path)
    if not os.path.isdir(indir_path) or not os.path.isdir(outdir_path):
        raise OSError()

    create_input_chunks(I, indir_path, file_format)
    baseline_rechunk(indir_path, outdir_path, O, I, R, file_format, True)