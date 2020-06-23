import numpy as np

from ..algorithms.keep_algorithm import *
from ..algorithms.utils import Volume

def test_remove_from_cache():
    cache = {
        2: [(Volume(0, (0,0,0), (5,5,5)), np.zeros((5,5,5)))]
    }
    outfile_index = 2
    volume_to_write = Volume(0, (0,0,0), (5,5,5))
    remove_from_cache(cache, outfile_index, volume_to_write)

    assert len(cache[outfile_index]) == 0


# def test_write_in_outfile():
#     write_in_outfile(data_part, vol_to_write, file_manager, outdir_path, outvolume, outfile_shape, outfiles_partition, cache, from_cache)


# def test_read_buffer():
#     read_buffer(buffer, buffers_to_infiles, involumes, file_manager, input_dirpath)


# def test_equals():
#     equals(vol_to_write, buff_volume)


# def test_add_to_cache():
#     add_to_cache(cache, vol_to_write, buff_volume, data_part, outvolume_index)


# def test_keep_algorithm():
#     keep_algorithm(R, O, I, B, volumestokeep, file_format)