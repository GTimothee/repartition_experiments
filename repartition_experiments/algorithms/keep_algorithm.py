import math
from .policy import compute_zones
from .utils import get_partition, get_named_volumes


def get_input_aggregate(O, I):
    lambd = list()
    dimensions = len(O)
    for dim in range(dimensions):
        lambd.append(math.ceil(O[dim]/I[dim])*I[dim])
    return lambd


def write_in_outfile(from_cache):
    region = get_region(regions_dict, outvolume.index, v)
    write(v)
    remove(v, arrays_dict)
    remove(bv, buff_vols)

    if from_cache:
        remove(cache, v, outvolume.index)
        

def get_buffers(R, B):
    """ Returns a dictionary mapping each buffer (numeric) index to a Volume object containing its coordinates in R.

    Arguments: 
    ----------
        R: original array
        B: buffer shape
    """
    buffers_partition = get_partition(R, B)
    return get_named_volumes(buffers_partition, B)


def keep_algorithm(R, O, I, B, volumestokeep):
    arrays_dict, regions_dict, buffer_to_outfiles = compute_zones(B, O, R, volumestokeep)
    buffers = get_buffers(R, B)

    for buffer in buffers:
        data = read(buffer)
        buff_vols = _break(data)

        for outvolume in buffer_to_outfiles[buffer_index]:
            for v in arrays_dict[outvolume.index]:
                for bv in buff_vols:
                    if intersection(v, bv) == 'complete':      
                        write_in_outfile(False)
                    else:
                        add_to_cache(cache, v, bv, outvolume.index)

                        if complete(cache, v, outvolume.index):
                            write_in_outfile(True)

