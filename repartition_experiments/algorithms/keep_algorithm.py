import math
from .policy import compute_zones
from .utils import get_partition, get_named_volumes, get_overlap_subarray


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
        

def get_volumes(R, B):
    """ Returns a dictionary mapping each buffer (numeric) index to a Volume object containing its coordinates in R.

    Arguments: 
    ----------
        R: original array
        B: buffer shape
    """
    buffers_partition = get_partition(R, B)
    return get_named_volumes(buffers_partition, B)


def get_buffers_to_infiles(buffers, involumes):
    """ Returns a dictionary mapping each buffer (numeric) index to the list of input files from which it needs to load data.
    """
    buffers_to_infiles = dict()

    for buffer_index, buffer_volume in buffer.items():
        buffers_to_infiles[buffer_index] = list()
        for involume in involumes.values():
            if hypercubes_overlap(buffer_volume, involume):
                buffers_to_infiles[buffer_index] = involume.index

    return buffers_to_infiles


def read_buffer(buffer, buffers_to_infiles, involumes, file_manager, input_dirpath):
    """ Read a buffer from several input files.

    Arguments: 
    ----------
        buffer: the buffer to read
        buffers_to_infiles: dict associating a buffer index to the input files it has to read
        involumes: dict associating a input file index to its Volume object
        file_manager: used to actually read


    Returns:
    --------
        data: dict, 
            - associate Volume object to data part loaded,
            - Volume.index:  index of input file containing the data loaded,
            - Volume.corners(): corners of volume in basis of R

    """
    involumes_list = buffers_to_infiles[buffer.index]
    data = dict()

    for involume_index in involumes_list:
        involume = involumes[involume_index]
        p1, p2 = get_overlap_subarray(buffer, involume)

        # create Volume for intersection in basis of input file for reading
        intersection = Volume(involume.index, p1, p2)
        intersection_read = Volume(0, p1, p2)
        offset = ((-1) * involume.p1[0], (-1) * involume.p1[1], (-1) * involume.p1[2])
        intersection_read.add_offset(offset)

        # read from infile
        i, j, k = numeric_to_3d_pos(involume.index, get_partition(R, I), order='C')
        p1, p2 = intersection_read.p1, intersection_read.p2
        slices = ((p1[0], p2[0]), (p1[1], p2[1]), (p1[2], p2[2]))
        data_part = file_manager.read_data(i, j, k, input_dirpath, slices)

        data[intersection] = data_part
    return data


def equals(vol_to_write, buff_volume):
    """ See if a buffer's volume is a complete volume to write;
    If not, then some data is missing so we will store the volume read into a cache for later use.
    """
    overlap = get_overlap_subarray(vol_to_write, buff_volume)
    vol_to_write_shape = (vol_to_write.p2[0] - vol_to_write.p1[0], vol_to_write.p2[1] - vol_to_write.p1[1], vol_to_write.p2[2] - vol_to_write.p1[2])
    overlap_shape = (overlap.p2[0] - overlap.p1[0], overlap.p2[1] - overlap.p1[1], overlap.p2[2] - overlap.p1[2])

    for i in range(len(overlap_shape)):
        if overlap_shape[i] != vol_to_write_shape[i]:
            return False 
    
    return True



def keep_algorithm(R, O, I, B, volumestokeep):
    """
    """
    arrays_dict, regions_dict, buffer_to_outfiles = compute_zones(B, O, R, volumestokeep)
    buffers = get_volumes(R, B)
    involumes = get_volumes(R, I)
    outvolumes = get_volumes(R, O)
    buffers_to_infiles = get_buffers_to_infiles(buffers, involumes)

    for buffer in buffers:
        data = read_buffer(buffer, buffers_to_infiles, involumes)

        for outvolume_index in buffer_to_outfiles[buffer_index]:
            outvolume = outvolumes[outvolume_index]

            for vol_to_write in arrays_dict[outvolume.index]:  # TODO: remove vol_to_write from arrays_dict when written
                
                for buff_volume, data_part in data.items():
                    if equals(vol_to_write, buff_volume):      
                        write_in_outfile(False)
                    else:
                        add_to_cache(cache, vol_to_write, buff_volume, outvolume.index)

                        if complete(cache, vol_to_write, outvolume.index):
                            write_in_outfile(True)

