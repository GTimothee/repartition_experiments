import math
import numpy as np
from .policy import compute_zones
from .utils import get_partition, get_named_volumes, get_overlap_subarray, get_file_manager, numeric_to_3d_pos, Volume


def get_input_aggregate(O, I):
    lambd = list()
    dimensions = len(O)
    for dim in range(dimensions):
        lambd.append(math.ceil(O[dim]/I[dim])*I[dim])
    return lambd


def remove_from_cache(cache, outfile_index, volume_to_write):
    """ Remove element from cache after it has been written
    """
    volumes_in_cache = cache[outfile_index]

    target = None
    for i, e in enumerate(volumes_in_cache):
        v, d = e
        p1, p2 = v.get_corners()
        print(p1, p2)
        print(volume_to_write.p1, volume_to_write.p2)
        if p1 == volume_to_write.p1 and p2 == volume_to_write.p2:
            target = i
            break
    
    if target == None:
        raise ValueError("Cannot remove data part from cache: data not in cache")
    else:
        del volumes_in_cache[target]
        cache[outfile_index] = volumes_in_cache


def write_in_outfile(data_part, vol_to_write, file_manager, outdir_path, outvolume, outfile_shape, outfiles_partition, cache, from_cache):
    """ Writes an output file part which is ready to be written.

    Arguments: 
    ----------
        data_part: data to write
        vol_to_write: Volume representing data_part in basis of R
        from_cache: if data has been read from cache or not. If true it simply deletes the piece of data from the cache.
        file_manager: to write the data
    """
    # find coordinates into the output file
    offset = ((-1) * outvolume.p1[0], (-1) * outvolume.p1[1], (-1) * outvolume.p1[2])
    vol_to_write_O_basis = Volume(vol_to_write.index, vol_to_write.p1, vol_to_write.p2)
    vol_to_write_O_basis.add_offset(offset)

    # get region in output file to write into
    p1, p2 = vol_to_write_O_basis.get_corners()
    slices = ((p1[0], p2[0]), (p1[1], p2[1]), (p1[2], p2[2]))

    # write
    i, j, k = numeric_to_3d_pos(outvolume.index, outfiles_partition, order='C')
    file_manager.write_data(i, j, k, outdir_path, data_part, slices, outfile_shape, dtype=np.float16)

    if from_cache:
        remove_from_cache(cache, outvolume.index, vol_to_write)
        

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


def read_buffer(buffer, buffers_to_infiles, involumes, file_manager, input_dirpath, R, I):
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
        pair = get_overlap_subarray(buffer, involume)
        p1, p2 = tuple(pair[0]), tuple(pair[1])

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
    pair = get_overlap_subarray(vol_to_write, buff_volume)
    p1, p2 = tuple(pair[0]), tuple(pair[1])
    overlap = Volume(0, p1, p2)

    vol_to_write_shape = (vol_to_write.p2[0] - vol_to_write.p1[0], vol_to_write.p2[1] - vol_to_write.p1[1], vol_to_write.p2[2] - vol_to_write.p1[2])
    overlap_shape = (overlap.p2[0] - overlap.p1[0], overlap.p2[1] - overlap.p1[1], overlap.p2[2] - overlap.p1[2])

    for i in range(len(overlap_shape)):
        if overlap_shape[i] != vol_to_write_shape[i]:
            return False 
    
    return True


def add_to_cache(cache, vol_to_write, buff_volume, data_part, outvolume_index):
    """
    cache: 
    ------
        key = outfile index
        value = (volumetowrite, array)
        array has shape volumetowrite, missing parts are full of zeros
    """
    if not outvolume_index in cache.keys():
        cache[outvolume_index] = list()

    stored_data = cache[outvolume_index]
    for element in stored_data:
        volume, array = element
        if equals(vol_to_write, volume):
            insert(array, buff_volume)
            element = (volume, array)
            return 

    # if reached, vol_to_write not in cache -> add to cache
    shape = (vol_to_write.p2[0] - vol_to_write.p1[0], vol_to_write.p2[1] - vol_to_write.p1[1], vol_to_write.p2[2] - vol_to_write.p1[2])
    array = np.zeros(shape)

    pair = get_overlap_subarray(vol_to_write, buff_volume)
    p1, p2 = tuple(pair[0]), tuple(pair[1])

    offset = ((-1) * vol_to_write.p1[0], (-1) * vol_to_write.p1[1], (-1) * vol_to_write.p1[2])
    overlap_volume = Volume(0, p1, p2)
    overlap_volume.add_offset(offset)
    p1, p2 = overlap_volume.get_corners()
    s = ((p1[0], p2[0]), (p1[1], p2[1]), (p1[2], p2[2]))
    array[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]] = data_part
    stored_data.append((vol_to_write, array))


def keep_algorithm(R, O, I, B, volumestokeep, file_format):
    """
        cache: dict,
            outfile_index -> list of volumes to write 
            when a volume to write' part is added into cache: create a zero array of shape volume to write, then write the part that has been loaded
            when searching for a part: search for outfile index, then for the right volume
    """
    arrays_dict, buffer_to_outfiles = compute_zones(B, O, R, volumestokeep)
    buffers = get_volumes(R, B)
    involumes = get_volumes(R, I)
    outvolumes = get_volumes(R, O)
    buffers_to_infiles = get_buffers_to_infiles(buffers, involumes)
    file_manager = get_file_manager(file_format)
    cache = dict()

    for buffer in buffers:
        data = read_buffer(buffer, buffers_to_infiles, involumes)

        for buff_volume, data_part in data.items():

            for outvolume_index in buffer_to_outfiles[buffer_index]:
                outvolume = outvolumes[outvolume_index]
                vols_to_write = arrays_dict[outvolume.index]
                vols_written = list()

                for j, vol_to_write in enumerate(vols_to_write):  # TODO: remove vol_to_write from arrays_dict when written
                    
                    if equals(vol_to_write, buff_volume):      
                        write_in_outfile(data_part, vol_to_write, file_manager, outdir_path, outvolume, outfile_shape, outfiles_partition, cache, False)
                        vols_written.append(j)
                    else:
                        add_to_cache(cache, vol_to_write, buff_volume, outvolume.index)

                        if complete(cache, vol_to_write, outvolume.index):
                            write_in_outfile(data_part, vol_to_write, file_manager, outdir_path, outvolume, outfile_shape, outfiles_partition, cache, True)
                            vols_written.append(j)

                for j in vols_written:
                    del vols_to_write[j]
                arrays_dict[outvolume.index] = vols_to_write