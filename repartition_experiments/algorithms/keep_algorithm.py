import math, time
import numpy as np
from .policy import compute_zones
from .utils import get_partition, get_named_volumes, get_overlap_subarray, get_file_manager, numeric_to_3d_pos, Volume, hypercubes_overlap, included_in
from .tracker import Tracker

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
        v, d, tracker = e
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
    # get region in output file to write into
    offset = ((-1) * outvolume.p1[0], (-1) * outvolume.p1[1], (-1) * outvolume.p1[2]) # find coordinates into the output file
    vol_to_write_O_basis = Volume(vol_to_write.index, vol_to_write.p1, vol_to_write.p2)
    vol_to_write_O_basis.add_offset(offset)
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
    return buffers_partition, get_named_volumes(buffers_partition, B)


def get_buffers_to_infiles(buffers, involumes):
    """ Returns a dictionary mapping each buffer (numeric) index to the list of input files from which it needs to load data.
    """
    buffers_to_infiles = dict()

    for buffer_index, buffer_volume in buffers.items():
        buffers_to_infiles[buffer_index] = list()
        for involume in involumes.values():
            if hypercubes_overlap(buffer_volume, involume):
                buffers_to_infiles[buffer_index].append(involume.index)

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


def equals(v1, v2):

    p1, p2 = v1.get_corners()
    p3, p4 = v2.get_corners()
    if p1 != p3:
        return False 
    if p2 != p4:
        return False

    pair = get_overlap_subarray(v1, v2)
    p1, p2 = tuple(pair[0]), tuple(pair[1])

    overlap = Volume(0, p1, p2)
    shape1 = (v1.p2[0] - v1.p1[0], v1.p2[1] - v1.p1[1], v1.p2[2] - v1.p1[2])
    shape2 = (v2.p2[0] - v2.p1[0], v2.p2[1] - v2.p1[1], v2.p2[2] - v2.p1[2])
    overlap_shape = (overlap.p2[0] - overlap.p1[0], overlap.p2[1] - overlap.p1[1], overlap.p2[2] - overlap.p1[2])

    if shape1 != overlap_shape:
        return False
    if shape2 != overlap_shape:
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
        volume, array, tracker = element

        if equals(vol_to_write, volume):
            pair = get_overlap_subarray(vol_to_write, buff_volume)

            p1, p2 = tuple(pair[0]), tuple(pair[1])
            offset = ((-1) * vol_to_write.p1[0], (-1) * vol_to_write.p1[1], (-1) * vol_to_write.p1[2])
            overlap_volume = Volume(0, p1, p2)
            overlap_volume.add_offset(offset)

            print("adding ", overlap_volume.p1, " ", overlap_volume.p2 ," to cache")
            tracker.add_volume(overlap_volume)

            p1, p2 = overlap_volume.get_corners()
            s = ((p1[0], p2[0]), (p1[1], p2[1]), (p1[2], p2[2]))
            print(s)
            array[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]] = data_part

            element = (volume, array, tracker)
            return 

    # if reached, vol_to_write not in cache -> add to cache
    shape = (vol_to_write.p2[0] - vol_to_write.p1[0], vol_to_write.p2[1] - vol_to_write.p1[1], vol_to_write.p2[2] - vol_to_write.p1[2])
    array = np.zeros(shape)
    tracker = Tracker()

    # print("getting subarr")
    pair = get_overlap_subarray(vol_to_write, buff_volume)
    p1, p2 = tuple(pair[0]), tuple(pair[1])
    offset = ((-1) * vol_to_write.p1[0], (-1) * vol_to_write.p1[1], (-1) * vol_to_write.p1[2])
    
    overlap_volume = Volume(0, p1, p2)
    # print("treating volume ", overlap_volume.p1, " ", overlap_volume.p2 ," for cache")
    overlap_volume.add_offset(offset)

    # print("adding ", overlap_volume.p1, " ", overlap_volume.p2 ," to cache")
    tracker.add_volume(overlap_volume)

    p1, p2 = overlap_volume.get_corners()
    s = ((p1[0], p2[0]), (p1[1], p2[1]), (p1[2], p2[2]))
    # print(s, ' (2)')
    array[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]] = data_part

    stored_data.append((vol_to_write, array, tracker))


def get_data_to_write(vol_to_write, buff_volume, data_part):
    """ get intersection between the buffer volume and the volume to write into outfile
    """
    # get data part to read
    pair = get_overlap_subarray(buff_volume, vol_to_write)  # overlap coordinates in basis of R
    p1, p2 = tuple(pair[0]), tuple(pair[1])
    
    # convert pair in basis of buff_volume to extract data of interest from data_part
    offset = ((-1) * buff_volume.p1[0], (-1) * buff_volume.p1[1], (-1) * buff_volume.p1[2]) 
    v1 = Volume(0, p1, p2)
    v2 = Volume(0, p1, p2)
    v2.add_offset(offset)
    p1, p2 = v2.get_corners()
    s = ((p1[0], p2[0]), (p1[1], p2[1]), (p1[2], p2[2]))
    return v1, data_part[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]]


def complete(cache, vol_to_write, outvolume_index):
    """ Test if a volume to write is complete in cache i.e. can be written
    """
    l = cache[outvolume_index]
    for e in l:
        v, a, tracker = e 
        if equals(vol_to_write, v):
            v_shape = (vol_to_write.p2[0] - vol_to_write.p1[0], vol_to_write.p2[1] - vol_to_write.p1[1], vol_to_write.p2[2] - vol_to_write.p1[2])
            if tracker.is_complete(v_shape): 
                return True, a
    return False, None


def keep_algorithm(R, O, I, B, volumestokeep, file_format, outdir_path, input_dirpath):
    """
        cache: dict,
            outfile_index -> list of volumes to write 
            when a volume to write' part is added into cache: create a zero array of shape volume to write, then write the part that has been loaded
            when searching for a part: search for outfile index, then for the right volume
    """
    import logging
    import logging.config
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
    })

    buffers_partition, buffers = get_volumes(R, B)
    infiles_partition, involumes = get_volumes(R, I)
    outfiles_partition, outvolumes = get_volumes(R, O)

    tpp = time.time()
    arrays_dict, buffer_to_outfiles = compute_zones(B, O, R, volumestokeep, buffers_partition, outfiles_partition, buffers, outvolumes)
    buffers_to_infiles = get_buffers_to_infiles(buffers, involumes)
    tpp = time.time() - tpp
    print("Preprocessing time: ", tpp)

    file_manager = get_file_manager(file_format)
    cache = dict()

    print("------------")

    for buffer_index, buffer in buffers.items():
        data = read_buffer(buffer, buffers_to_infiles, involumes, file_manager, input_dirpath, R, I)

        print("processing buffer ", buffer_index)

        for outvolume_index in buffer_to_outfiles[buffer_index]:
            
            print("buffer ", buffer_index, " overlaps with outfile ", outvolume_index)

            outvolume = outvolumes[outvolume_index]
            vols_to_write = arrays_dict[outvolume.index]
            vols_written = list()

            for j, vol_to_write in enumerate(vols_to_write):  # TODO: remove vol_to_write from arrays_dict when written
                
                for buff_volume, data_part in data.items():
                    print("treating buff_volume")
                    buff_volume.print()

                    if hypercubes_overlap(buff_volume, vol_to_write):
                        v, data_to_write = get_data_to_write(vol_to_write, buff_volume, data_part)

                        if equals(vol_to_write, v):   
                            write_in_outfile(data_to_write, vol_to_write, file_manager, outdir_path, outvolume, O, outfiles_partition, cache, False)
                            print("writing ", vol_to_write.p1, " ", vol_to_write.p2 ," in ", outvolume_index)
                            vols_written.append(j)
                        else:
                            
                            add_to_cache(cache, vol_to_write, buff_volume, data_to_write, outvolume.index)

                            is_complete, arr = complete(cache, vol_to_write, outvolume.index)
                            if is_complete:
                                write_in_outfile(arr, vol_to_write, file_manager, outdir_path, outvolume, O, outfiles_partition, cache, True)
                                print("writing ", vol_to_write.p1, " ", vol_to_write.p2 ," in ", outvolume_index)
                                vols_written.append(j)
                        
            for j in vols_written:
                del vols_to_write[j]
            arrays_dict[outvolume.index] = vols_to_write
                
    # optimisation possible: stop la boucle quand tout buff_volume a été process