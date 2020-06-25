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
    vol_to_write_O_basis = to_basis(vol_to_write, outvolume)
    slices = vol_to_write_O_basis.get_slices()

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


def to_basis(v, basis):
    """ Create a new volume from volume v with basis changed from R to basis

    Arguments: 
    ----------
        v: Volume obj
        basis: Volume obj
    """
    v2 = Volume(0, v.p1, v.p2)
    offset = ((-1) * basis.p1[0], (-1) * basis.p1[1], (-1) * basis.p1[2])
    v2.add_offset(offset)

    # sanity check
    p1, p2 = v2.get_corners()
    for p in [p1, p2]:
        for e in p:
            if e < 0:
                print("Volume in basis R:")
                v.print()
                print("Basis:")
                basis.print()
                raise ValueError("An error occured while changing from basis R to new basis")
    return v2


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
        pair = get_overlap_subarray(buffer, involume)  # overlap in R
        p1, p2 = tuple(pair[0]), tuple(pair[1])

        # convert overlap in basis of I for reading
        intersection_in_R = Volume(involume.index, p1, p2)
        intersection_read = to_basis(intersection_in_R, involume)

        # get infile 3d position, get slices to read from overlap volume, read data
        i, j, k = numeric_to_3d_pos(involume.index, get_partition(R, I), order='C')
        slices = intersection_read.get_slices()
        data_part = file_manager.read_data(i, j, k, input_dirpath, slices)

        data[intersection_in_R] = data_part
    return data


def equals(v1, v2):
    """ Test if two volumes have same coordinates and shape
    """
    # test coords
    p1, p2 = v1.get_corners()
    p3, p4 = v2.get_corners()
    if p1 != p3:
        return False 
    if p2 != p4:
        return False

    # test shape
    overlap = get_overlap_volume(v1, v2)
    overlap_shape = overlap.get_shape()
    if v1.get_shape() != overlap_shape:
        return False
    if v2.get_shape() != overlap_shape:
        return False
    
    return True


def add_to_cache(cache, vol_to_write, buff_volume, data_part, outvolume_index, overlap_vol_in_R):
    """
    cache: 
    ------
        key = outfile index
        value = (volumetowrite, array)
        array has shape volumetowrite, missing parts are full of zeros
    """

    def write_in_arr(array, data_part, overlap_volume):
        """ Write part of vol_to_write into cache
        Arguments: 
        ----------
            array: receiver
            data_part: what to write
            overlap_volume: where to write
        """
        s = overlap_volume.get_slices()
        array[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]] = data_part
        return array 


    # add list in cache for outfile index if nothing from this file in cache yet
    if not outvolume_index in cache.keys():
        cache[outvolume_index] = list()

    overlap_volume = to_basis(overlap_vol_in_R, vol_to_write)
    stored_data_list = cache[outvolume_index]  # get list of outfile parts partially loaded in cache

    # if cache already contains part of the outfile part, we add data to it 
    for element in stored_data_list:
        volume, array, tracker = element

        if equals(vol_to_write, volume):
            array = write_in_arr(array, data_part, overlap_volume)
            tracker.add_volume(overlap_volume)
            element = (volume, array, tracker)  # update element
            return 

    # add new element
    array = write_in_arr(np.zeros(vol_to_write.get_shape()), data_part, overlap_volume)
    tracker = Tracker()
    tracker.add_volume(overlap_volume)
    stored_data_list.append((vol_to_write, array, tracker))
    cache[outvolume_index] = stored_data_list


def get_overlap_volume(v1, v2):
    pair = get_overlap_subarray(v1, v2)  # overlap coordinates in basis of R
    p1, p2 = tuple(pair[0]), tuple(pair[1])
    return Volume(0, p1, p2)


def get_data_to_write(vol_to_write, buff_volume, data_part):
    """ get intersection between the buffer volume and the volume to write into outfile
    """
    v1 = get_overlap_volume(vol_to_write, buff_volume) 
    v2 = to_basis(v1, buff_volume)
    s = v2.get_slices()
    return v1, data_part[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]]


def complete(cache, vol_to_write, outvolume_index):
    """ Test if a volume to write is complete in cache i.e. can be written
    """
    l = cache[outvolume_index]
    for e in l:
        v, a, tracker = e 
        if equals(vol_to_write, v):
            if tracker.is_complete(vol_to_write.get_shape()): 
                return True, a
    return False, None


# optimisation possible: stop la boucle quand tout buff_volume a été process -> ac un tracker
# optimisation possible: remove vol_to_write from arrays_dict when written
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

            for j, vol_to_write in enumerate(vols_to_write):  
                
                for buff_volume, data_part in data.items():
                    print("Treating buff_volume")
                    buff_volume.print()

                    if hypercubes_overlap(buff_volume, vol_to_write):
                        data_to_write_vol, data_to_write = get_data_to_write(vol_to_write, buff_volume, data_part)

                        if equals(vol_to_write, data_to_write_vol):   
                            write_in_outfile(data_to_write, vol_to_write, file_manager, outdir_path, outvolume, O, outfiles_partition, cache, False)
                            print("Writing ", vol_to_write.p1, " ", vol_to_write.p2 ," in ", outvolume_index)
                            vols_written.append(j)

                        else:
                            add_to_cache(cache, vol_to_write, buff_volume, data_to_write, outvolume.index, data_to_write_vol)
                            is_complete, arr = complete(cache, vol_to_write, outvolume.index)

                            if is_complete:
                                write_in_outfile(arr, vol_to_write, file_manager, outdir_path, outvolume, O, outfiles_partition, cache, True)
                                print("writing ", vol_to_write.p1, " ", vol_to_write.p2 ," in ", outvolume_index)
                                vols_written.append(j)
                        
            for j in vols_written:
                del vols_to_write[j]
            arrays_dict[outvolume.index] = vols_to_write
                
    return tpp