
from ..algorithms.utils import *
from ..algorithms.policy_remake import compute_zones_remake
from ..algorithms.keep_algorithm import get_buffers_to_infiles


def add_to_cache(cache, vol_to_write, buffer_slices, outvolume_index, overlap_vol_in_R, datapart_volume):
    s = buffer_slices

    # add list in cache for outfile index if nothing for this file in cache yet
    if not outvolume_index in cache.keys():
        cache[outvolume_index] = list()

    # if cache already contains part of the outfile part, we add data to it 
    for element in cache[outvolume_index]:
        vol_to_write_tmp, volumes_list, tracker = element

        if equals(vol_to_write, vol_to_write_tmp):
            volumes_list.append(overlap_vol_in_R)
            tracker.add_volume(overlap_vol_in_R)
            element = (vol_to_write_tmp, volumes_list, tracker)  # update element
            return 

    # add new element
    volumes_list = [overlap_vol_in_R]
    tracker = Tracker()
    tracker.add_volume(overlap_vol_in_R)
    cache[outvolume_index].append((vol_to_write, volumes_list, tracker))      


def _write(vol_to_write):
    pass


def write_or_cache(outvolume, vol_to_write, buffer, cache):
    data_to_write_vol, buffer_slices = get_data_to_write(vol_to_write, buffer, data)
    volume_written = False
    data_moved = 0
    
    if equals(vol_to_write, data_to_write_vol):  
        _write(vol_to_write) 
        volume_written = True

    else:
        add_to_cache(cache, vol_to_write, buffer_slices, outvolume.index, data_to_write_vol, buffer)
        is_complete, arr = complete(cache, vol_to_write, outvolume.index)
        
        if is_complete:
            _write(vol_to_write) 
            del arr
            volume_written = True

    return volume_written


def read_buffer(buffer, buffers_to_infiles, involumes, R, I):
    involumes_list = buffers_to_infiles[buffer.index]
    nb_opening_seeks_tmp = 0
    nb_inside_seeks_tmp = 0

    for involume_index in involumes_list:
        involume = involumes[involume_index]
        pair = get_overlap_subarray(buffer, involume)  # overlap in R
        p1, p2 = tuple(pair[0]), tuple(pair[1])

        # convert overlap in basis of I for reading
        intersection_in_R = Volume(involume.index, p1, p2)
        intersection_read = to_basis(intersection_in_R, involume)

        s = intersection_read.get_shape()
        if s[2] != I[2]:
            nb_inside_seeks_tmp += s[0]*s[1]
        elif s[1] != I[1]:
            nb_inside_seeks_tmp += s[0]
        elif s[0] != I[0]:
            nb_inside_seeks_tmp += 1
        else:
            pass
        nb_opening_seeks_tmp += 1

    return nb_opening_seeks_tmp, nb_inside_seeks_tmp


def process_buffer(arrays_dict, buffers, buffer, buffers_to_infiles, buffer_to_outfiles, cache, involumes):
    data_shape = buffer.get_shape()
    buffer_size = data_shape[0] * data_shape[1] * data_shape[2]

    nb_opening_seeks_tmp, nb_inside_seeks_tmp = read_buffer(buffer, buffers_to_infiles, involumes, R, I)

    # below is not useful for computing seeks
    # # looping over the output blocks related to the current read buffer
    # for outvolume_index in buffer_to_outfiles[buffer.index]:
    #     outfile_parts_written = list()

    #     # for each part of output file (for each write buffer)
    #     for j, outfile_part in enumerate(arrays_dict[outvolumes[outvolume_index].index]):  
            
    #         # for each part of read buffer
    #         if hypercubes_overlap(buffer, outfile_part):

    #             # write write buffer if complete, or put part of write bufer into the cache
    #             volume_written = write_or_cache(outvolumes[outvolume_index], outfile_part, buffer, cache)
                
    #             if volume_written:
    #                 outfile_parts_written.append(j)

    #     # if part of output file entirely processed, remove from arrays_dict
    #     for j in outfile_parts_written:
    #         del arrays_dict[outvolumes[outvolume_index].index][j]

    return nb_opening_seeks_tmp, nb_inside_seeks_tmp


def keep_algorithm(arg_R, arg_O, arg_I, arg_B, volumestokeep):
    global R, O, I, B
    R, O, I, B = tuple(arg_R), tuple(arg_O), tuple(arg_I), tuple(arg_B)
    buffers_partition, buffers = get_volumes(R, B)
    infiles_partition, involumes = get_volumes(R, I)
    outfiles_partition, outvolumes = get_volumes(R, O)

    arrays_dict, buffer_to_outfiles, nb_outfile_openings, nb_outfile_inside_seeks = compute_zones_remake(B, O, R, volumestokeep, outfiles_partition, outvolumes, buffers, True)
    buffers_to_infiles = get_buffers_to_infiles(buffers, involumes) # same than buffer_to_outfiles for input blocks

    cache = dict()
    nb_infile_openings = 0
    nb_infile_inside_seeks = 0
    nb_buffers = len(buffers.keys())

    buffer_shape = buffers[0].get_shape()
    buffer_size = buffer_shape[0] * buffer_shape[1] * buffer_shape[2] * 2

    for buffer_index in range(nb_buffers):
        buffer = buffers[buffer_index]
        # print("\nBUFFER ", buffer_index, '/', nb_buffers)
            
        nb_opening_seeks_tmp, nb_inside_seeks_tmp = process_buffer(arrays_dict, buffers, buffer, buffers_to_infiles, buffer_to_outfiles, cache, involumes)
        nb_infile_openings += nb_opening_seeks_tmp
        nb_infile_inside_seeks += nb_inside_seeks_tmp

    return [nb_outfile_openings, nb_outfile_inside_seeks, nb_infile_openings, nb_infile_inside_seeks]
