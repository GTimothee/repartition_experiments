import math, time, csv, psutil, sys
import numpy as np
from repartition_experiments.algorithms.policy import compute_zones
from repartition_experiments.algorithms.utils import get_partition, get_named_volumes, get_overlap_subarray, get_file_manager, numeric_to_3d_pos, Volume, hypercubes_overlap, included_in, get_volumes, to_basis
from repartition_experiments.algorithms.tracker import Tracker
from repartition_experiments.algorithms.utils import get_opened_files
from repartition_experiments.algorithms.voxel_tracker import VoxelTracker
import gc

def get_input_aggregate(O, I):
    lambd = list()
    dimensions = len(O)
    for dim in range(dimensions):
        lambd.append(math.ceil(O[dim]/I[dim])*I[dim])
    return tuple(lambd)


def remove_from_cache(cache, outfile_index, volume_to_write):
    """ Remove element from cache after it has been written
    """
    volumes_in_cache = cache[outfile_index]

    target = None
    for i, e in enumerate(volumes_in_cache):
        v, _, _, _ = e
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
    t2 = time.time()
    empty_dataset = file_manager.write_data(i, j, k, outdir_path, data_part, slices, outfile_shape)
    t2 = time.time() - t2

    if from_cache:
        remove_from_cache(cache, outvolume.index, vol_to_write)

    return t2, empty_dataset


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

    t1 = 0 
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

        # get infile 3d position, get slices to read from overlap volume, read data
        i, j, k = numeric_to_3d_pos(involume.index, get_partition(R, I), order='C')
        slices = intersection_read.get_slices()

        # if intersection_read.get_shape() == I:
        #     filepath = file_manager.get_filepath(i, j, k, input_dirpath)
        #     t_tmp = time.time()
        #     data_part = file_manager.read_all(filepath)
        #     t1 += time.time() - t_tmp
        # else:
        t_tmp = time.time()
        data[intersection_in_R] = (file_manager.read_data(i, j, k, input_dirpath, slices), Tracker())
        t1 += time.time() - t_tmp

    return data, t1, nb_opening_seeks_tmp, nb_inside_seeks_tmp


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


def add_to_cache(cache, vol_to_write, data_part, outvolume_index, overlap_vol_in_R):
    """
    cache: 
    ------
        key = outfile index
        value = [(volumetowrite, array),...]
        array has shape volumetowrite, missing parts are full of zeros
    """

    # add list in cache for outfile index if nothing for this file in cache yet
    if not outvolume_index in cache.keys():
        cache[outvolume_index] = list()

    stored_data_list = cache[outvolume_index]  # get list of outfile parts partially loaded in cache

    # if cache already contains part of the outfile part, we add data to it 
    for element in stored_data_list:
        vol_to_write_tmp, volumes_list, arrays_list, tracker = element

        if equals(vol_to_write, vol_to_write_tmp):
            volumes_list.append(overlap_vol_in_R)
            arrays_list.append(data_part)
            tracker.add_volume(overlap_vol_in_R)
            element = (vol_to_write_tmp, volumes_list, arrays_list, tracker)  # update element
            return 

    # add new element
    arrays_list = [data_part]
    volumes_list = [overlap_vol_in_R]
    tracker = Tracker()
    tracker.add_volume(overlap_vol_in_R)
    stored_data_list.append((vol_to_write, volumes_list, arrays_list, tracker))
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
        v, v_list, a_list, tracker = e 
        if equals(vol_to_write, v):
            if tracker.is_complete(vol_to_write.get_corners()): 
                arr = np.empty(vol_to_write.get_shape(), dtype=np.float16)
                for v_tmp, a_tmp in zip(v_list, a_list):
                    s = to_basis(v_tmp, vol_to_write).get_slices()
                    arr[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]] = a_tmp
                return True, arr
    return False, None


def print_mem_info():
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    used_ram = (mem.total - mem.available) /1024 /1024
    used_swap = swap.used /1024 /1024 
    print("Used RAM: ", used_ram, "MB")
    print("Used swap: ", used_swap, "MB")


def keep_algorithm(R, O, I, B, volumestokeep, file_format, outdir_path, input_dirpath, addition, sanity_check=False):
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

    R, O, I, B = tuple(R), tuple(O), tuple(I), tuple(B)

    buffers_partition, buffers = get_volumes(R, B)
    infiles_partition, involumes = get_volumes(R, I)
    outfiles_partition, outvolumes = get_volumes(R, O)

    print("Preprocessing...")
    tpp = time.time()
    arrays_dict, buffer_to_outfiles, nb_outfile_openings, nb_outfile_inside_seeks = compute_zones(B, O, R, volumestokeep, buffers_partition, outfiles_partition, buffers, outvolumes)
    buffers_to_infiles = get_buffers_to_infiles(buffers, involumes)
    tpp = time.time() - tpp
    print("Preprocessing time: ", tpp)

    file_manager = get_file_manager(file_format)
    cache = dict()

    print("------------")

    read_time = 0
    write_time = 0

    nb_infile_openings = 0
    nb_infile_inside_seeks = 0

    if sanity_check:
        written_shapes = list()
        nb_oneshot_writes = 0
        volumes_written = list()
        nb_file_initializations = 0
        nb_volumes_written = 0

        nb_volumes_to_write = 0
        for k, v in arrays_dict.items():
            nb_volumes_to_write += len(v)

        outvolumes_trackers = dict()
        for index, outvol in outvolumes.items():
            outvolumes_trackers[index] = Tracker()
    
    print("Starting monitor... Memory status: \n")
    print_mem_info()

    voxel_tracker = VoxelTracker()
    from monitor.monitor import Monitor
    _monitor = Monitor(enable_print=False, enable_log=False, save_data=True)
    _monitor.disable_clearconsole()
    _monitor.set_delay(5)
    _monitor.start()
    nb_buffers = len(buffers.keys())
    print("nb buffers:", nb_buffers)
    for buffer_index in range(nb_buffers):
        print("BUFFER ", buffer_index, '/', nb_buffers)
        buffer = buffers[buffer_index]
        
        data, t1, nb_opening_seeks_tmp, nb_inside_seeks_tmp = read_buffer(buffer, buffers_to_infiles, involumes, file_manager, input_dirpath, R, I)
        data_shape = buffer.get_shape()
        print("[read] buffer of shape : ", data_shape)
        print_mem_info()

        buffer_size = data_shape[0]*data_shape[1]*data_shape[2]
        voxel_tracker.add_voxels(buffer_size)
        data_movement = 0
        
        read_time += t1
        nb_infile_openings += nb_opening_seeks_tmp
        nb_infile_inside_seeks += nb_inside_seeks_tmp

        for outvolume_index in buffer_to_outfiles[buffer_index]:
    
            outvolume = outvolumes[outvolume_index]
            vols_to_write = arrays_dict[outvolume.index]
            vols_written = list()

            for j, vol_to_write in enumerate(vols_to_write):  
                
                all_keys = list(data.keys())
                for datapart_volume in all_keys:
                    data_part, writing_tracker = data[datapart_volume]
                    # print("after getting copy of data part")
                    # print_mem_info()

                    if hypercubes_overlap(datapart_volume, vol_to_write):
                        # print("before getdatatowrite")
                        # print_mem_info()
                        data_to_write_vol, data_to_write = get_data_to_write(vol_to_write, datapart_volume, data_part)
                        # print('after get data to write')
                        # print_mem_info()

                        writing_tracker.add_volume(data_to_write_vol)
                        if writing_tracker.is_complete(datapart_volume.get_corners()):
                            del data_part
                            del writing_tracker
                            del data[datapart_volume]
                        # print("after removing data part from buffer data:")
                        # print_mem_info()

                        if equals(vol_to_write, data_to_write_vol):  
                            
                            # write
                            if addition:
                                data_to_write = data_to_write +1
                            t2, initialized = write_in_outfile(data_to_write, vol_to_write, file_manager, outdir_path, outvolume, O, outfiles_partition, cache, False)
                            # print("[write] data_to_write of shape : ", data_to_write.shape)
                            
                            # stats
                            write_time += t2
                            
                            if sanity_check:
                                assert vol_to_write.get_shape() == data_to_write_vol.get_shape() and vol_to_write.get_shape() == data_to_write.shape
                                written_shapes.append(vol_to_write.get_shape())
                                outvolumes_trackers[outvolume_index].add_volume(vol_to_write)
                                if initialized:
                                    nb_file_initializations += 1
                                nb_volumes_written += 1
                                nb_oneshot_writes += 1

                            # garbage collection
                            vols_written.append(j)

                        else:
                            # print("Memory before add to cache:\n")
                            # print_mem_info()
                            add_to_cache(cache, vol_to_write, data_to_write, outvolume.index, data_to_write_vol)
                            # print("[cache+] add data_to_write of shape : ", data_to_write.shape)
                            # print_mem_info()

                            # stats
                            tmp_s = data_to_write_vol.get_shape()
                            data_movement += tmp_s[0]*tmp_s[1]*tmp_s[2]

                            is_complete, arr = complete(cache, vol_to_write, outvolume.index)
                            if is_complete:
                                # write
                                if addition:
                                    arr = arr +1
                                t2, initialized = write_in_outfile(arr, vol_to_write, file_manager, outdir_path, outvolume, O, outfiles_partition, cache, True)
                                # print("[cache-] remove arr of shape : ", arr.shape)
                                # print_mem_info()
                                
                                # stats
                                write_time += t2
                                tmp_s = vol_to_write.get_shape()
                                data_movement -= tmp_s[0]*tmp_s[1]*tmp_s[2]

                                # garbage collection
                                vols_written.append(j)

                                if sanity_check:
                                    written_shapes.append(vol_to_write.get_shape())
                                    outvolumes_trackers[outvolume_index].add_volume(vol_to_write)
                                    volumes_written.append(vol_to_write.get_shape())
                                    if initialized:
                                        nb_file_initializations += 1
                                    nb_volumes_written += 1

                        # print("before removing data_to_write")
                        # print_mem_info()
                        del data_to_write
                        # print("after")
                        # print_mem_info()



            # print("before removing data from arrays_dict ")
            # print_mem_info()
            # garbage collection
            for j in vols_written:
                del vols_to_write[j]
            arrays_dict[outvolume.index] = vols_to_write
            # print("after")
            # print_mem_info()

        # garbage collection
        # print("before removing buffer data loaded")
        # print_mem_info()
        data.clear()
        del data
        print("after removing buffer data loaded")
        print_mem_info()

        # stats
        data_movement -= buffer_size
        voxel_tracker.add_voxels(data_movement)
        print('[tracker] end of buffer -> nb voxels:', voxel_tracker.nb_voxels)

        # to del
        file_manager.close_infiles()
        sys.exit()

    file_manager.close_infiles()

    _monitor.stop()
    ram_pile, swap_pile = _monitor.get_mem_piles()

    if sanity_check:
        print(f"number of outfiles parts written in one shot: {nb_oneshot_writes}")
        for k, tracker in outvolumes_trackers.items():
            assert tracker.is_complete(outvolumes[k].get_corners())

        # sanity check
        if not nb_volumes_written == nb_volumes_to_write:
            print("WARNING: All outfile parts have not been written")  
        else:
            print(f"Sanity check passed: All outfile parts have been written")

        # sanity check
        miss = False
        for k, v in cache.items():
            if len(v) != 0:
                miss = True
                print(f'cache for outfile {k}, not empty')
        if miss:
            print("WARNING: Cache not empty at the end of process")    
        else:
            print(f"Sanity check passed: Empty cache at the end of process")

        # sanity check
        if nb_file_initializations != len(outvolumes.keys()):
            print("WARNING: number of initilized files is different than number outfiles")
            print("Number initializations:", nb_file_initializations)
            print("Number outfiles: ", len(outvolumes.keys()))
        else:
            print(f"Sanity check passed: Number initializations == Number outfiles ({nb_file_initializations}=={len(outvolumes.keys())})")

        print("\nShapes written:")
        for row in volumes_written: 
            print(row)

    print("\nMax nb voxels in RAM: ", voxel_tracker.get_max())
                
    get_opened_files()
    return tpp, read_time, write_time, [nb_outfile_openings, nb_outfile_inside_seeks, nb_infile_openings, nb_infile_inside_seeks], voxel_tracker, [ram_pile, swap_pile]