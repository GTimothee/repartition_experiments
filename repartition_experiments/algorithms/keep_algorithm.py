import math, time, csv, psutil, sys, copy, os, json
import numpy as np
from repartition_experiments.algorithms.policy_remake import compute_zones_remake
from repartition_experiments.algorithms.utils import get_partition, get_named_volumes, get_overlap_subarray, get_file_manager, numeric_to_3d_pos, Volume, hypercubes_overlap, included_in, get_volumes, to_basis
from repartition_experiments.algorithms.tracker import Tracker
from repartition_experiments.algorithms.utils import get_opened_files
from repartition_experiments.algorithms.voxel_tracker import VoxelTracker
from multiprocessing import Process, Queue

DEBUG = False

import gc


def get_input_aggregate(O, I):
    """ Returns the input aggregate's shape
    """
    lambd = list()
    dimensions = len(O)
    for dim in range(dimensions):
        lambd.append(math.ceil(O[dim]/I[dim])*I[dim])
    return tuple(lambd)


def remove_from_cache(cache, outfile_index, volume_to_write):
    """ Remove element from cache after it has been written

    Arguments: 
    ----------
        cache
        outfile_index: output block to write
        volume_to_write: write buffer
    """
    volumes_in_cache = cache[outfile_index]

    target = None
    for i, e in enumerate(volumes_in_cache):
        p1, p2 = e[0].get_corners()
        if p1 == volume_to_write.p1 and p2 == volume_to_write.p2:
            target = i
            break
    
    if target == None:
        raise ValueError("Cannot remove data part from cache: data not in cache")
    
    del volumes_in_cache[target]
    cache[outfile_index] = volumes_in_cache


def write_in_outfile(data_part, vol_to_write, file_manager, outvolume, outfile_shape, outfiles_partition, cache, from_cache):
    """ Writes an output file part (write buffer) which is ready to be written.

   Arguments: 
    ----------
        data_part: write buffer's data
        vol_to_write: write buffer's Volume object
        file_manager: to read/write with a specific file format
        outvolume: output block Volume object
        outfile_shape: output block shape
        outfiles_partition: tuple of length 3 with number of output blocks per dimension in the original array (ex (3,3,3) means 3 buffers in k in j and in i)
        cache
    """
    global outdirs_dict, outdir_index

    # get region in output file to write into
    vol_to_write_O_basis = to_basis(vol_to_write, outvolume)
    slices = vol_to_write_O_basis.get_slices()

    # write
    i, j, k = numeric_to_3d_pos(outvolume.index, outfiles_partition, order='C')

    if (i, j, k) in outdirs_dict.keys():
        outdir_path = outdirs_dict[(i, j, k)]
        print(f"Writing at: {outdir_path}")
    else:
        outdir_path = '/disk' + str(outdir_index) + '/gtimothee/output'
        outdirs_dict[(i, j, k)] = outdir_path
        outdir_index += 1
        if outdir_index == 6:
            outdir_index = 0

        print(f"Writing at: {outdir_path}")
        print(f"Increasing writing index: {outdir_index}")

    t2 = time.time()
    empty_dataset = file_manager.write_data(i, j, k, outdir_path, data_part, slices, outfile_shape)
    t2 = time.time() - t2

    return t2, empty_dataset


def write_in_outfile2(data, buffer_slices, vol_to_write, file_manager, outvolume, outfile_shape, outfiles_partition, cache, from_cache):
    """ Writes an output file part which is ready to be written.

    Arguments: 
    ----------
        data: read buffer data
        buffer_slices: slices from the buffer to extract the part from buffer that must be written
        vol_to_write: write buffer's Volume object
        file_manager: to read/write with a specific file format
        outvolume: output block Volume object
        outfile_shape: output block shape
        outfiles_partition: tuple of length 3 with number of output blocks per dimension in the original array (ex (3,3,3) means 3 buffers in k in j and in i)
        cache
    """
    global outdirs_dict, outdir_index

    s = buffer_slices

    # get region in output file to write into
    vol_to_write_O_basis = to_basis(vol_to_write, outvolume)
    slices = vol_to_write_O_basis.get_slices()

    # write
    i, j, k = numeric_to_3d_pos(outvolume.index, outfiles_partition, order='C')

    if (i, j, k) in outdirs_dict.keys():
        outdir_path = outdirs_dict[(i, j, k)]
        print(f"Writing at: {outdir_path}")
    else:
        outdir_path = '/disk' + str(outdir_index) + '/gtimothee/output'
        outdirs_dict[(i, j, k)] = outdir_path
        outdir_index += 1
        if outdir_index == 6:
            outdir_index = 0

        print(f"Writing at: {outdir_path}")
        print(f"Increasing writing index: {outdir_index}")

    t2 = time.time()
    empty_dataset = file_manager.write_data(i, j, k, outdir_path, data[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]], slices, outfile_shape)
    t2 = time.time() - t2

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


def read_buffer(data, buffer, buffers_to_infiles, involumes, file_manager, input_dirpath, R, I):
    """ Read a buffer from several input files.

    Arguments: 
    ----------
        data: read buffer object (will contain the actual data loaded from input blocks)
        buffer: read buffer's Volume object
        buffers_to_infiles: dict associating a buffer index to the input files it has to read
        involumes: dict associating a input file index to its Volume object
        file_manager: used to actually read
        R: shape of original array 
        I: shape of input block
        input_dirpath; directory containing the input blocks


    Returns:
    --------
        data: dict, 
            - associate Volume object to data part loaded,
            - Volume.index:  index of input file containing the data loaded,
            - Volume.corners(): corners of volume in basis of R

    """
    print(f"Initializing buffer reading...")

    involumes_list = buffers_to_infiles[buffer.index]

    # print(f"Found list of inblocks crossed: {involumes_list}")

    t1 = 0 
    nb_opening_seeks_tmp = 0
    nb_inside_seeks_tmp = 0
    repartition_dict = None

    # section in case of distributed mode --------------
    if global_distributed:
        # print(f"Distributed")
        
        json_filename = '/disk0/gtimothee/repartition_dict.json'
        if not os.path.isfile(json_filename):
            # print("cannot find association dict json file")
            sys.exit(1)
        else:
            pass # print(f"json file found")

        try: 
            with open(json_filename) as f:
                repartition_dict = json.load(f)
        except Exception as e: 
            print(e)
            # print("error (1)")
            sys.exit(1)

        if repartition_dict == None:
            # print("error (2)")
            sys.exit(1)
        else:
            pass # print(f"Found reparition dict: {repartition_dict}")
    # ----------------------------------------------------

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
        
        # print(f"Preparing read slices...")
        # get infile 3d position, get slices to read from overlap volume, read data
        i, j, k = numeric_to_3d_pos(involume.index, get_partition(R, I), order='C')
        slices = intersection_read.get_slices()
        s = to_basis(intersection_in_R, buffer).get_slices()

        if global_distributed:
            # print(f"Reading (1)...")
            t_tmp = time.time()
            data[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]] = file_manager.read_data_from_fp(repartition_dict[str((i,j,k))], slices)
            t1 += time.time() - t_tmp
        else:
            # print(f"Reading (2)...")
            t_tmp = time.time()
            data[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]] = file_manager.read_data(i, j, k, input_dirpath, slices)
            t1 += time.time() - t_tmp

    print(f"Successful, returning data...")
    return data, t1, nb_opening_seeks_tmp, nb_inside_seeks_tmp


def equals(v1, v2):
    """ Test if two Volumes have same coordinates and shape
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


def add_to_cache(cache, vol_to_write, data, buffer_slices, outvolume_index, overlap_vol_in_R, datapart_volume):
    """ Add part of write buffer into the cache

    Arguments: 
    ----------
        cache
        vol_to_write: write buffer
        data: read buffer 
        buffer_slices: slices from the read buffer containing data of the write buffer 
        outvolume_index: index of output block of interest
        overlap_vol_in_R: Volume representing the intersection between the read buffer and the write buffer, in the coordinates system of R (the original image)
        datapart_volume: overlap_vol_in_R in the coordinates system of the read buffer

    cache: 
    ------
        key = outfile index
        value = [(volumetowrite, array),...]
        array has shape volumetowrite, missing parts are full of zeros
    """
    s = buffer_slices

    # add list in cache for outfile index if nothing for this file in cache yet
    if not outvolume_index in cache.keys():
        cache[outvolume_index] = list()
        if DEBUG:
            print("add key")
            print_mem_info()

    # if cache already contains part of the outfile part, we add data to it 
    for element in cache[outvolume_index]:
        vol_to_write_tmp, volumes_list, arrays_list, tracker = element

        if equals(vol_to_write, vol_to_write_tmp):
            volumes_list.append(overlap_vol_in_R)
            arrays_list.append(copy.deepcopy(data[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]]))
            tracker.add_volume(overlap_vol_in_R)
            element = (vol_to_write_tmp, volumes_list, arrays_list, tracker)  # update element
            return 

    # add new element
    if DEBUG:
        print_mem_info()
    arrays_list = [copy.deepcopy(data[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]])]
    volumes_list = [overlap_vol_in_R]
    tracker = Tracker()
    tracker.add_volume(overlap_vol_in_R)
    cache[outvolume_index].append((vol_to_write, volumes_list, arrays_list, tracker))      


def get_overlap_volume(v1, v2):
    pair = get_overlap_subarray(v1, v2)  # overlap coordinates in basis of R
    p1, p2 = tuple(pair[0]), tuple(pair[1])
    return Volume(0, p1, p2)


def get_data_to_write(vol_to_write, buff_volume, data):
    """ Get intersection between the buffer volume and the volume to write into outfile

    Arguments: 
    ----------
        vol_to_write: write buffer's Volume object
        buff_volume: read buffer's Volume object
        data: read buffer
    """
    v1 = get_overlap_volume(vol_to_write, buff_volume) 
    return v1, to_basis(v1, buff_volume).get_slices()


def complete(cache, vol_to_write, outvolume_index):
    """ Test if a volume to write (write buffer) is complete in cache i.e. can be written down

    Arguments: 
    ----------
        cache: cache dictionary 
        vol_to_write: write buffer Volume object
        outvolume_index: index of output block 
    """

    is_complete = False
    to_del = -1
    arr = None
    for i, e in enumerate(cache[outvolume_index]):
        v, v_list, a_list, tracker = e 
        if equals(vol_to_write, v):
            if tracker.is_complete(vol_to_write.get_corners()): 
                
                arr = np.empty(copy.deepcopy(vol_to_write.get_shape()), dtype=np.float16)
                for v_tmp, a_tmp in zip(v_list, a_list):
                    s = to_basis(v_tmp, vol_to_write).get_slices()
                    arr[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]] = np.copy(a_tmp)
                    del a_tmp

                to_del = i
                is_complete = True
                break

    if to_del != -1:
        del cache[outvolume_index][to_del]
    
    return is_complete, arr



def print_mem_info():
    """ For debug purposes
    """
    global start_mem

    process = psutil.Process(os.getpid())
    # d = process.memory_info().vms // 1024 // 1024
    mem = psutil.virtual_memory()
    d = (mem.total - mem.available) /1024 /1024
    if start_mem == None:
        start_mem = d
        print("start_mem: ", start_mem)
    else:
        d = d - start_mem
    print(d)


def write_or_cache(outvolume, vol_to_write, buffer, cache, data):
    """ 
    Arguments: 
    ----------
        outvolume: Volume object representing the output block 
        vol_to_write: Volume object representing a write buffer which will write into the output block represented by outvolume
        buffer: Volume object representing the read buffer (its coordinates)
        cache: dictionary representing the cache
        data: read buffer
    """
    data_to_write_vol, buffer_slices = get_data_to_write(vol_to_write, buffer, data)
    buff_write = 0
    volume_written = False
    data_moved = 0
    
    if equals(vol_to_write, data_to_write_vol):  
        if DEBUG:           
            print("equals")  

        # write
        if DEBUG:
            print_mem_info()
        t2, initialized = write_in_outfile2(data, buffer_slices, vol_to_write, file_manager, outvolume, O, outfiles_partition, cache, False)
        if DEBUG:
            print("write")
            print_mem_info()

        # stats
        buff_write += t2
        
        if sanity_check:
            assert vol_to_write.get_shape() == data_to_write_vol.get_shape() and vol_to_write.get_shape() == data_to_write.shape
            written_shapes.append(vol_to_write.get_shape())
            outvolumes_trackers[outvolume_index].add_volume(vol_to_write)
            if initialized:
                nb_file_initializations += 1
            nb_volumes_written += 1
            nb_oneshot_writes += 1

        # garbage collection
        volume_written = True

    else:
        
        add_to_cache(cache, vol_to_write, data, buffer_slices, outvolume.index, data_to_write_vol, buffer)
        
        if DEBUG:
            print("[cache+]")
            print_mem_info()

        # stats
        tmp_s = data_to_write_vol.get_shape()
        data_moved += tmp_s[0]*tmp_s[1]*tmp_s[2]

        is_complete, arr = complete(cache, vol_to_write, outvolume.index)
        
        if is_complete:
            # write
            if addition:
                arr = arr +1

            if DEBUG:
                print("[cache write]")
                print_mem_info()
            t2, initialized = write_in_outfile(arr, vol_to_write, file_manager, outvolume, O, outfiles_partition, cache, True)
            del arr
            
            # stats
            buff_write += t2
            tmp_s = vol_to_write.get_shape()
            data_moved -= tmp_s[0]*tmp_s[1]*tmp_s[2]

            # garbage collection
            volume_written = True

            if sanity_check:
                written_shapes.append(vol_to_write.get_shape())
                outvolumes_trackers[outvolume_index].add_volume(vol_to_write)
                volumes_written.append(vol_to_write.get_shape())
                if initialized:
                    nb_file_initializations += 1
                nb_volumes_written += 1

    return buff_write, volume_written, data_moved


def process_buffer(data, arrays_dict, buffers, buffer, voxel_tracker, buffers_to_infiles, buffer_to_outfiles, cache):
    """ 
    Arguments: 
    ----------
        data: read buffer to be filled (the buffer object is not destroyed between the read buffers, its content is simply replaced)
        arrays_dict: dictionary mapping each output block index to its list of write blocks
        buffers: list of read buffers' Volumes
        buffer: Volume representing current read buffer
        voxel_tracker: for monitoring purposes
        buffers_to_infiles: maps each read buffer index to the list of input blocks it crosses -> to know which input block to read
        buffer_to_outfiles: maps each read buffer index to the list of output blocks it crosses -> to only search for the write buffers linked to that read buffer
        cache: dictionary representing the cache

    Description of the cache object: 
        maps outfile_index (numeric index of an output block) to the list of volumes to write (write buffers)
    """
    print(f"Processing buffer...")
    # voxel tracker
    data_shape = buffer.get_shape()
    buffer_size = data_shape[0]*data_shape[1]*data_shape[2]
    
    data_movement = 0
    tmp_write = 0

    # read buffer -> data contains the loaded data. 
    data, t1, nb_opening_seeks_tmp, nb_inside_seeks_tmp = read_buffer(data, buffer, buffers_to_infiles, involumes, file_manager, input_dirpath, R, I)

    print(f"Processing data...")

    # looping over the output blocks related to the current read buffer
    for outvolume_index in buffer_to_outfiles[buffer.index]:
        outfile_parts_written = list()

        # for each part of output file (for each write buffer)
        for j, outfile_part in enumerate(arrays_dict[outvolumes[outvolume_index].index]):  
            
            # for each part of read buffer
            if hypercubes_overlap(buffer, outfile_part):

                # write write buffer if complete, or put part of write bufer into the cache
                buff_write, volume_written, data_moved = write_or_cache(outvolumes[outvolume_index], outfile_part, buffer, cache, data)
                
                data_movement += data_moved
                tmp_write += buff_write

                if volume_written:
                    outfile_parts_written.append(j)


        # if part of output file entirely processed, remove from arrays_dict
        for j in outfile_parts_written:
            del arrays_dict[outvolumes[outvolume_index].index][j]

    voxel_tracker.add_voxels(data_movement)

    return nb_opening_seeks_tmp, nb_inside_seeks_tmp, t1, tmp_write


def _run_keep(arrays_dict, buffers, buffers_to_infiles, buffer_to_outfiles):
    """
    Arguments: 
    ----------
        arrays_dict: dictionary mapping each output block index to its list of write blocks
        buffers: list of Volume objects (see utils.py) representing the read buffers. Each volume contains the coordinates of the buffer in the original image.  
        buffers_to_infiles: maps each read buffer index to the list of input blocks it crosses -> to know which input block to read
        buffer_to_outfiles: maps each read buffer index to the list of output blocks it crosses -> to only search for the write buffers linked to that read buffer
    """
    cache = dict()
    voxel_tracker = VoxelTracker()
    nb_infile_openings = 0
    nb_infile_inside_seeks = 0
    nb_buffers = len(buffers.keys())
    read_time = 0
    write_time = 0

    from monitor.monitor import Monitor
    _monitor = Monitor(enable_print=False, enable_log=False, save_data=True)
    _monitor.disable_clearconsole()
    _monitor.set_delay(5)
    _monitor.start()
    
    buffer_shape = buffers[0].get_shape()
    buffer_size = buffer_shape[0] * buffer_shape[1] * buffer_shape[2] * 2
    buffer_data = np.empty(copy.deepcopy(buffer_shape), dtype=np.float16)
    voxel_tracker.add_voxels(buffer_size)

    # for each read buffer
    for buffer_index in range(nb_buffers):
        print("\nBUFFER ", buffer_index, '/', nb_buffers)
        if DEBUG:
            print_mem_info() 
            
        buffer = buffers[buffer_index]
        nb_opening_seeks_tmp, nb_inside_seeks_tmp, t1, t2 = process_buffer(buffer_data, arrays_dict, buffers, buffer, voxel_tracker, buffers_to_infiles, buffer_to_outfiles, cache)
        
        read_time += t1
        write_time += t2
        nb_infile_openings += nb_opening_seeks_tmp
        nb_infile_inside_seeks += nb_inside_seeks_tmp

        if DEBUG:
            print("End of buffer - Memory info:")
            print_mem_info()

            if buffer_index == 1:
                sys.exit()

        buffer_data = np.empty(copy.deepcopy(buffer_shape), dtype=np.float16)

    file_manager.close_infiles()

    _monitor.stop()
    ram_pile, swap_pile = _monitor.get_mem_piles()
    return [read_time, write_time], ram_pile, swap_pile, nb_infile_openings, nb_infile_inside_seeks, voxel_tracker


def end_sanity_check():
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


start_mem = None


def keep_algorithm(arg_R, arg_O, arg_I, arg_B, volumestokeep, arg_file_format, arg_outdir_path, arg_input_dirpath, arg_addition, arg_global_distributed, arg_sanity_check=False):
    """

        Arguments: 
        ----------
            arg_R: shape of reconstructed(/original) image
            arg_O: output block shape
            arg_I: input block shape
            arg_B: read buffer shape
            volumestokeep: remainder volumes to keep (see case)
            arg_file_format: file format (HDF5 only for now)
            arg_outdir_path: in non distributed mode, path to write the output blocks
            arg_input_dirpath: in non distributed mode, path to read the input blocks
            arg_global_distributed: activates the distributed mode -> input and output blocks are distributed on several nodes of the cluster
            arg_sanity_check: boolean, set to True to test if resplit was successful, cannot be used in distributed mode

        Description of the cache object used: 
            dict,
            maps outfile_index (numeric index of an output block) to the list of volumes to write (write buffers)
    """
    import logging
    import logging.config
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
    })

    # initialize utility variables
    global outdir_path, R, O, I, B, file_format, input_dirpath, sanity_check, addition
    global buffers_partition, infiles_partition, outfiles_partition
    global buffers, involumes, outvolumes
    global file_manager
    global global_distributed
    global outdirs_dict, outdir_index

    print(f"Setting arguments...")
    outdirs_dict = dict()
    outdir_index = 0
    file_format, input_dirpath = arg_file_format, arg_input_dirpath
    R, O, I, B = tuple(arg_R), tuple(arg_O), tuple(arg_I), tuple(arg_B)
    buffers_partition, buffers = get_volumes(R, B)
    infiles_partition, involumes = get_volumes(R, I)
    outfiles_partition, outvolumes = get_volumes(R, O)
    file_manager = get_file_manager(file_format)
    sanity_check = arg_sanity_check
    addition = arg_addition
    global_distributed = arg_global_distributed

    # compute_zones is used to return the write buffers and a dictionary buffer_to_outfiles which maps each read buffer to the output blocks it overlaps
    print("Preprocessing...")
    tpp = time.time()
    arrays_dict, buffer_to_outfiles, nb_outfile_openings, nb_outfile_inside_seeks = compute_zones_remake(B, O, R, volumestokeep, outfiles_partition, outvolumes, buffers, True)
    buffers_to_infiles = get_buffers_to_infiles(buffers, involumes) # same than buffer_to_outfiles for input blocks
    tpp = time.time() - tpp
    print("Preprocessing time: ", tpp)

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

    # core of the algorithm
    times, ram_pile, swap_pile, nb_infile_openings, nb_infile_inside_seeks, voxel_tracker = _run_keep(arrays_dict, buffers, buffers_to_infiles, buffer_to_outfiles)

    if sanity_check:
        end_sanity_check()
                
    get_opened_files()  # sanity check to see how many file objects are still open (monitor if files are closed)
    return tpp, times[0], times[1], [nb_outfile_openings, nb_outfile_inside_seeks, nb_infile_openings, nb_infile_inside_seeks], voxel_tracker, [ram_pile, swap_pile]