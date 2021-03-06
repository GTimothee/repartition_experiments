import math
from repartition_experiments.algorithms.utils import Volume, get_file_manager, get_blocks_shape
from repartition_experiments.file_formats.hdf5 import HDF5_manager
from repartition_experiments.algorithms.utils import get_opened_files


def get_entity_sizes(cs, bytes_per_voxel, partition):
    bs = cs[0] * cs[1]  * cs[2] * bytes_per_voxel  # block size
    brs = bs * partition[2]  # block row size
    bss = brs * partition[1]  # block slice size
    return bs, brs, bss


def get_strategy(buffer_mem_size, block_size, block_row_size, block_slice_size): 
    """ Get clustered writes best load strategy given the memory available for io optimization.

    Returns:
    ---------
        strategy
    """
    if buffer_mem_size < block_size:
        raise ValueError("Buffer size too small for one chunk")

    if math.floor(buffer_mem_size / block_slice_size) > 0: 
        return 2
    else:
        if math.floor(buffer_mem_size / block_row_size) > 0: 
            return 1
        else:
            return 0


def compute_buffers(buffer_mem_size, strategy, origarr_size, cs, block_size, block_row_size, block_slice_size, partition, R, bytes_per_voxel):
    """
        partition: partition tuple of R by O = nb chunks per dimension
    """
    def get_last_slab():
        return

    buffers = dict()
    index = 0

    if strategy == 2:
        slices_per_buffer = math.floor(buffer_mem_size / block_slice_size)
        buffer_shape = (slices_per_buffer * cs[0], R[1], R[2])
        buffer_size = buffer_shape[0] * buffer_shape[1] * buffer_shape[2] * bytes_per_voxel

        nb_plain_buffers = math.floor(origarr_size / buffer_size)

        for i in range(nb_plain_buffers):
            lowcorner = (i * buffer_shape[0], 0, 0)
            upcorner = ((i + 1) * buffer_shape[0], buffer_shape[1], buffer_shape[2])
            buffers[i] = Volume(
                            i,
                            lowcorner,
                            upcorner)

        if nb_plain_buffers > 0:
            prev_buff = buffers[nb_plain_buffers-1]
            if prev_buff.p2[0] != (R[0]):
                buffers[nb_plain_buffers] = Volume(nb_plain_buffers,
                                                (nb_plain_buffers * buffer_shape[0], 0, 0),
                                                    R)  
        else:
            buffers[nb_plain_buffers] = Volume(nb_plain_buffers,
                                               (nb_plain_buffers * buffer_shape[0], 0, 0),
                                               R)  

    elif strategy == 1:
        nb_block_slices = partition[0]
        nb_block_rows_per_buffer = math.floor(buffer_mem_size/block_row_size)
        buffer_size = nb_block_rows_per_buffer * block_row_size

        for i in range(nb_block_slices):
            nb_buffers_per_slice = math.floor(block_slice_size / buffer_size)
            
            for j in range(nb_buffers_per_slice): # a buffer is one or more block rows here
                lowcorner =(i*cs[0], j * nb_block_rows_per_buffer * cs[1], 0)
                upcorner = ((i+1)*cs[0], (j+1) * nb_block_rows_per_buffer * cs[1], R[2])
                buffers[index] = Volume(index, lowcorner, upcorner)
                index += 1
            
            prev_buff = buffers[index-1]
            if prev_buff.p2[1] != (R[1]):
                buffers[index] = Volume(index, 
                                        (i * cs[0], nb_buffers_per_slice * cs[1], 0),
                                        ((i + 1) * cs[0], R[1], R[2])) 
                index += 1

    elif strategy == 0:

        for i in range(partition[0]): 
            start_i, end_i = i*cs[0], (i+1)*cs[0]

            for j in range(partition[1]): 
                start_j, end_j = j*cs[1], (j+1)*cs[1]

                nb_blocks_per_buff = math.floor(buffer_mem_size/block_size)
                buffer_size = nb_blocks_per_buff * block_size
                nb_buffer_per_row = math.floor(block_row_size / buffer_size)

                for k in range(nb_buffer_per_row):
                    if k == 0:
                        start_k = 0
                    else:
                        start_k = buffers[index-1].p2[2]
                    end_k = (k+1) * nb_blocks_per_buff * cs[2]

                    buffer_volume = Volume(index,
                                        (start_i, start_j, start_k),
                                        (end_i, end_j, end_k))
                    buffers[index] = buffer_volume
                    index += 1

                prev_buff = buffers[index-1]
                if prev_buff.p2[2] != (R[2]):
                    last_buffer = Volume(index,
                                        (start_i, start_j, prev_buff.p2[2]),
                                        (end_i, end_j, R[2]))
                    buffers[index] = last_buffer
                    index += 1

    else:
        raise ValueError("Strategy does not exist")

    return buffers


def read_buffer(arr, file_manager, buffer):
    p1, p2 = buffer.get_corners()
    # print("reading slices:", p1[0], p2[0], p1[1], p2[1], p1[2], p2[2])
    return arr[p1[0]: p2[0], p1[1]: p2[1], p1[2]: p2[2]]
    

def write_splits(file_manager, buffer, buffer_data, cs, outdir_path):
    p1, p2 = buffer.get_corners()
    first_index = (p1[0]/cs[0], p1[1]/cs[1], p1[2]/cs[2])
    buffer_shape = (p2[0]-p1[0], p2[1]-p1[1], p2[2]-p1[2])
    buff_partition = get_blocks_shape(buffer_shape, cs)

    _3d_index = first_index
    for i in range(buff_partition[0]):
        for j in range(buff_partition[1]):
            for k in range(buff_partition[2]):
                split_data = buffer_data[
                    i * cs[0]:(i+1) * cs[0], 
                    j * cs[1]:(j+1) * cs[1], 
                    k * cs[2]:(k+1) * cs[2]]

                region = ((0, cs[0]), (0, cs[1]), (0, cs[2]))
                file_manager.write_data(int(_3d_index[0] + i), 
                                        int(_3d_index[1] + j), 
                                        int(_3d_index[2] + k), 
                                        outdir_path, 
                                        split_data, 
                                        region, 
                                        cs)


def clustered_writes(origarr_filepath, R, cs, bpv, m, ff, outdir_path):
    """ Implementation of the clustered strategy for splitting a 3D array.
    Output file names are following the following regex: outdir_path/{i}_{j}_{k}.extension
    WARNING: this implementation loads the whole input array in RAM. We had 250GB of RAM for our experiments so we decided to use it.

    Arguments: 
    ----------
        R: original array shape
        m: memory available for the buffer
        cs: chunk shape
        bpv: number of bytes per voxel
        ff: file_format
        outdir_path: where to write the splits
    """

    strategies = {
        0: "blocks",
        1: "block_rows",
        2: "block_slices"
    }
    
    file_manager = get_file_manager(ff)

    partition = get_blocks_shape(R, cs)
    bs, brs, bss = get_entity_sizes(cs, bpv, partition)
    strategy = get_strategy(m, bs, brs, bss)

    origarr_size = R[0] * R[1] * R[2] * bpv
    buffers = compute_buffers(m, strategy, origarr_size, cs, bs, brs, bss, partition, R, bpv)

    origarr = file_manager.get_dataset(origarr_filepath, '/data')
    for buffer_index in range(len(buffers.values())):
        buffer = buffers[buffer_index]
        buffer_data = read_buffer(origarr, file_manager, buffer)
        write_splits(file_manager, buffer, buffer_data, cs, outdir_path)

    file_manager.close_infiles()
    get_opened_files()