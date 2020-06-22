import math
from .utils import Volume

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


def compute_buffers(strategy, origarr_size, block_size, block_row_size, block_slice_size):
    """
        partition: partition tuple of R by O = nb chunks per dimension
    """
    def get_last_slab():
        return

    buffers = dict()

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

        buffers[nb_plain_buffers] = Volume(nb_plain_buffers,
                                           (nb_plain_buffers * buffer_shape[0], 0, 0),
                                            R)  # coords du dernier slab jusqua la fin de R 

    elif strategy == 1:
        nb_block_slices = partition[0]
        nb_block_rows_per_buffer = math.floor(buffer_mem_size/block_row_size)
        buffer_size = nb_block_rows_per_buffer * block_row_size

        index = 0
        for i in range(nb_block_slices):
            nb_buffers_per_slice = block_slice_size / buffer_size
            
            for j in range(nb_buffers_per_slice):
                lowcorner =(i*cs[0], j * nb_block_rows_per_buffer * cs[1], 0)
                upcorner = ((i+1)*cs[0], (j+1) * nb_block_rows_per_buffer * cs[1], R[2])
                buffers[index] = Volume(index, lowcorner, upcorner)
                index += 1
            
            buffers[index] = Volume(index, 
                                    (i * cs[0], nb_buffers_per_slice * cs[1], 0),
                                    ((i + 1) * cs[0], R[1], R[2]))  # add last block rows 
            index += 1

    elif strategy == 0:
        index = 0
        for i in range(partition[0]): 
            for j in range(partition[1]): 
                nb_blocks = math.floor(buffer_mem_size/block_size)

                buffer_volume = Volume(index,
                                       (i*cs[0], j*cs[1], 0),
                                       ((i+1)*cs[0], (j+1)*cs[1]), nb_blocks*cs[2])
                buffers[index] = buffer_volume
                index += 1

                last_buffer = Volume(index,
                                     (i*cs[0], j*cs[1], nb_blocks*cs[2]),
                                     ((i+1)*cs[0], (j+1)*cs[1]), R[2])
                buffers[index] = last_buffer
                index += 1

    else:
        raise ValueError("Strategy does not exist")

    return buffers


def clustered_writes():
    strategies = {
        0: "blocks",
        1: "block_rows",
        2: "block_slices"
    }
    
    bs, brs, bss = get_entity_sizes(cs, bytes_per_voxel, partition)
    strategy = get_strategy(m, bs, brs, bss)
    buffers = compute_buffers(strategy, origarr_size)

    for buffer in buffers:
        buffer_data = read(buffer)  
        write_splits(buffer_data)