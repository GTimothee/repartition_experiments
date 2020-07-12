import os, h5py, time
import numpy as np

from .clustered_writes import get_entity_sizes, get_strategy, compute_buffers
from .utils import get_overlap_subarray, Volume, get_file_manager, get_blocks_shape


def clustered_reads(outdir_path, R, cs, bpv, m, ff, indir_path, dtype=np.float16):
    strategies = {
        0: "blocks",
        1: "block_rows",
        2: "block_slices"
    }
    
    if m < cs[0]*cs[1]*cs[2]*bpv:
        raise ValueError("m not big enough to store one block!")

    out_filepath = os.path.join(outdir_path, 'merged.hdf5')
    file_manager = get_file_manager(ff)
    file_manager.clean_directory(outdir_path)

    partition = get_blocks_shape(R, cs)
    bs, brs, bss = get_entity_sizes(cs, bpv, partition)
    strategy = get_strategy(m, bs, brs, bss)
    print("Running clustered reads with strategy: ", strategy)
    buffers = compute_buffers(m, strategy, R[0]*R[1]*R[2]*bpv, cs, bs, brs, bss, partition, R, bpv)

    f = h5py.File(out_filepath, "w")
    dset = None
    read_time = 0
    write_time = 0
    nb_infiles_opening = 0
    nb_outfiles_opening = 0
    nb_infiles_seeks = 0
    nb_outfiles_seeks = 0

    for buffer_index in range(len(buffers.values())):
        print(f"Processing buffer {buffer_index}")
        buffer = buffers[buffer_index]

        # preprocessing
        shape = buffer.get_shape()
        p1, p2 = buffer.get_corners()
        start_3d_pos = [int(p1[i]/cs[i]) for i in range(3)]
        partition_shape = [int(shape[i]/cs[i]) for i in range(3)]

        # read buffer
        buffer_data = np.empty(shape, dtype=dtype)        
        for i in range(partition_shape[0]):
            for j in range(partition_shape[1]):
                for k in range(partition_shape[2]):
                    _3d_pos = (start_3d_pos[0] + i, start_3d_pos[1] + j, start_3d_pos[2] + k)
                    print(f'Loading input file: ', _3d_pos)
                    rt = time.time()
                    data = file_manager.read_data(_3d_pos[0], _3d_pos[1], _3d_pos[2], indir_path, None)
                    read_time += time.time() - rt 
                    buffer_data[i*cs[0]:(i+1)*cs[0], j*cs[0]:(j+1)*cs[1], k*cs[2]:(k+1)*cs[2]] = data

                    nb_infiles_opening += 1

        # write buffer
        print("Writing buffer...")
        if dset == None:
            wt = 0
            dset = f.create_dataset("/data", shape, data=buffer_data, maxshape=R)
            write_time += time.time() - wt 
            nb_outfiles_opening += 1
        else:
            new_shape = tuple([max(dset.shape[i], p2[i]) for i in range(3)])
            dset.resize(new_shape)
            s = buffer.get_slices()
            wt = 0
            dset[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]] = buffer_data
            write_time += time.time() - wt 
            nb_outfiles_opening += 1

            # compute seeks
            dset_volume = Volume(0, (0,0,0), new_shape)
            pair = get_overlap_subarray(buffer, dset_volume)  # overlap in R
            p1, p2 = tuple(pair[0]), tuple(pair[1])
            intersection_vol = Volume(0, p1, p2)

            s = intersection_vol.get_shape()
            tmp = 0
            if s[2] != new_shape[2]:
                tmp += s[0]*s[1]
            elif s[1] != new_shape[1]:
                tmp += s[0]
            elif s[0] != new_shape[0]:
                tmp += 1
            else:
                pass
            nb_outfiles_seeks += tmp

    f.close()
    return read_time, write_time, [nb_outfiles_opening, nb_outfiles_seeks, nb_infiles_opening, nb_infiles_seeks]