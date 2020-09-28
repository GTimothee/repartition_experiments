import math

from repartition_experiments.algorithms.utils import *
from repartition_experiments.algorithms.policy_remake import compute_zones_remake

def shape_to_end_coords(M, A, d=3):
    '''
    M: block shape M=(M1, M2, M3). Example: (500, 500, 500)
    A: input array shape A=(A1, A2, A3). Example: (3500, 3500, 3500)
    Return: end coordinates of the blocks, in each dimension. Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    '''
    return [ [ (j+1)*M[i] for j in range(int(A[i]/M[i])) ] for i in range(d)]

def seeks(A, M, D):
    '''
    A: shape of the large array. Example: (3500, 3500, 3500)
    M: coordinates of memory block ends (read or write). Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    D: coordinates of disk block ends (input or output). Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    Returns: number of seeks required to write M blocks into D blocks. This number is also the number of seeks
             to read D blocks into M blocks.
    '''

    c = [ 0 for i in range(len(A))] # number of cuts in each dimension
    m = [] # number of matches in each dimension

    # n = 1 # Total number of disk blocks
    # for i in range(len(A)):
    #     n *= len(D[i])

    for d in range(len(A)): # d is the dimension index
        
        nd = len(D[d])
        Cd = [ ]  # all the cut coordinates (for debugging and visualization)
        for i in range(nd): # for each output block, check how many pieces need to be written
            if i == 0:
                Cid = [ m for m in M[d] if 0 < m and m < D[d][i] ]  # number of write block endings in the output block
            else:               
                Cid = [ m for m in M[d] if D[d][i-1] < m and m < D[d][i] ]  # number of write block endings in the output block
            if len(Cid) == 0:
                continue
            c[d] += len(Cid) + 1
            Cd += Cid

        m.append(len(set(M[d]).union(set(D[d]))) - c[d])

    s = A[0]*A[1]*c[2] + A[0]*c[1]*m[2] + c[0]*m[1]*m[2] # + n

    return s


def compute_baseline_seeks_model(A, I, O):
    # computing number of outblocks openings: 
    read_cuts = shape_to_end_coords(I, A, d=3)
    outblocks_cuts = shape_to_end_coords(O, A, d=3)
    for i in range(3):
        for e in read_cuts[i]:
            if e not in outblocks_cuts[i]:
                outblocks_cuts[i].append(e)
    nb_outfile_openings = len(outblocks_cuts[0])*len(outblocks_cuts[1])*len(outblocks_cuts[2])

    ni = int(A[0]/I[0] * A[1]/I[1] * A[2]/I[2])
    nb_outfile_seeks = seeks(A, shape_to_end_coords(I, A), shape_to_end_coords(O, A))
    nb_infile_openings = ni 
    nb_infile_seeks = 0
    return [nb_outfile_openings, nb_outfile_seeks, nb_infile_openings, nb_infile_seeks]


def compute_keep_seeks_model(A, R, I, O, W, nb_write_buffers):
    # computing number of inblocks openings
    read_cuts = shape_to_end_coords(R, A, d=3)
    inblock_cuts = shape_to_end_coords(I, A, d=3)
    for i in range(3):
        for e in read_cuts[i]:
            if e not in inblock_cuts[i]:
                inblock_cuts[i].append(e)
    nb_infile_openings = len(inblock_cuts[0])*len(inblock_cuts[1])*len(inblock_cuts[2])

    s1 = seeks(A, shape_to_end_coords(R, A), shape_to_end_coords(I, A))
    s2 = seeks(A, W, shape_to_end_coords(O, A))

    print(f"[Model] total seeks inside inblocks: {s1}, nb_infile_openings: {nb_infile_openings}")
    print(f"[Model] total seeks due to read buffers: {s1 + nb_infile_openings}")
    s1 += nb_infile_openings
    print(f"[Model] total seeks inside outblocks: {s2}, nb outfile openings: {nb_write_buffers}")
    print(f"[Model] total seeks due to write buffers: {s2 + nb_write_buffers}")
    s2 += nb_write_buffers
    return s1 + s2


def get_volumes_to_keep(A, B, O):
    # compute theta max
    buffers_partition, buffers = get_volumes(A, B)
    T_max = [0,0,0]
    for buffer_index in buffers.keys():
        _3d_index = numeric_to_3d_pos(buffer_index, buffers_partition, order='C')
        T, Cs = get_theta(buffers, buffer_index, _3d_index, O, B)
        for i in range(3):
            if T[i] > T_max[i]:
                T_max[i] = T[i]
    print(f"Found theta max: {T_max}")

    # get volumes to keep
    volumestokeep = [1]
    if B[1] > T_max[1]:
        print(f"{B[1]} > {T_max[1]}")
        volumestokeep.extend([2,3])
    if B[0] > T_max[0]:
        print(f"{B[0]} > {T_max[0]}")
        volumestokeep.extend([4,5,6,7])
    print(f"volumes to keep: {volumestokeep}")

    return volumestokeep


def keep_model_seeks(A, B, O, I):
    volumestokeep = get_volumes_to_keep(A, B, O)
    outfiles_partition = get_blocks_shape(A, O)
    outblocks = get_named_volumes(outfiles_partition, O)
    buffers = get_named_volumes(get_blocks_shape(A, B), B)
    arrays_dict, _, nb_file_openings, nb_inside_seeks = compute_zones_remake(B, O, A, volumestokeep, outfiles_partition, outblocks, buffers, False)

    W = [list(), list(), list()]
    nb_write_buffers = 0
    for outblock_index, write_buffers in arrays_dict.items():
        for write_buff in write_buffers:
            nb_write_buffers += 1
            p1, p2 = write_buff.get_corners()
            for d in range(3):
                if not p2[d] in W[d]:
                    W[d].append(p2[d])
    for d in range(3):
        W[d].sort()

    model_total = compute_keep_seeks_model(A, B, I, O, W, nb_write_buffers)
    
    return model_total, volumestokeep