import argparse, logging, json, sys
from .algorithms.utils import get_blocks_shape, get_named_volumes, numeric_to_3d_pos, get_theta


DEBUG=False
def compute_max_mem(R, B, O, nb_bytes_per_voxel):
    """ Algorithm to compute the maximum amount of memory to be consumed by the keep algorithm.
    """
    buffers_partition = get_blocks_shape(R, B)
    buffers_volumes = get_named_volumes(buffers_partition, B)

    # create lists of remainders
    k_remainder_list = [0]
    j_remainder_list = [0] * buffers_partition[2]
    i_remainder_list = [0] * (buffers_partition[2] * buffers_partition[1])

    if DEBUG:
        print(f"Image partition by B: {buffers_partition}")
        print(f"Lists initialization...")
        print(f"k: {k_remainder_list}")
        print(f"j: {j_remainder_list}")
        print(f"i: {i_remainder_list}")

    nb_voxels_max = B[0] * B[1] * B[2]
    nb_voxels = B[0] * B[1] * B[2]

    if DEBUG:
        print(f"Initialization nb voxels (=1 buffer): {nb_voxels}")
    i, j, k = 0, 1, 2
    for buffer_index in buffers_volumes.keys():
        if DEBUG:
            print(f"Processing buffer {buffer_index}")
        _3d_index = numeric_to_3d_pos(buffer_index, buffers_partition, order='C')
        theta, omega = get_theta(buffers_volumes, buffer_index, _3d_index, O, B)
        if DEBUG:
            print(f"3d buffer index: {_3d_index}")

        F1 = omega[k] * theta[j] * theta[i]
        F2 = theta[k] * omega[j] * theta[i]
        F3 = omega[k] * omega[j] * theta[i]
        F4 = theta[k] * theta[j] * omega[i]
        F5 = omega[k] * theta[j] * omega[i]
        F6 = theta[k] * omega[1] * omega[i]
        F7 = omega[k] * omega[j] * omega[i]

        if theta[i] >= O[i] and theta[j] >= O[j] and omega[k]  >= O[k]:
            F1 = 0
        if theta[i] >= O[i] and omega[j] >= O[j] and theta[k]  >= O[k]:
            F2 = 0
        if theta[i] >= O[i] and omega[j] >= O[j] and omega[k]  >= O[k]:
            F3 = 0
        if omega[i] >= O[i] and theta[j] >= O[j] and theta[k]  >= O[k]:
            F4 = 0
        if omega[i] >= O[i] and theta[j] >= O[j] and omega[k]  >= O[k]:
            F5 = 0
        if omega[i] >= O[i] and omega[j] >= O[j] and theta[k]  >= O[k]:
            F6 = 0
        if omega[i] >= O[i] and omega[j] >= O[j] and omega[k]  >= O[k]:
            F7 = 0

        k_remainder = F1
        j_remainder = F2 + F3
        i_remainder = F4 + F5 + F6 + F7

        index_j = _3d_index[2]
        index_i = _3d_index[1]*len(j_remainder_list) + _3d_index[2]
        if DEBUG:
            print(f"Indices: {index_j}, {index_i}")
            print(f"Lengths: {len(j_remainder_list)}, {len(i_remainder_list)}")

        nb_voxels -= k_remainder_list[0] + j_remainder_list[index_j] + i_remainder_list[index_i]

        k_remainder_list[0] = k_remainder
        j_remainder_list[index_j] = j_remainder
        i_remainder_list[index_i] = i_remainder

        nb_voxels += k_remainder_list[0] + j_remainder_list[index_j] + i_remainder_list[index_i]

        if DEBUG:
            print(f"k: {k_remainder_list}")
            print(f"j: {j_remainder_list}")
            print(f"i: {i_remainder_list}")
            print(f"Number of voxels: {nb_voxels}")

        if nb_voxels > nb_voxels_max:
            nb_voxels_max = nb_voxels

    if DEBUG:
        print(f"Number of voxels max: {nb_voxels_max}")
        print(f"RAM consumed: {nb_voxels_max * nb_bytes_per_voxel}")
    return nb_voxels_max