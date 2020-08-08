from .utils import Volume
import logging 

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__ + 'policy_remake')


def get_dims_to_keep(volumestokeep):
    if 4 in volumestokeep:
        return [0,1,2]
    elif 3 in volumestokeep:
        return [1,2]
    elif 1 in volumestokeep:
        return [2]
    else:
        return list()


def get_grads(R, O, B, dims_to_keep):
    """ Grads corresponds to all the out borders of both output files and buffers.
    """
    grads_o = [set(range(O[i], R[i]+O[i], O[i])) for i in range(3)]
    grads_b = [set(range(B[i], R[i]+B[i], B[i])) for i in range(3)]
    grads = [set().union(grads_o[i], grads_b[i]) for i in range(3)]
    
    logger.debug("grads before merge:")
    logger.debug(grads)

    for i in range(3):
        if i in dims_to_keep:
            for buff_border in grads_b[i]:
                if not buff_border in grads_o[i]:
                    grads[i].remove(buff_border)
    return grads, [sorted(g) for g in grads_o]


def get_outfiles_parts(grads, grads_o, _3d_to_numeric_pos_dict):
    d = dict()

    grads = [sorted(g) for g in grads]
    prev_i = 0
    for i in range(0, len(grads[0])):

        prev_j = 0
        for j in range(0, len(grads[1])):

            prev_k = 0
            for k in range(0, len(grads[2])):
                add_to_dict(d, grads_o, Volume(0, (prev_i, prev_j, prev_k), (grads[0][i], grads[1][j], grads[2][k])), _3d_to_numeric_pos_dict)
                prev_k = grads[2][k]
            
            prev_j = grads[1][j]
        prev_i = grads[0][i]

    return d


def add_to_dict(d, grads_o, volume, _3d_to_numeric_pos_dict):
    """
        volume: volume to add to the dictionary 
    """
    outfile_pos = list()
    for dim in range(3):
        # logger.debug("dim: " + str(dim))
        for i, upper_border in enumerate(grads_o[dim]):
            if i != 0 :
                lower_border = grads_o[dim][i-1]
            else:
                lower_border = 0

            if upper_border >= volume.p2[dim] and lower_border <= volume.p1[dim]:
                outfile_pos.append(i)
                # logger.debug("upper border: " + str(upper_border))
                # logger.debug("lower border: " + str(lower_border))
                # logger.debug("pts: " + str(volume.get_corners()))


    outfile_pos = _3d_to_numeric_pos_dict[tuple(outfile_pos)]
    if not outfile_pos in d:
        d[outfile_pos] = list()
    d[outfile_pos].append(volume)


def get_pos_association_dict(volumestokeep, outfiles_partititon):
    index = 0
    _3d_to_numeric_pos_dict = dict()
    for i in range(outfiles_partititon[0]):
        for j in range(outfiles_partititon[1]):
            for k in range(outfiles_partititon[2]):
                _3d_to_numeric_pos_dict[(i,j,k)] = index
                index += 1
    return _3d_to_numeric_pos_dict


def compute_zones(B, O, R, volumestokeep, outfiles_partititon): #, buffers_partition, outfiles_partititon, buffers_volumes, outfiles_volumes):
    """ Main function of the module. Compute the "arrays" and "regions" dictionary for the resplit case.

    Arguments:
    ----------
        B: buffer shape
        O: output file shape
        R: shape of reconstructed image
        volumestokeep: volumes to be kept by keep strategy
    """
    _3d_to_numeric_pos_dict = get_pos_association_dict(volumestokeep, outfiles_partititon)
    return get_outfiles_parts(*get_grads(R, O, B, get_dims_to_keep(volumestokeep)), _3d_to_numeric_pos_dict)
    # return arrays_dict, buffer_to_outfiles, nb_file_openings, nb_inside_seeks