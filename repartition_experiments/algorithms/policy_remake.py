from .utils import Volume
import logging, copy

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

    # créer liste de tuples
    # ne pas ajouter de b si jamais il existe déjà dans grads_o -> on ne le considère pas comme une bordure d'intérêt car il n'y a pas de remainder volume du coup
    # faire un map sur les grads_b et faire un add au lieu de ci-dessous
    """
    
    grads_o = [{e for e in range(O[i], R[i]+O[i], O[i])} for i in range(3)] 
    grads_b = [{(e, "b") for e in range(B[i], R[i]+B[i], B[i])} for i in range(3)] 

    grads = [set(), set(), set()] 
    for i in range(3):
        for e in grads_b[i]:
            if e[0] not in grads_o[i]:
                grads[i].add(e)
            else:
                grads[i].add((e[0],"ob"))
                grads_o[i].remove(e[0])

        grads[i] = set(map(lambda e: (e,"o"), grads_o[i]))
    
    remainder_markers = grads_b
    grads = [sorted(g, key=lambda e: e[0]) for g in grads]
    for i in range(3):
        for j, e in enumerate(grads[i]):
            if j == len(grads[i]) - 1:
                break 

            if grads[i][j+1][1] == "b" or grads[i][j+1][1] == "ob": # TODO: rajouter les b qui matchent o
                remainder_markers[i].add((e[0], "t")) # theta
    
    return grads, [sorted(g) for g in grads_o], [sorted(g) for g in remainder_markers]


def get_outfiles_parts(grads, grads_o, remainder_markers, _3d_to_numeric_pos_dict, dims_to_keep):
    """
        grads: list of list of tuples
        grads_o: list of list of values
    """
    d = dict()
    logger.debug("grads before merge:")
    logger.debug(grads)
    logger.debug("remainder_markers:")
    logger.debug(remainder_markers)
    logger.debug("dims to keep:")
    logger.debug(dims_to_keep)

    prev_i = 0
    for i in range(0, len(grads[0])):

        prev_j = 0
        mark_j = "F1"
        x_j = 0
        for j in range(0, len(grads[1])):
            logger.debug("current grad j : " + str(grads[1][j]))
            logger.debug("mark j : " + str(mark_j))
            prev_k = 0
            for k in range(0, len(grads[2])):
                if grads[2][k][1] == "b":
                    if 2 in dims_to_keep and mark_j == "F1":
                        logger.debug("fuse in k")
                        continue 
                    elif 1 in dims_to_keep and mark_j == "F2/F3":
                        logger.debug("fuse in j")
                        continue

                # else
                logger.debug("adding")
                add_to_dict(d, grads_o, Volume(0, (prev_i, prev_j, prev_k), (grads[0][i][0], grads[1][j][0], grads[2][k][0])), _3d_to_numeric_pos_dict)
                prev_k = grads[2][k][0]
            
            marker = remainder_markers[1][x_j]
            logger.debug("i,j,k: " + str(i) + "," + str(j) + "," + str(k))
            logger.debug("marker: " + str(marker))
            if marker[1] == "b":
                mark_j = "F1"
            else:
                mark_j = "F2/F3"
            if marker[0] == grads[1][j][0]:
                x_j += 1

            prev_j = grads[1][j][0]
        prev_i = grads[0][i][0]

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


def compute_zones(B, O, R, volumestokeep, outfiles_partititon):
    """ Main function of the module. Compute the "arrays" and "regions" dictionary for the resplit case.

    Arguments:
    ----------
        B: buffer shape
        O: output file shape
        R: shape of reconstructed image
        volumestokeep: volumes to be kept by keep strategy
    """
    _3d_to_numeric_pos_dict = get_pos_association_dict(volumestokeep, outfiles_partititon)
    dims_to_keep = get_dims_to_keep(volumestokeep)

    arrays_dict =  get_outfiles_parts(*get_grads(R, O, B, dims_to_keep), _3d_to_numeric_pos_dict, dims_to_keep)
    buffer_to_outfiles = None
    nb_file_openings = None
    nb_inside_seeks = None
    return arrays_dict, buffer_to_outfiles, nb_file_openings, nb_inside_seeks