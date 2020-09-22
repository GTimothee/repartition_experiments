from .utils import Volume, hypercubes_overlap
from .tracker import Tracker
import logging, copy, sys

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__ + 'policy_remake')


DEBUG = False


def get_dims_to_keep(volumestokeep):
    if 4 in volumestokeep:
        return [0,1,2]
    elif 3 in volumestokeep:
        return [1,2]
    elif 1 in volumestokeep:
        return [2]
    else:
        return list()


def get_grads(R, O, B):
    """ Grads corresponds to all the out borders of both output files and buffers. (= cuts)

    Outputs (computed): 
    --------
        grads: ordered list of all cuts, with different labels (b, o or ob) (1 set per dimension) -> initialization : grads = [set(), set(), set()]
            list of list of tuples 
            label b => buffer cut
            label o => output block cut 
            label ob => shape match between buffer and output block
        grads_o: all cuts from output blocks
            list of list of values
        remainder_markers: all thetas cuts
   """
    if DEBUG:
        print("args: ", R, O, B)
    grads_o = [{e for e in range(O[i], R[i]+O[i], O[i])} for i in range(3)] 
    grads_b = [{(e, "b") for e in range(B[i], R[i]+B[i], B[i])} for i in range(3)] 

    grads_o_copy = copy.deepcopy(grads_o)

    # creates "grads" (sets of all cuts, with different labels, see below)
    # foreach grad of grad_b: 
    grads = [set(), set(), set()] 
    for i in range(3):
        for e in grads_b[i]:
            if e[0] not in grads_o_copy[i]: # add grad to grads if grad not in grads_o (added with label "b" for buffer)
                grads[i].add(e)
            else: # if grad in both o and b, add tuple: (grad, "ob") to mark (/label) it as a shape match
                grads[i].add((e[0],"ob"))
                grads_o_copy[i].remove(e[0])

        grads[i] = set(map(lambda e: (e,"o"), grads_o_copy[i])).union(grads[i]) # mark all grads_o with an "o" and merge the sets
    
    # creates a list of thetas values (see the nomenclature in paper)
    remainder_markers = grads_b
    grads = [sorted(g, key=lambda e: e[0]) for g in grads]
    for i in range(3): # in each dimension
        for j, e in enumerate(grads[i]): # for each grad of grads
            if j == len(grads[i]) - 1: 
                break 

            # mark the thetas
            if (grads[i][j+1][1] == "b" or grads[i][j+1][1] == "ob") and e[1] != "b" and e[1] != "ob":
                remainder_markers[i].add((e[0], "t")) # theta

    return grads, [sorted(g) for g in grads_o], [sorted(g) for g in remainder_markers]


def get_outfiles_parts(grads, grads_o, remainder_markers, _3d_to_numeric_pos_dict, dims_to_keep):
    """ Actually computes the write buffers.

    Arguments: 
    ----------
        grads: ordered list of all cuts, with different labels (b, o or ob) (1 set per dimension) -> initialization : grads = [set(), set(), set()]
            list of list of tuples 
            label b => buffer cut
            label o => output block cut 
            label ob => shape match between buffer and output block
        grads_o: all cuts from output blocks
            list of list of values
        remainder_markers: all thetas cuts
        _3d_to_numeric_pos_dict: dictionary to converts 3d index to numeric index
        dims_to_keep: dimensions in which remainder volumes must be kept

    "marker" == "label"
    """
    d = dict()
    if DEBUG:
        print("grads before merge:")
        print(grads)
        print("remainder_markers:")
        print(remainder_markers)
        print("dims to keep:")
        print(dims_to_keep)

    prev_i = 0
    mark_i = "None"
    x_i = 0
    for i in range(0, len(grads[0])):

        if grads[0][i][1] == "b" and 0 in dims_to_keep and mark_i == "F4":
            marker_i = remainder_markers[0][x_i]

            if DEBUG:
                print("fuse in i")
                print("marker_i: " + str(marker_i))

            if marker_i[1] == "b":
                mark_i = "None"
            else:
                mark_i = "F4"
            if marker_i[0] == grads[0][i][0]:
                x_i += 1
            continue

        prev_j = 0
        mark_j = "F1"
        x_j = 0
        for j in range(0, len(grads[1])):
            if DEBUG:
                print("current grad j : " + str(grads[1][j]))
                print("mark j : " + str(mark_j))

            fuse_in_j = False
            if grads[1][j][1] == "b" and 1 in dims_to_keep and mark_j == "F2/F3":
                fuse_in_j = True
                if DEBUG:
                    print("fuse in j")

            if fuse_in_j:
                marker_j = remainder_markers[1][x_j]
                if DEBUG:
                    print("marker_j: " + str(marker_j))
                if marker_j[1] == "b":
                    mark_j = "F1"
                else:
                    mark_j = "F2/F3"
                if marker_j[0] == grads[1][j][0]:
                    x_j += 1
                continue

            prev_k = 0
            for k in range(0, len(grads[2])):
                if grads[2][k][1] == "b" and 2 in dims_to_keep and mark_j == "F1":
                    if DEBUG:
                        print("fuse in k")
                        print(f"pass buffer marker: {grads[2][k][0]}")
                    continue 
                elif grads[2][k][1] == "b" and 2 in dims_to_keep and mark_j == "F2/F3" and 1 in dims_to_keep:
                    if DEBUG:
                        print("fuse in k bec fuse in j")
                        print(f"pass buffer marker: {grads[2][k][0]}")
                    continue 

                # else
                d = add_to_dict(d, grads_o, Volume(0, (prev_i, prev_j, prev_k), (grads[0][i][0], grads[1][j][0], grads[2][k][0])), _3d_to_numeric_pos_dict)
                prev_k = grads[2][k][0]

            marker_j = remainder_markers[1][x_j]
            if DEBUG:
                print("marker_j: " + str(marker_j))
            if marker_j[1] == "b":
                mark_j = "F1"
            else:
                mark_j = "F2/F3"
            if marker_j[0] == grads[1][j][0]:
                x_j += 1

            prev_j = grads[1][j][0]

        marker_i = remainder_markers[0][x_i]
        if DEBUG:
            print("marker_i: " + str(marker_i))
        if marker_i[1] == "b":
            mark_i = "None"
        else:
            mark_i = "F4"
        if marker_i[0] == grads[0][i][0]:
            x_i += 1

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

    return d


def get_pos_association_dict(volumestokeep, outfiles_partititon):
    """ Converts 3d index to numeric index
    """
    index = 0
    _3d_to_numeric_pos_dict = dict()
    for i in range(outfiles_partititon[0]):
        for j in range(outfiles_partititon[1]):
            for k in range(outfiles_partititon[2]):
                _3d_to_numeric_pos_dict[(i,j,k)] = index
                index += 1
    return _3d_to_numeric_pos_dict


def compute_zones_remake(B, O, R, volumestokeep, outfiles_partititon, out_volumes, buffers, get_buffer_to_outfiles):
    """ Compute the write buffers.

    Arguments:
    ----------
        B: buffer shape
        O: output file shape
        R: shape of reconstructed image
        volumestokeep: volumes to be kept by keep strategy
        outfiles_partititon: number of outblocks per dimension
        out_volumes: dict outfile numeric position -> Volume
        buffers: dictionary mapping each read buffer index (following the buffer order k,j,i) to the Volume representing the buffer
        get_buffer_to_outfiles: if True, also return the buffer_to_outfiles dictionary
    """
    # preprocessing
    _3d_to_numeric_pos_dict = get_pos_association_dict(volumestokeep, outfiles_partititon)
    dims_to_keep = get_dims_to_keep(volumestokeep)

    # compute the write buffers
    arrays_dict =  get_outfiles_parts(*get_grads(R, O, B), _3d_to_numeric_pos_dict, dims_to_keep)
    
    # print(f"arrays_dict nb keys: {len(arrays_dict.keys())}")

    # sanity check
    if DEBUG:
        print("sanity check...")
        for k, subvol_list in arrays_dict.items():
            t = Tracker()
            for v in subvol_list:
                t.add_volume(v)
            assert t.is_complete(out_volumes[k].get_corners())

    # compute number of seeks
    if DEBUG:
        print("computing nb seeks...")
    nb_file_openings = 0
    nb_inside_seeks = 0
    for outfile_index, volumes_list in arrays_dict.items():
        nb_file_openings += len(volumes_list)
        outfile_shape = out_volumes[outfile_index].get_shape()

        for v in volumes_list:
            s = v.get_shape()
            
            if s[2] != outfile_shape[2]:
                nb_inside_seeks += s[0]*s[1]
            elif s[1] != outfile_shape[1]:
                nb_inside_seeks += s[0]
            elif s[0] != outfile_shape[0]:
                nb_inside_seeks += 1
            else:
                pass

    if DEBUG:
        print(f"nb seeks: {nb_file_openings}, {nb_inside_seeks}")

    # create buffer_to_outfiles
    buffer_to_outfiles = dict()
    if get_buffer_to_outfiles:
        for outvolume in out_volumes.values():
            for buffer in buffers.values():
                if hypercubes_overlap(outvolume, buffer):
                    if not buffer.index in buffer_to_outfiles:
                        buffer_to_outfiles[buffer.index] = list()
                    buffer_to_outfiles[buffer.index].append(outvolume.index)

    return arrays_dict, buffer_to_outfiles, nb_file_openings, nb_inside_seeks