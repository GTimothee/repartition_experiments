import math, copy, logging
from repartition_experiments.algorithms.utils import *
from repartition_experiments.algorithms.tracker import Tracker
logger = logging.getLogger(__name__)

DEBUG_LOCAL=False

def get_main_volumes(B, T):
    """ I- Get a dictionary associating volume indices to volume positions in the buffer.
    Indexing following the keep algorithm indexing in storage order.
    Position following pillow indexing for rectangles i.e. (bottom left corner, top right corner)

    Arguments:
    ----------
        B: buffer shape
        T: Theta prime shape -> Theta value for C_x(n) (see paper)
    """
    logger.debug("\t== Function == get_main_volumes")
    logger.debug("\t[Arg] T: %s", T)

    main_volumes = [
        Volume(0,
               (0,0,0),
               (T[Axes.i.value], T[Axes.j.value], T[Axes.k.value]))]

    if B[Axes.k.value] > T[Axes.k.value]:
        main_volumes = main_volumes + [Volume(1,
               (0,0,T[Axes.k.value]),
               (T[Axes.i.value], T[Axes.j.value], B[Axes.k.value]))]
    
    if B[Axes.j.value] > T[Axes.j.value]:
        main_volumes.append(Volume(2,
                (0, T[Axes.j.value], 0),
                (T[Axes.i.value], B[Axes.j.value], T[Axes.k.value])))

        if B[Axes.k.value] > T[Axes.k.value]:
            main_volumes.append(Volume(3,
                    (0, T[Axes.j.value], T[Axes.k.value]),
                    (T[Axes.i.value], B[Axes.j.value], B[Axes.k.value])))
    
    if B[Axes.i.value] > T[Axes.i.value]:
        if B[Axes.j.value] > T[Axes.j.value]:
            main_volumes.append(
                Volume(4,
                    (T[Axes.i.value], 0, 0),
                    (B[Axes.i.value], T[Axes.j.value], T[Axes.k.value]))
            )
            if B[Axes.k.value] > T[Axes.k.value]:
                main_volumes.append(
                    Volume(5,
                        (T[Axes.i.value], 0, T[Axes.k.value]),
                        (B[Axes.i.value], T[Axes.j.value], B[Axes.k.value]))
                )

            main_volumes.append(
                Volume(6,
                    (T[Axes.i.value], T[Axes.j.value], 0),
                    (B[Axes.i.value], B[Axes.j.value], T[Axes.k.value]))
            )

            if B[Axes.k.value] > T[Axes.k.value]:
                main_volumes.append(
                    Volume(7,
                        (T[Axes.i.value], T[Axes.j.value], T[Axes.k.value]),
                        (B[Axes.i.value], B[Axes.j.value], B[Axes.k.value]))
                )
        else:
            main_volumes.append(
                Volume(4,
                    (T[Axes.i.value], 0, 0),
                    (B[Axes.i.value], T[Axes.j.value], T[Axes.k.value]))
            )

            if B[Axes.k.value] > T[Axes.k.value]:
                main_volumes.append(
                    Volume(5,
                        (T[Axes.i.value], 0, T[Axes.k.value]),
                        (B[Axes.i.value], T[Axes.j.value], B[Axes.k.value]))
                )
        
    return main_volumes


def add_offsets(volumes_list, _3d_index, B):
    """ III - Add offset to volumes positions to get positions in the reconstructed image.
    """
    offset = [B[dim] * _3d_index[dim] for dim in range(len(_3d_index))]
    for volume in volumes_list:
        volume.add_offset(offset)


def get_arrays_dict(buff_to_vols, buffers_volumes, outfiles_volumes, outfiles_partititon):
    """ IV - Assigner les volumes à tous les output files, en gardant référence du type de volume que c'est
    """
    print("== Function == get_arrays_dict")
    array_dict = dict()
    buffer_to_outfiles = dict()

    for buffer_index, volumes_in_buffer in buff_to_vols.items():
        buffer_of_interest = buffers_volumes[buffer_index]
        buffer_to_outfiles[buffer_index] = list()  # initialization
        # crossed_outfiles = get_crossed_outfiles(buffer_of_interest, outfiles_volumes) # refine search

        for volume_in_buffer in volumes_in_buffer:
            crossed=False
            for outfile_volume in outfiles_volumes.values(): # crossed_outfiles:
                if included_in(volume_in_buffer, outfile_volume):
                    add_to_array_dict(array_dict, outfile_volume, volume_in_buffer)
                    crossed=True
                    buffer_to_outfiles[buffer_index].append(outfile_volume.index)
                    break # a volume can belong to only one output file
            if not crossed:
                print("volume miss:")
                volume_in_buffer.print()
                
    # below lies a sanity check
    outfileskeys = list()
    for k, v in outfiles_volumes.items():
        outfileskeys.append(v.index)
    arraysdictkeys = list(array_dict.keys())
    missing_keys = set(outfileskeys) - set(arraysdictkeys)
    if not len(array_dict.keys()) == len(outfileskeys):
        print(f'len(array_dict.keys()): {len(arraysdictkeys)}')
        print(f'len(outfileskeys): {len(outfileskeys)}')
        print(f'nb missing keys: {len(missing_keys)}')
        raise ValueError("Something is wrong, not all output files will be written")
    return array_dict, buffer_to_outfiles


def merge_cached_volumes(arrays_dict, volumestokeep, outfiles_volumes):
    """ V - Pour chaque output file, pour chaque volume, si le volume doit être kept alors fusionner
    """
    logger.debug("== Function == merge_cached_volumes")
    print("== Function == merge_cached_volumes")
    merge_rules = get_merge_rules(volumestokeep)

    for outfileindex in sorted(list(arrays_dict.keys())):
        logger.debug("Treating outfile n°%s", outfileindex)
        volumes = arrays_dict[outfileindex]
        # print(f'volumes before:')
        # for v in volumes:
        #     v.print()
        
        for remainder_index in merge_rules.keys():
            for i in range(len(volumes)):
                
                name = volumes[i].index
                index = int(name.split('_')[0])
                if index == remainder_index:
                    volumetomerge = volumes.pop(i)
                    merge_directions = merge_rules[index]
                    new_volume = apply_merge(volumetomerge, volumes, merge_directions)  # also pops the volumes that are merged with
                    volumes.append(new_volume)
                    break

        tracker = Tracker()
        for v in volumes:
            tracker.add_volume(v)
        try:
            assert tracker.is_complete(outfiles_volumes[outfileindex].get_corners())
        except Exception as e:
            print(f'outfile of interest: ')
            outfiles_volumes[outfileindex].print()
            
            print(f'volumes:')
            for v in volumes:
                v.print()
            raise e

        arrays_dict[outfileindex] = volumes

    logger.debug("End\n")


def get_merge_rules(volumestokeep):
    """ Get merge rules corresponding to volumes to keep.
    See thesis for explanation of the rules.
    """
    rules = {
        1: [Axes.k] if 1 in volumestokeep else None,
        2: [Axes.j] if 2 in volumestokeep else None,
        3: [Axes.k] if 3 in volumestokeep else None,
        4: [Axes.i] if 4 in volumestokeep else None,
        5: [Axes.k] if 5 in volumestokeep else None,
        6: [Axes.j] if 6 in volumestokeep else None,
        7: [Axes.k, Axes.j] if 7 in volumestokeep else None
    }
    rules[3].append(Axes.j) if 2 in volumestokeep else None
    for i in [5,6,7]:
        rules[i].append(Axes.i) if 4 in volumestokeep else None
    for k in list(rules.keys()):
        if rules[k] == None:
            del rules[k]  # see usage in merge_cached_volumes
    return rules


def get_regions_dict(array_dict, outfiles_volumes):
    """ Create regions dict from arrays dict by removing output file offset (low corner) from slices.
    """
    print("== Function == get_regions_dict")
    regions_dict = copy.deepcopy(array_dict)

    slice_to_list = lambda s: [s.start, s.stop, s.step]
    list_to_slice = lambda s: slice(s[0], s[1], s[2])

    for v in outfiles_volumes.values():
        p1 = v.p1 # (x, y, z)
        outputfile_data = regions_dict[v.index]

        for i in range(len(outputfile_data)):
            slices_list = outputfile_data[i]
            s1, s2, s3 = slices_list

            s1 = slice_to_list(s1) # start, stop, step
            s2 = slice_to_list(s2) # start, stop, step
            s3 = slice_to_list(s3) # start, stop, step
            slices_list = [s1, s2, s3]

            for dim in range(3):
                s = slices_list[dim]
                s[0] -= p1[dim]
                s[1] -= p1[dim]
                slices_list[dim] = list_to_slice(s)

            outputfile_data[i] = tuple(slices_list)
    return regions_dict


def split_main_volumes(volumes_list, O):
    """ Split the remainder volumes into volumes by the boundaries of the output files.
    """
    
    def get_dim_pts(bound, it, step, pts_list):
        while it > bound:
            it -= step
            if it > bound:
                pts_list.append(it)
    
    def get_points(volume, O):
        upright_corner = volume.p2
        botleft_corner = volume.p1

        Oi, Oj, Ok = O
        i_max, j_max, k_max = upright_corner
        i_min, j_min, k_min = botleft_corner
        pts_i, pts_j, pts_k = [i_max, i_min], [j_max, j_min], [k_max, k_min] # add borders of volume to points
        
        first_outfile_borders = (i_max - (i_max % Oi), j_max - (j_max % Oj), k_max - (k_max % Ok)) # compute first outfile borders
        fi, fj, fk = first_outfile_borders
        if fi > i_min:
            i_max = fi
            if not i_max in pts_i: # if first outfile borders != max then add it to points too
                pts_i.append(i_max)
        if fj > j_min:
            j_max = fj
            if not j_max in pts_j:
                pts_j.append(j_max)
        if fk > k_min:
            k_max = fk
            if not k_max in pts_k:
                pts_k.append(k_max)     

        get_dim_pts(i_min, i_max, Oi, pts_i) # from max, compute the other outfiles borders and add it to points
        get_dim_pts(j_min, j_max, Oj, pts_j)
        get_dim_pts(k_min, k_max, Ok, pts_k)
        return (sorted(pts_i), sorted(pts_j), sorted(pts_k))

    def get_volumes_from_points(volume, points):
        i, j, k = volume.p1
        pts_i, pts_j, pts_k = points
        
        index = 0
        remainder_hid_vols = list()
        for i in range(len(pts_i)-1):
            for j in range(len(pts_j)-1):
                for k in range(len(pts_k)-1):
                    name = str(volume.index) + '_' + str(index)
                    botleft_corner = (pts_i[i], pts_j[j], pts_k[k])
                    upright_corner = (pts_i[i+1], pts_j[j+1], pts_k[k+1])
                    new_vol = Volume(name, botleft_corner, upright_corner)
                    remainder_hid_vols.append(new_vol)
                    index += 1
        
        return remainder_hid_vols

    split_volumes = list()
    for volume in volumes_list:
        points = get_points(volume, O)
        hid_vols = get_volumes_from_points(volume, points)
        split_volumes.extend(hid_vols)
    return split_volumes


def get_buff_to_vols(R, B, O, buffers_volumes, buffers_partition):
    """ Outputs a dictionary associating buffer_index to list of Volumes indexed as in paper.
    """

    def get_theta(buffers_volumes, buffer_index, _3d_index, O, B):
        T = list()
        Cs = list()
        for dim in range(len(buffers_volumes[buffer_index].p1)):
            if B[dim] < O[dim]:
                C = 0 
            else:            
                C = ((_3d_index[dim]+1) * B[dim]) % O[dim]
                # print(f'{((_3d_index[dim]+1) * B[dim])}mod{O[dim]} = {C}')
                if C == 0 and B[dim] != O[dim]:  # particular case 
                    C = O[dim]

            if C < 0:
                raise ValueError("modulo should not return negative value")

            Cs.append(C)
            T.append(B[dim] - C)   
        # print(f'C: {Cs}')
        # print(f'theta: {T}')
        return T

    def first_sanity_check(buffers_volumes, buffer_index, volumes_list):
        """ see if volumes coordinates found are inside buffer
        Arguments:
        ----------
            buffers_volumes: dict containing all buffers (Volumes)
            buffer_index: index of current buffer
            volumes_list: list of volumes found in buffer
        """
        xs, ys, zs = list(), list(), list()
        for volume in volumes_list:
            x1, y1, z1 = volume.p1
            x2, y2, z2 = volume.p2 
            xs.append(x1)
            xs.append(x2)
            ys.append(y1)
            ys.append(y2)
            zs.append(z1)
            zs.append(z2)
        err = -1
        buff_vol = buffers_volumes[buffer_index]
        if not min(xs) == buff_vol.p1[0]:
            err = 0
        if not min(ys) == buff_vol.p1[1]:
            err = 1
        if not min(zs) == buff_vol.p1[2]:
            err = 2
        if not max(xs) == buff_vol.p2[0]:
            err = 3
        if not max(ys) == buff_vol.p2[1]:
            err = 4
        if not max(zs) == buff_vol.p2[2]:
            err = 5
        if err > -1:
            print(f'buffer lower corner: {buff_vol.p1}')
            print(f'volumes lower corner: {(min(xs), min(ys), min(zs))}')
            print(f'buffer upper corner: {buff_vol.p2}')
            print(f'volumes upper corner: {(max(xs), max(ys), max(zs))}')
            raise ValueError("[get_buff_to_vols] Error " + str(err))

    def second_sanity_check(B, O, volumes_list):
        """ see if sum of all volumes equals the volume of the buffer 
        + see if each volume is <= volume of an output file as a volume cannot be bigger than an output file
        """
        volumes_volume = 0
        buffer_volume = B[0]*B[1]*B[2]
        outfile_volume = O[0]*O[1]*O[2]
        for volume in volumes_list:
            x1, y1, z1 = volume.p1
            x2, y2, z2 = volume.p2 
            vol = (x2-x1)*(y2-y1)*(z2-z1)

            if vol > outfile_volume:
                print(f'Outfile volume: {outfile_volume}')
                print(f'Volume considered: {vol}')
                raise ValueError("A volume should not be bigger than outfile")

            volumes_volume += vol

        if buffer_volume != volumes_volume:
            print(f'Buffer volume: {buffer_volume}')
            print(f'Sum of volumes: {volumes_volume}')
            raise ValueError("sum of volumes should be equal to buffer volume")

    logger.debug("== Function == get_buff_to_vols")
    print("== Function == get_buff_to_vols")
    buff_to_vols = dict()
    
    rows = list()
    for buffer_index in buffers_volumes.keys():
        # print(f'\nProcessing buffer {buffer_index}')
        if DEBUG_LOCAL:
            buffers_volumes[buffer_index].print()
        _3d_index = numeric_to_3d_pos(buffer_index, buffers_partition, order='C')

        T = get_theta(buffers_volumes, buffer_index, _3d_index, O, B)
        volumes_list = get_main_volumes(B, T)  # get coords in basis of buffer
        add_offsets(volumes_list, _3d_index, B)  # convert coords in basis of R - WARNING: important to be in this order, we need basis R for split_main_volumes
        
        tracker = Tracker()
        for v in volumes_list:
            tracker.add_volume(v)
        try:
            assert tracker.is_complete(buffers_volumes[buffer_index].get_corners())
        except:
            print("\n-----------get_main_volumes error")
            print("buffer of interest:")
            buffers_volumes[buffer_index].print()
            print(f"B: {B}")
            print("volumes found")
            for v in volumes_list:
                v.print()
            raise ValueError()

        if DEBUG_LOCAL:
            print('Main volumes found:')
            for v in volumes_list:
                v.print()

        volumes_list = split_main_volumes(volumes_list, O) # seek for hidden volumes in main volumes
        if DEBUG_LOCAL:
            print('Split volumes found:')
            for v in volumes_list:
                v.print()

        tracker = Tracker()
        for v in volumes_list:
            tracker.add_volume(v)
        assert tracker.is_complete(buffers_volumes[buffer_index].get_corners())
        
        if DEBUG_LOCAL:
            print('\nVolumes found:')
            for v in volumes_list:
                v.print()
            print('\n')

        first_sanity_check(buffers_volumes, buffer_index, volumes_list)
        second_sanity_check(B, O, volumes_list)

        buff_to_vols[buffer_index] = volumes_list
        
    logger.debug("End\n")
    return buff_to_vols


def compute_zones(B, O, R, volumestokeep, buffers_partition, outfiles_partititon, buffers_volumes, outfiles_volumes):
    """ Main function of the module. Compute the "arrays" and "regions" dictionary for the resplit case.

    Arguments:
    ----------
        B: buffer shape
        O: output file shape
        R: shape of reconstructed image
        volumestokeep: volumes to be kept by keep strategy
    """
    logger.debug("\n\n-----------------Compute zones [main file function]-----------------\n\n")

    if DEBUG_LOCAL:
        print(f'Buffers found:')
        for i, buff in buffers_volumes.items():
            buff.print()
        print('Outfile volumes:')
        for k, outfiles_volume in outfiles_volumes.items():
            outfiles_volume.print()
        print(f'buffers partition: {buffers_partition}')
        print(f'outfiles partition: {outfiles_partititon}')

    buff_to_vols = get_buff_to_vols(R, B, O, buffers_volumes, buffers_partition)
    arrays_dict, buffer_to_outfiles = get_arrays_dict(buff_to_vols, buffers_volumes, outfiles_volumes, outfiles_partititon) 

    # sanity check
    outvolumes_trackers_preprocess = dict()
    for index, outvol in outfiles_volumes.items():
        outvolumes_trackers_preprocess[index] = Tracker()
    for k, v_list in arrays_dict.items():
        for v in v_list:
            outvolumes_trackers_preprocess[k].add_volume(v)
        assert outvolumes_trackers_preprocess[k].is_complete(outfiles_volumes[k].get_corners())

    merge_cached_volumes(arrays_dict, volumestokeep, outfiles_volumes)

    # sanity check
    outvolumes_trackers_preprocess = dict()
    for index, outvol in outfiles_volumes.items():
        outvolumes_trackers_preprocess[index] = Tracker()
    for k, v_list in arrays_dict.items():
        for v in v_list:
            outvolumes_trackers_preprocess[k].add_volume(v)
        assert outvolumes_trackers_preprocess[k].is_complete(outfiles_volumes[k].get_corners())

    # compute number of seeks
    nb_file_openings = 0
    nb_inside_seeks = 0
    for outfile_index, volumes_list in arrays_dict.items():
        nb_file_openings += len(volumes_list)
        outfile_shape = outfiles_volumes[outfile_index].get_shape()

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

    logger.debug("-----------------End Compute zones-----------------")
    return arrays_dict, buffer_to_outfiles, nb_file_openings, nb_inside_seeks