import operator, logging, math, psutil
from enum import Enum

from repartition_experiments.file_formats.hdf5 import HDF5_manager

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Axes(Enum):
    i = 0
    j = 1
    k = 2


class Volume:
    def __init__(self, index, p1, p2):
        if (not isinstance(p1, tuple) 
            or not isinstance(p2, tuple)):
            raise TypeError()

        self.index = index
        self.p1 = p1  # bottom left corner
        self.p2 = p2  # top right corner


    def get_shape(self):
        return (self.p2[0] - self.p1[0], self.p2[1] - self.p1[1], self.p2[2] - self.p1[2])


    def get_slices(self):
        return ((self.p1[0], self.p2[0]), (self.p1[1], self.p2[1]), (self.p1[2], self.p2[2]))

    def add_offset(self, offset):
        """
        offset: a tuple
        """
        self.p1 = self._add_offset(self.p1, offset)
        self.p2 = self._add_offset(self.p2, offset)
            

    def _add_offset(self, p, offset):
        if isinstance(offset, list):
            offset = tuple(offset)
        elif not isinstance(offset, tuple):
            raise TypeError("Expected tuple")
        return tuple(map(operator.add, p, offset))


    def get_corners(self):
        return (self.p1, self.p2)


    def equals(self, volume):
        if not self.index == volume.index:
            return False 
        if not self.p1 == volume.p1:
            return False 
        if not self.p2 == volume.p2:
            return False 
        return True

    def print(self):
        print(f"Volume name: {self.index}, ({self.p1[0]}:{self.p2[0]},{self.p1[1]}:{self.p2[1]},{self.p1[2]}:{self.p2[2]}), shape:({self.p2[0]-self.p1[0]},{self.p2[1]-self.p1[1]},{self.p2[2]-self.p1[2]})")


def get_opened_files():
    proc = psutil.Process()
    print(f"Number of opened files: {len(proc.open_files())}") 


def get_volumes(R, B):
    """ Returns a dictionary mapping each buffer (numeric) index to a Volume object containing its coordinates in R.

    Arguments: 
    ----------
        R: original array
        B: buffer shape
    """
    buffers_partition = get_partition(R, B)
    return buffers_partition, get_named_volumes(buffers_partition, B)


def hypercubes_overlap(hypercube1, hypercube2):
    """ Evaluate if two hypercubes cross each other.
    """
    if not isinstance(hypercube1, Volume) or \
        not isinstance(hypercube2, Volume):
        raise TypeError()

    lowercorner1, uppercorner1 = hypercube1.get_corners()
    lowercorner2, uppercorner2 = hypercube2.get_corners()
    nb_dims = len(uppercorner1)
    
    nb_matching_dims = 0
    for i in range(nb_dims):
        if uppercorner1[i] <= lowercorner2[i] or \
            uppercorner2[i] <= lowercorner1[i]:
            return False
        elif uppercorner1[i] == uppercorner2[i] and lowercorner1[i] == lowercorner2[i]:
            nb_matching_dims += 1 

    if nb_matching_dims == nb_dims: # if corners are the same
        return True

    return True


def get_blocks_shape(big_array, small_array):
    """ Return the number of small arrays in big array in all dimensions as a shape. 
    """
    return tuple([int(b/s) for b, s in zip(big_array, small_array)])


def get_crossed_outfiles(buffer_of_interest, outfiles_volumes):
    """ Returns list of output files that are crossing buffer at buffer_index.

    Arguments: 
    ----------
        outfiles_volumes: dict of volumes representing the output files, indexed in storage order.
    """
    crossing = list()
    for outfile in outfiles_volumes.values():
        if hypercubes_overlap(buffer_of_interest, outfile):
            crossing.append(outfile)  # we add a Volume obj
    return crossing


def merge_volumes(volume1, volume2):
    """ Merge two volumes into one.
    """
    if not isinstance(volume1, Volume) or \
        not isinstance(volume2, Volume):
        raise TypeError()

    lowercorner1, uppercorner1 = volume1.get_corners()
    lowercorner2, uppercorner2 = volume2.get_corners()
    lowercorner = (min(lowercorner1[0], lowercorner2[0]), 
                   min(lowercorner1[1], lowercorner2[1]),
                   min(lowercorner1[2], lowercorner2[2]))
    uppercorner = (max(uppercorner1[0], uppercorner2[0]), 
                   max(uppercorner1[1], uppercorner2[1]),
                   max(uppercorner1[2], uppercorner2[2]))
    return Volume('0_merged', lowercorner, uppercorner)


def included_in(volume, outfile):
    """ Alias of hypercubes_overlap. 
    We do not verify that it is included but by definition
    of the problem if volume crosses outfile then volume in outfile.

    Arguments: 
    ----------
        volume: Volume in buffer
        outfile: Volume representing an output file
    """
    if not isinstance(volume, Volume) or \
        not isinstance(outfile, Volume):
        raise TypeError()

    volume_bl, volume_ur = volume.get_corners()  # ur=upper right, bl=bottom left
    outfile_bl, outfile_ur = outfile.get_corners()

    nb_dims = len(outfile_bl)
    nb_matching_dims = 0
    for dim in range(nb_dims):
        out_min, out_max = outfile_bl[dim], outfile_ur[dim]
        volume_min, volume_max = volume_bl[dim], volume_ur[dim]
        if (volume_min >= out_min and volume_min <= out_max) and (volume_max >= out_min and volume_max <= out_max):
            nb_matching_dims += 1

    if nb_matching_dims == nb_dims:
        return True

    return False


def add_to_array_dict(array_dict, outfile, volume):
    """ Add volume information to dictionary associating output file index to 

    Arguments:
    ----------
        outfile: outfile volume
        volume: volume from buffer
    """
    if (not isinstance(outfile.index, int) 
        or not isinstance(volume, Volume) 
        or not isinstance(outfile, Volume)):
        raise TypeError()

    if not outfile.index in array_dict.keys():
        array_dict[outfile.index] = list()
    array_dict[outfile.index].append(volume)


def convert_Volume_to_slices(v):
    if not isinstance(v, Volume):
        raise TypeError()
    p1, p2 = v.get_corners()
    return tuple([slice(p1[dim], p2[dim], None) for dim in range(len(p1))])


def clean_arrays_dict(arrays_dict):
    """ From a dictionary of Volumes, creates a dictionary of list of slices.
    The new arrays_dict associates each output file to each volume that must be written at a time.
    """
    for k in arrays_dict.keys():
        volumes_list = arrays_dict[k]
        arrays_dict[k] = [convert_Volume_to_slices(v) for v in volumes_list]


def get_overlap_subarray(hypercube1, hypercube2):
    """ Find the intersection of both files.
    Refactor of hypercubes_overlap to return the overlap subarray

    Returns: 
    --------
        pair of corners of the subarray
    See also:
    ---------
        utils.hypercubes_overlap
    """

    if not isinstance(hypercube1, Volume) or \
        not isinstance(hypercube2, Volume):
        raise TypeError()

    lowercorner1, uppercorner1 = hypercube1.get_corners()
    lowercorner2, uppercorner2 = hypercube2.get_corners()
    nb_dims = len(uppercorner1)
    
    subarray_lowercorner = list()
    subarray_uppercorner = list()
    for i in range(nb_dims):
        subarray_lowercorner.append(max(lowercorner1[i], lowercorner2[i]))
        subarray_uppercorner.append(min(uppercorner1[i], uppercorner2[i]))

    # print(f"Overlap subarray : {subarray_lowercorner[0]}:{subarray_uppercorner[0]}, {subarray_lowercorner[1]}:{subarray_uppercorner[1]}, {subarray_lowercorner[2]}:{subarray_uppercorner[2]}")
    return (subarray_lowercorner, subarray_uppercorner)


def get_named_volumes(blocks_partition, block_shape):
    """ Return the coordinates of all entities of shape block shape in the reconstructed image.
    The first entity is placed at the origin of the base.

    Returns: 
    ---------
        d: dictionary mapping each buffer numeric index to a Volume representing its coordinates

    Arguments: 
    ----------
        blocks_partition: Number of blocks in each dimension. Shape of the reconstructed image in terms of the blocks considered.
        block_shape: shape of one block, all blocks having the same shape 
    """
    # logger.debug("== Function == get_named_volumes")
    d = dict()
    # logger.debug("[Arg] blocks_partition: %s", blocks_partition)
    # logger.debug("[Arg] block_shape: %s", block_shape)
    for i in range(blocks_partition[0]):
        for j in range(blocks_partition[1]):
            for k in range(blocks_partition[2]):
                bl_corner = (block_shape[0] * i,
                             block_shape[1] * j,
                             block_shape[2] * k)
                tr_corner = (block_shape[0] * (i+1),
                             block_shape[1] * (j+1),
                             block_shape[2] * (k+1))   
                index = _3d_to_numeric_pos((i, j, k), blocks_partition, order='C')
                d[index] = Volume(index, bl_corner, tr_corner)
    # logger.debug("Indices of names volumes found: %s", d.keys())
    # logger.debug("End\n")
    return d


def apply_merge(volume, volumes, merge_directions):
    """ Merge volume with other volumes from volumes list in the merge directions.

    Arguments: 
    ----------
        volume: volume to merge
        volumes: list of volumes 
        merge_directions: indicates neighbours to merge with
    """
    
    def get_new_volume(volume, lowcorner):
        v2 = get_volume(lowcorner)
        if v2 != None:
            return merge_volumes(volume, v2)
        else:
            _id = volume.index.split('_')[0]
            volume.index = str(_id) + '_merged'
            return volume

    def get_volume(lowcorner):
        if not isinstance(lowcorner, tuple):
            raise TypeError()  # required for "=="

        for i in range(len(volumes)):
            v = volumes[i]
            if v.p1 == lowcorner:
                logger.debug("\tMerging volume with low corner %s", v.p1)
                return volumes.pop(i)
        
        logger.warning("\tNo volume to merge with")
        return None

    import copy

    logger.debug("\t== Function == apply_merge")

    p1, p2 = volume.get_corners()
    logger.debug("\tTargetting volume with low corner %s", p1)

    if len(merge_directions) == 1:
        if Axes.k in merge_directions:
            p1_target = list(copy.deepcopy(p1))
            p1_target[Axes.k.value] = p2[Axes.k.value]
            new_volume = get_new_volume(volume, tuple(p1_target))

        elif Axes.j in merge_directions:
            p1_target = list(copy.deepcopy(p1))
            p1_target[Axes.j.value] = p2[Axes.j.value]
            new_volume = get_new_volume(volume, tuple(p1_target))

        elif Axes.i in merge_directions:
            p1_target = list(copy.deepcopy(p1))
            p1_target[Axes.i.value] = p2[Axes.i.value]
            new_volume = get_new_volume(volume, tuple(p1_target))

    elif len(merge_directions) == 2:
        logger.debug("\tMerge directions: %s", merge_directions)
        axis1, axis2 = merge_directions

        p1_target = list(copy.deepcopy(p1))
        p1_target[axis1.value] = p2[axis1.value]
        volume_axis1 = get_new_volume(volume, tuple(p1_target))

        new_volume_axis1 = apply_merge(volume_axis1, volumes, [axis2])
        new_volume_axis2 = apply_merge(volume, volumes, [axis2])
        new_volume = merge_volumes(new_volume_axis1, new_volume_axis2)

    elif len(merge_directions) == 3:
        logger.debug("\tMerge directions %s", merge_directions)
        axis1, axis2, axis3 = merge_directions
        
        p1_target = list(copy.deepcopy(p1))
        p1_target[axis1.value] = p2[axis1.value]
        volume_axis1 = get_new_volume(volume, tuple(p1_target))

        new_vol1 = apply_merge(volume, volumes, [axis2, axis3])
        new_vol2 = apply_merge(volume_axis1, volumes, [axis2, axis3])
        new_volume = merge_volumes(new_vol1, new_vol2)

    else:
        raise ValueError()

    logger.debug("\tEnd")
    return new_volume


def numeric_to_3d_pos(numeric_pos, blocks_partition, order):
    """ Convert numeric block position into its 3d position in the array in a given storage order.
    See also: 
    --------
        get_partition
    """
    if order == 'C':
        nb_blocks_per_row = blocks_partition[2]
        nb_blocks_per_slice = blocks_partition[1] * blocks_partition[2]
    else:
        raise ValueError("unsupported")

    i = math.floor(numeric_pos / nb_blocks_per_slice)
    numeric_pos -= i * nb_blocks_per_slice
    j = math.floor(numeric_pos / nb_blocks_per_row)
    numeric_pos -= j * nb_blocks_per_row
    k = numeric_pos
    return (i, j, k)


def _3d_to_numeric_pos(_3d_pos, blocks_partition, order):
    """ Convert 3d block position into its numeric position in a given storage order.
    See also: 
    --------
        get_partition
    """
    if order == 'C':
        nb_blocks_per_row = blocks_partition[2]
        nb_blocks_per_slice = blocks_partition[1] * blocks_partition[2]
    else:
        raise ValueError("unsupported")

    return (_3d_pos[0] * nb_blocks_per_slice) + \
        (_3d_pos[1] * nb_blocks_per_row) + _3d_pos[2]


def get_partition(array_shape, chunk_shape):
    """ Returns partition of array by chunks. 
    Arguments:
    ----------
        array_shape: shape of input array
        chunk_shape: shape of one chunk
    Returns: 
    --------
        the partition as a tuple
    """
    chunks = chunk_shape 
    # logger.debug(f'Chunks for get_array_block_dims: {chunks}')
    if not len(array_shape) == len(chunks):
        raise ValueError(
            "chunks and shape should have the same dimension",
            array_shape,
            chunks)
    return tuple([int(s / c) for s, c in zip(array_shape, chunks)])


def get_file_manager(file_format):
    if file_format == "HDF5":
        return HDF5_manager()
    else:
        print("File format not supported yet. Aborting...")
        raise ValueError()