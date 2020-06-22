from ..algorithms.clustered_writes import *


def test_get_entity_sizes():
    # in C order
    bytes_per_voxel = 1
    R = (10,9,10)
    cs = (5,3,2)
    partition = (2,3,5)
    bs, brs, bss = get_entity_sizes(cs, bytes_per_voxel, partition)

    assert bs == 5*3*2
    assert brs == 5*3*2*5
    assert bss == 5*3*2*5*3


def test_get_strategy():
    # in C order
    bytes_per_voxel = 1
    R = (20,9,10)
    cs = (5,3,2)
    partition = (4,3,5)
    bs, brs, bss = get_entity_sizes(cs, bytes_per_voxel, partition)
    
    test_case = {
        5*2*3: 0, # 1 block 
        5*2*3*4: 0, # 4 blocks 
        5*2*3*5: 1, # 1 row 
        5*2*3*5*2: 1, # 2 rows
        5*2*3*5*3: 2, # 1 slice 
        5*2*3*5*3*3: 2, # 3 slices 
        5*2*3*5*3*4: 2, # whole img
    }

    for buffer_mem_size, expected in test_case.items():
        strategy = get_strategy(buffer_mem_size, bs, brs, bss)
        assert strategy == expected


def test_compute_buffers():
    # in C order
    bytes_per_voxel = 1
    R = (20,9,10)
    cs = (5,3,2)
    partition = (4,3,5)
    bs, brs, bss = get_entity_sizes(cs, bytes_per_voxel, partition)
    origarr_size = R[0]*R[1]*R[2]*bytes_per_voxel
    
    test_case = {
        5*2*3: 4*3*5, # 1 block 
        5*2*3*4: 4*3*2, # 4 blocks 
        5*2*3*5: 4*3, # 1 row 
        5*2*3*5*2: 4*2, # 2 rows
        5*2*3*5*3: 4, # 1 slice 
        5*2*3*5*3*3: 2, # 3 slices 
        5*2*3*5*3*4: 1, # whole img
    }

    for buffer_mem_size, expected in test_case.items():
        strategy = get_strategy(buffer_mem_size, bs, brs, bss)
        buffers = compute_buffers(buffer_mem_size, strategy, origarr_size, cs, bs, brs, bss, partition, R, bytes_per_voxel)

        # test number of buffers
        nb_buffers = len(buffers.values())
        assert nb_buffers == expected