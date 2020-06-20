from ..algorithms.utils import _3d_to_numeric_pos, numeric_to_3d_pos

def test_3d_to_numeric_pos():
    blocks_partition = (2,5,3)
    test_C_order = {
        (0,0,0): 0,
        (0,0,1): 1,
        (1,4,2): 29,
        (0,2,1): 7,
        (1,3,0): 24
    }
    for _3d_pos, expected in test_C_order.items():
        assert expected == _3d_to_numeric_pos(_3d_pos, blocks_partition, order='C')
    

def test_numeric_to_3d_pos():
    blocks_partition = (2,5,3)
    test_C_order = {
        (0,0,0): 0,
        (0,0,1): 1,
        (1,4,2): 29,
        (0,2,1): 7,
        (1,3,0): 24
    }
    for expected, numeric_pos in test_C_order.items():
        assert expected == numeric_to_3d_pos(numeric_pos, blocks_partition, order='C')


