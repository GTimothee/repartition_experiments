from ..algorithms.policy_remake import *
from ..algorithms.utils import get_volumes


def test_get_grads():
    cases = [
        [
            [
                (120,120,120),
                (40,40,40),
                (60,60,60),
                [1]
            ], [
                set([40,60,80,120]),
                set([40,60,80,120]),
                set([40,80,120])
            ]
        ],
        [
            [
                (120,120,120),
                (40,40,40),
                (60,60,60),
                [1,2,3]
            ], [
                set([40,60,80,120]),
                set([40,80,120]),
                set([40,80,120])
            ]
        ]
    ]
    for case in cases:
        R, O, B, volumestokeep = case[0]
        expected = case[1]
        grads, _ = get_grads(R, O, B, get_dims_to_keep(volumestokeep))

        assert grads[0] == expected[0]
        assert grads[1] == expected[1]
        assert grads[2] == expected[2]


def test_get_outfiles_parts():
    cases = [
        [
            [
                (120,120,120),
                (40,40,40),
                (60,60,60),
                [1]
            ], {
                0: 1,
                1: 1,
                2: 1,
                3: 2,
                4: 3,
                5: 2,
                6: 1,
                7: 2,
                8: 1,
            }
        ],
        [
            [
                (120,120,120),
                (40,40,40),
                (60,60,60),
                [1,2,3]
            ], {
                0: 1,
                1: 1,
                2: 1,
                3: 2,
                4: 3,
                5: 2,
                6: 1,
                7: 2,
                8: 1,
            }
        ]
    ]

    for case in cases:
        R, O, B, volumestokeep = case[0]
        outfiles_partition, outvolumes = get_volumes(R, O)
        expected = case[1]
        _3d_to_numeric_pos_dict = get_pos_association_dict(volumestokeep, outfiles_partition)

        grads, grads_o = get_grads(R, O, B, get_dims_to_keep(volumestokeep))
        d = get_outfiles_parts(grads, grads_o, _3d_to_numeric_pos_dict)
        for k,v in d.items():
            assert len(v) == expected[k]