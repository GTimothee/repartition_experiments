from ..algorithms.tracker import Tracker
from ..algorithms.keep_algorithm import get_input_aggregate
from ..algorithms.policy_remake import compute_zones_remake
from ..algorithms.utils import get_volumes


# def test_policy():
#     def is_partition(volumes_list, R):
#         tr = Tracker()
#         for v in volumes_list:
#             tr.add_volume(v)
#         assert tr.is_complete(((0,0,0), R))

#     case = {
#         "R": [390,300,350],
#         "I": [78,60,70],
#         "O": [65,50,50],
#         "B": [],
#         "volumestokeep": [],
#     }

#     R, O, I, B, volumestokeep = case["R"], case["O"], case["I"], case["B"], case["volumestokeep"]
#     lambd = get_input_aggregate(O, I)
#     B, volumestokeep = (lambd[0],lambd[1],lambd[2]), list(range(1,8))

#     # test
#     buffers_partition, buffers = get_volumes(R, B)
#     infiles_partition, involumes = get_volumes(R, I)
#     outfiles_partition, outvolumes = get_volumes(R, O)

#     is_partition(buffers.values(), R)
#     is_partition(involumes.values(), R)
#     is_partition(outvolumes.values(), R)

#     arrays_dict, buffer_to_outfiles, _, _ = compute_zones(B, O, R, volumestokeep, buffers_partition, outfiles_partition, buffers, outvolumes)


def test_compute_zone_remake():
    B = (1, 15, 45)
    O = (1, 27, 27)
    R = (1,135,135)
    volumestokeep = [1,2,3,4,5,6,7]
    outfiles_partititon, out_volumes = get_volumes(R, O)
    _, buffers = get_volumes(R, B)
    
    d, _, _, _ = compute_zones_remake(B, O, R, volumestokeep, outfiles_partititon, out_volumes, buffers, get_buffer_to_outfiles=False)

    for outfile, write_buffers in d.items():
        print(f"OUTFILE {outfile}")
        for wb in write_buffers:
            wb.print()

    assert False