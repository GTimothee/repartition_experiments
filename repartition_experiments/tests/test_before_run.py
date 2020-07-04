from ..algorithms.policy import compute_zones
from ..algorithms.keep_algorithm import get_input_aggregate
from ..algorithms.utils import get_volumes
from ..exp_utils import load_json

import os


def test_before_run():
    """ See if all runs are working well with compute_zones
    """
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../cases_config.json')
    cases = load_json(filepath)

    for case_name, case in cases.items():
        print(f"processing case: {case_name}")

        for run in case:
            R, O, I, B, volumestokeep, ref = tuple(run["R"]), tuple(run["O"]), tuple(run["I"]), tuple(run["B"]), run["volumestokeep"], run["ref"]
            print(f"processing run: {ref}")
            if case_name.split('_')[0] == "case 1":
                lambd = get_input_aggregate(O, I)
                B, volumestokeep = (lambd[0],lambd[1],lambd[2]), list(range(1,8))
                run["volumestokeep"] = volumestokeep

            R, O, I, B = tuple(R), tuple(O), tuple(I), tuple(B)

            buffers_partition, buffers = get_volumes(R, B)
            infiles_partition, involumes = get_volumes(R, I)
            outfiles_partition, outvolumes = get_volumes(R, O)

            try:
                arrays_dict, buffer_to_outfiles, _, _ = compute_zones(B, O, R, volumestokeep, buffers_partition, outfiles_partition, buffers, outvolumes)
            except Exception as e:
                print(f'error {e} \nError with ref {ref}')
                raise Exception()
    assert True


def test_before_run_mid():
    """ See if all runs are working well with compute_zones
    """
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../mid_cases_config.json')
    cases = load_json(filepath)

    for case_name, case in cases.items():
        print(f"processing case: {case_name}")

        for run in case:
            R, O, I, B, volumestokeep, ref = tuple(run["R"]), tuple(run["O"]), tuple(run["I"]), tuple(run["B"]), run["volumestokeep"], run["ref"]
            print(f"processing run: {ref}")
            if case_name.split('_')[0] == "case 1":
                lambd = get_input_aggregate(O, I)
                B, volumestokeep = (lambd[0],lambd[1],lambd[2]), list(range(1,8))
                run["volumestokeep"] = volumestokeep

            R, O, I, B = tuple(R), tuple(O), tuple(I), tuple(B)

            buffers_partition, buffers = get_volumes(R, B)
            infiles_partition, involumes = get_volumes(R, I)
            outfiles_partition, outvolumes = get_volumes(R, O)

            try:
                arrays_dict, buffer_to_outfiles, _, _ = compute_zones(B, O, R, volumestokeep, buffers_partition, outfiles_partition, buffers, outvolumes)
            except Exception as e:
                print(f'error {e} \nError with ref {ref}')
                raise Exception()
    assert True