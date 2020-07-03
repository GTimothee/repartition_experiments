import os, csv, sys, time
import numpy as np

from ..exp_utils import create_empty_dir, verify_results
from ..algorithms.baseline_algorithm import baseline_rechunk
from ..algorithms.keep_algorithm import keep_algorithm, get_input_aggregate
from ..algorithms.clustered_writes import clustered_writes
from ..algorithms.utils import get_file_manager
from ..experiment import experiment, write_results, load_json
from ..algorithms.baseline_algorithm import baseline_rechunk

def flush_cache():
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches') 


class Args:
    def __init__(self, case_name, model):
        test_dir = os.path.dirname(os.path.abspath(__file__))
        self.paths_config = os.path.join(test_dir, 'paths_config.json')
        self.cases_config = os.path.join(test_dir, 'cases_config.json')
        self.file_format = 'HDF5'
        self.overwrite = True
        self.addition = False

        self.model = model
        self.case_name = case_name


def test_experiment_keep():
    results = experiment(Args("case 1_0", "keep"))
    for r in results:
        success = r[-1]
        assert success

    csv_path = write_results(results, Args("case 1_0", "keep"))
    nb_lines = 0
    with open(csv_path, "r") as f:
        reader = csv.reader(f, delimiter=",")
        for i, line in enumerate(reader):
            nb_lines += 1
    assert nb_lines == (len(results) + 1)

    results = experiment(Args("case 2", "keep"))
    for r in results:
        success = r[-1]
        assert success

    csv_path = write_results(results, Args("case 2", "keep"))
    nb_lines = 0
    with open(csv_path, "r") as f:
        reader = csv.reader(f, delimiter=",")
        for i, line in enumerate(reader):
            nb_lines += 1
    assert nb_lines == (len(results) + 1)

    arggs = Args("case 2", "keep")
    arggs.addition = True
    assert arggs.addition == True
    results = experiment(arggs)
    for r in results:
        success = r[-1]
        assert success


def test_experiment_baseline():
    results = experiment(Args("case 1_0", "baseline"))
    for r in results:
        success = r[-1]
        assert success

    results = experiment(Args("case 2", "baseline"))
    for r in results:
        success = r[-1]
        assert success


def test_compare1():
    flush_cache()
    results_keep = experiment(Args("case 1_1", "keep"))

    flush_cache()
    results_baseline = experiment(Args("case 1_1", "baseline"))

    # for i in range(len(results_keep)):
    #     rk = results_keep[i]
        
    #     for row in results_baseline:
    #         if (row[0], row[1]) == (rk[0], rk[1]):
    #             rb = row

    #             rtk, wtk = rk[5], rk[6]
    #             rtb, wtb = rb[5], rb[6]

    #             assert rk[-1] == True
    #             assert rb[-1] == True
    #             assert rtk < rtb
    #             assert wtk < wtb
    #             break


def test_compare2():
    flush_cache()
    results_keep = experiment(Args("case 1_2", "keep"))

    flush_cache()
    results_baseline = experiment(Args("case 1_2", "baseline"))

    # for i in range(len(results_keep)):
    #     rk = results_keep[i]
        
    #     for row in results_baseline:
    #         if (row[0], row[1]) == (rk[0], rk[1]):
    #             rb = row

    #             rtk, wtk = rk[5], rk[6]
    #             rtb, wtb = rb[5], rb[6]

    #             print(rtk, 'vs', rtb)
    #             print(wtk, 'vs', wtb)
    #             assert rk[-1] == True
    #             assert rb[-1] == True
    #             assert rtk < rtb
    #             assert wtk < wtb
    #             break


def test_compare():
    # case = {
    #     "R": [1,12,12],
    #     "O": [1,4,4],
    #     "I": [1,6,6],
    #     "B": [],
    #     "volumestokeep": [],
    # }
    # case = {
    #     "R": [390,300,350],
    #     "I": [78,60,70],
    #     "O": [65,50,50],
    #     "B": [],
    #     "volumestokeep": [],
    # }
    case = {
        "R": [350,350,350],
        "I": [35,35,35],
        "O": [25,25,25],
        "B": [],
        "volumestokeep": [],
    }
    R, O, I, B, volumestokeep = case["R"], case["O"], case["I"], case["B"], case["volumestokeep"]

    lambd = get_input_aggregate(O, I)
    B, volumestokeep = (lambd[0],lambd[1],lambd[2]), list(range(1,8))

    indir_path, outdir_path, file_format = './input_dir', './output_dir', 'HDF5'
    create_empty_dir(indir_path)
    create_empty_dir(outdir_path)

    fm = get_file_manager(file_format)
    fm.remove_all('./')

    # create input array
    origarr_filepath = './original_array.hdf5'
    if os.path.isfile(origarr_filepath):
        os.remove(origarr_filepath)
    data = np.random.normal(size=R)
    t = time.time()
    fm.write(origarr_filepath, data, R, _slices=None)
    t = time.time() -t 
    print(f"write time for the whole array: {t}")

    # split before resplit
    bpv = 2 # bytes per voxel
    R_size = R[0]*R[1]*R[2]*bpv
    clustered_writes(origarr_filepath, R, I, bpv, R_size, file_format, indir_path)

    flush_cache()
    t = time.time()
    ppt, rt1, wt1, data = keep_algorithm(R, O, I, B, volumestokeep, file_format, outdir_path, indir_path, False)
    t = time.time()- t
    assert verify_results(outdir_path, origarr_filepath, R, O, file_format, False)
    print("total processing time: ", t)
    print("read time", rt1)
    print("write time", wt1)

    flush_cache()
    create_empty_dir(outdir_path)
    t = time.time()
    rt2, wt2, data = baseline_rechunk(indir_path, outdir_path, O, I, R, file_format, False, clean_out_dir=False) 
    t = time.time() - t
    assert verify_results(outdir_path, origarr_filepath, R, O, file_format, False)
    print("total processing time: ", t)
    print("read time", rt2)
    print("write time", wt2)

    # assert rt1 < rt2
    assert wt1 < wt2


def test_sanity_realcases():
    # test if buffer is partition of R
    cases = {
        "case 1_1": [
            {
                "R": [3500,3500,3500],
                "I": [875,875,875],
                "O": [875,1750,875],
                "B": [],
                "volumestokeep": [],
                "ref": 0
            },
            {
                "R": [3500,3500,3500],
                "I": [875,875,875],
                "O": [700,875,700],
                "B": [],
                "volumestokeep": [],
                "ref": 1
            },{
                "R": [3500,3500,3500],
                "I": [350,350,350],
                "O": [500,500,500],
                "B": [],
                "volumestokeep": [],
                "ref": 2
            },{
                "R": [3500,3500,3500],
                "I": [350,350,350],
                "O": [250,250,250],
                "B": [],
                "volumestokeep": [],
                "ref": 3
            },{
                "R": [3500,3500,3500],
                "I": [175,175,175],
                "O": [250,250,250],
                "B": [],
                "volumestokeep": [],
                "ref": 4
            },
            {
                "R": [3500,3500,3500],
                "I": [350,875,350],
                "O": [500,875,500],
                "B": [],
                "volumestokeep": [],
                "ref": 5
            },
            {
                "R": [3500,3500,3500],
                "I": [350,875,350],
                "O": [350,500,350],
                "B": [],
                "volumestokeep": [],
                "ref": 6
            }
        ]
    }
    for case_name, case in cases.items():
        print(f"processing case {case_name}")
        for run in case:
            
            R, O, I, B, volumestokeep = tuple(run["R"]), tuple(run["O"]), tuple(run["I"]), tuple(run["B"]), run["volumestokeep"]
            print("R, O, I, B, volumestokeep: \n", R, O, I, B, volumestokeep)
            if case_name.split('_')[0] == "case 1":
                lambd = get_input_aggregate(O, I)
                B, volumestokeep = (lambd[0],lambd[1],lambd[2]), list(range(1,8))
                run["volumestokeep"] = volumestokeep
            
            run["R"] = R 
            run["I"] = I
            run["O"] = O
            run["B"] = B

            print(f"R: {R}")
            print(f"I: {I}")
            print(f"O: {O}")
            print(f"B: {B}")
            for shape_to_test in [O, I, B]:
                print(f"testing shape {shape_to_test}")
                for dim in range(3):
                    assert R[dim] % shape_to_test[dim] == 0