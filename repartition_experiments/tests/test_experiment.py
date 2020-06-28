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


# class Args:
#     def __init__(self, case_name, model):
#         test_dir = os.path.dirname(os.path.abspath(__file__))
#         self.paths_config = os.path.join(test_dir, 'paths_config.json')
#         self.cases_config = os.path.join(test_dir, 'cases_config.json')
#         self.file_format = 'HDF5'
#         self.overwrite = True

#         self.model = model
#         self.case_name = case_name


# def test_experiment_keep():
#     results = experiment(Args("case 1_0", "keep"))
#     for r in results:
#         success = r[-1]
#         assert success

#     csv_path = write_results(results, Args("case 1_0", "keep"))
#     nb_lines = 0
#     with open(csv_path, "r") as f:
#         reader = csv.reader(f, delimiter=",")
#         for i, line in enumerate(reader):
#             nb_lines += 1
#     assert nb_lines == (len(results) + 1)

#     results = experiment(Args("case 2", "keep"))
#     for r in results:
#         success = r[-1]
#         assert success

#     csv_path = write_results(results, Args("case 2", "keep"))
#     nb_lines = 0
#     with open(csv_path, "r") as f:
#         reader = csv.reader(f, delimiter=",")
#         for i, line in enumerate(reader):
#             nb_lines += 1
#     assert nb_lines == (len(results) + 1)


# def test_experiment_baseline():
#     results = experiment(Args("case 1_0", "baseline"))
#     for r in results:
#         success = r[-1]
#         assert success

#     results = experiment(Args("case 2", "baseline"))
#     for r in results:
#         success = r[-1]
#         assert success


# def test_compare1():
#     flush_cache()
#     results_keep = experiment(Args("case 1_1", "keep"))

#     flush_cache()
#     results_baseline = experiment(Args("case 1_1", "baseline"))

#     for i in range(len(results_keep)):
#         rk = results_keep[i]
        
#         for row in results_baseline:
#             if (row[0], row[1]) == (rk[0], rk[1]):
#                 rb = row

#                 rtk, wtk = rk[5], rk[6]
#                 rtb, wtb = rb[5], rb[6]

#                 assert rk[7] == True
#                 assert rb[7] == True
#                 assert rtk < rtb
#                 assert wtk < wtb
#                 break


# def test_compare2():
#     flush_cache()
#     results_keep = experiment(Args("case 1_2", "keep"))

#     flush_cache()
#     results_baseline = experiment(Args("case 1_2", "baseline"))

#     for i in range(len(results_keep)):
#         rk = results_keep[i]
        
#         for row in results_baseline:
#             if (row[0], row[1]) == (rk[0], rk[1]):
#                 rb = row

#                 rtk, wtk = rk[5], rk[6]
#                 rtb, wtb = rb[5], rb[6]

#                 assert rk[7] == True
#                 assert rb[7] == True
#                 assert rtk < rtb
#                 assert rtb < wtb
#                 break


# def test_compare():
#     # case = {
#     #     "R": [1,12,12],
#     #     "O": [1,4,4],
#     #     "I": [1,6,6],
#     #     "B": [],
#     #     "volumestokeep": [],
#     # }
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

#     indir_path, outdir_path, file_format = './input_dir', './output_dir', 'HDF5'
#     create_empty_dir(indir_path)
#     create_empty_dir(outdir_path)

#     # create input array
#     origarr_filepath = './original_array.hdf5'
#     if os.path.isfile(origarr_filepath):
#         os.remove(origarr_filepath)
#     data = np.random.normal(size=R)
#     fm = get_file_manager(file_format)
#     t = time.time()
#     fm.write(origarr_filepath, data, R, _slices=None)
#     t = time.time() -t 
#     print(f"write time for the whole array: {t}")

#     # split before resplit
#     bpv = 2 # bytes per voxel
#     R_size = R[0]*R[1]*R[2]*bpv
#     clustered_writes(origarr_filepath, R, I, bpv, R_size, file_format, indir_path)

#     flush_cache()
#     ppt, rt1, wt1 = keep_algorithm(R, O, I, B, volumestokeep, file_format, outdir_path, indir_path)
#     assert verify_results(outdir_path, origarr_filepath, R, O, file_format)
#     print("read time", rt1)
#     print("write time", wt1)

#     flush_cache()
#     create_empty_dir(outdir_path)
#     rt2, wt2 = baseline_rechunk(indir_path, outdir_path, O, I, R, file_format, True, clean_out_dir=False) 
#     assert verify_results(outdir_path, origarr_filepath, R, O, file_format)
#     print("read time", rt2)
#     print("write time", wt2)

#     assert rt1 < rt2
#     assert wt1 < wt2


def test_sanity_realcases():
    cases = {
        "case 1_1": [
            {
                "R": [3900,3000,3500],
                "I": [780,600,700],
                "O": [650,500,500],
                "B": [],
                "volumestokeep": [],
                "ref": 0
            },{
                "R": [3900,3000,3500],
                "I": [390,300,350],
                "O": [650,500,700],
                "B": [],
                "volumestokeep": [],
                "ref": 2
            }
        ],
        "case 1_2": [
            {
                "R": [3900,3000,3500],
                "I": [390,300,350],
                "O": [325,250,250],
                "B": [],
                "volumestokeep": [],
                "ref": 0
            },
            {
                "R": [3900,3000,3500],
                "I": [195,150,175],
                "O": [300,250,250],
                "B": [],
                "volumestokeep": [],
                "ref": 1
            },{
                "R": [3900,3000,3500],
                "I": [650,375,500],
                "O": [390,300,350],
                "B": [],
                "volumestokeep": [],
                "ref": 2
            }
        ],
        "case 2": [
            {
                "R": [3900,3000,3500],
                "I": [780,600,700],
                "O": [650,500,500],
                "B": [390,600,700],
                "volumestokeep": [1,2,3],
                "ref": 0
            }, {
                "R": [3900,3000,3500],
                "I": [390,300,350],
                "O": [650,500,700],
                "B": [390,600,700],
                "volumestokeep": [1,2,3],
                "ref": 1
            }, {
                "R": [3900,3000,3500],
                "I": [390,300,350],
                "O": [325,250,250],
                "B": [195,300,350],
                "volumestokeep": [1,2,3],
                "ref": 2
            }
        ],
        "case 3": [
            {
                "R": [3900,3000,3500],
                "I": [780,600,700],
                "O": [780,3000,700],
                "B": [390,3000,700],
                "volumestokeep": [1,2,3],
                "ref": 0
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
            run["O"] = O
            run["I"] = I
            run["B"] = B

            print(f"R: {R}")
            print(f"O: {O}")
            print(f"I: {I}")
            print(f"B: {B}")
            for shape_to_test in [O, I, B]:
                print(f"testing shape {shape_to_test}")
                for dim in range(3):
                    assert R[dim] % shape_to_test[dim] == 0