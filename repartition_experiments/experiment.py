import random, argparse, sys, os, json, time, csv
from time import strftime, gmtime
import numpy as np


def flush_cache():
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches') 


def get_arguments():
    """ Get arguments from console command.
    """
    parser = argparse.ArgumentParser(description="This experiment is referenced as experiment 3 in Guédon et al.")
    
    parser.add_argument('paths_config', 
        action='store', 
        type=str, 
        help='Path to configuration file containing paths of data directories.')

    parser.add_argument('cases_config',
        action='store',
        type=str,
        help='Path to configuration file containing experiment cases.')

    parser.add_argument('model',
        action='store',
        type=str,
        help='Name of model to run.')

    parser.add_argument('case_name',
        action='store',
        type=str,
        help='Case from cases_config to run.')

    parser.add_argument('-f', '--file_format',
        action='store',
        type=str,
        dest='file_format',
        default='HDF5',
        help='File format of arrays manipulated.')

    parser.add_argument('-o', '--overwrite', 
        action='store_true', 
        dest='overwrite',
        default=False,
        help='Set to true to overwrite original array if it already exists. Default is False.')

    return parser.parse_args()


def create_input_file(shape, dirname, file_manager):
    filename = f'{shape[0]}_{shape[1]}_{shape[2]}_original.hdf5'
    filepath = os.path.join(dirname, filename)

    if not os.path.isfile(filepath):
        data = np.random.normal(size=shape)
        file_manager.write(filepath, data, shape, _slices=None)

    return filepath


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)


def experiment(args, paths):
    """
    Notes: 
    - data type is np.float16
    """
    cases = load_json(args.cases_config)
    bpv = 2

    indir_path, outdir_path = os.path.join(paths["ssd_path"], 'indir'), os.path.join(paths["ssd_path"], 'outdir')
    create_empty_dir(indir_path)
    create_empty_dir(outdir_path)

    fm = get_file_manager(args.file_format)
    if args.overwrite:
        fm.remove_all(paths["ssd_path"])
    
    case = cases[args.case_name]
    random.shuffle(case)
    results = list()
    R_prev, I_prev = (0,0,0), (0,0,0)
    for run in case:
        R, O, I, B, volumestokeep = run["R"], run["O"], run["I"], run["B"], run["volumestokeep"]
        if args.case_name.split('_')[0] == "case 1":
            lambd = get_input_aggregate(O, I)
            B, volumestokeep = (lambd[0],lambd[1],lambd[2]), list(range(1,8))
        origarr_filepath = create_input_file(R, paths["ssd_path"], fm)

        # split 
        if R_prev != R or (R_prev == R and I_prev != I):
            create_empty_dir(indir_path)
            R_size = R[0]*R[1]*R[2]*bpv
            clustered_writes(origarr_filepath, R, I, bpv, R_size, args.file_format, indir_path)
            
        # resplit
        flush_cache()
        if args.model == "baseline":
            t = time.time()
            baseline_rechunk(indir_path, outdir_path, O, I, R, args.file_format)
            t = time.time() - t 
            tpp = 0
        elif args.model == "keep":
            t = time.time()
            tpp = keep_algorithm(R, O, I, B, volumestokeep, args.file_format, outdir_path, indir_path)
            t = time.time() - t - tpp
        else:
            raise ValueError("Bad model name")

        # verify and clean output
        success = verify_results(outdir_path, origarr_filepath, R, O, args.file_format)
        results.append([
            args.case_name,
            run["ref"],
            args.model, 
            t,
            tpp,
            success
        ])
        create_empty_dir(outdir_path)
        R_prev, I_prev = R, I 

    return results


def write_results(rows, args):
    case_name = args.case_name.replace(' ', '_')
    time_string = strftime("%b_%d_%Y_%H:%M:%S", gmtime(time.time()))
    filename = f"results_{case_name}_{args.model}_{time_string}.csv"
    paths = load_json(args.paths_config)
    csv_path = os.path.join(paths["outdir_path"], filename)

    columns = [
        'case_name',
        'run_ref',
        'model',
        'process_time',
        'preprocess_time',
        'success'
    ]

    with open(csv_path, mode='w+') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(columns)
        for row in rows: 
            writer.writerow(row)

    return csv_path


if __name__ == "__main__":
    args = get_arguments()
    paths = load_json(args.paths_config)

    for k, v in paths:
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)
    from repartition_experiments.exp_utils import create_empty_dir, verify_results
    from repartition_experiments.algorithms.baseline_algorithm import baseline_rechunk
    from repartition_experiments.algorithms.keep_algorithm import keep_algorithm, get_input_aggregate
    from repartition_experiments.algorithms.clustered_writes import clustered_writes
    from repartition_experiments.algorithms.utils import get_file_manager

    results = experiment(args)
    write_results(results, args)