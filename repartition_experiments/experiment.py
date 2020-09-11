import random, argparse, sys, os, json, time, csv
from time import strftime, gmtime
import numpy as np
import dask.array as da


def flush_cache():
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches') 


def get_arguments():
    """ Get arguments from console command.
    """
    parser = argparse.ArgumentParser(description="")
    
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

    parser.add_argument('-a', '--addition', 
        action='store_true', 
        dest='addition',
        default=False,
        help='Set to true to do an addition operation before writing data. Default is False.')

    parser.add_argument('-v', '--verify', 
        action='store_true', 
        dest='verify',
        default=False,
        help='Verify results. Default is False for benchmarks because it creates overhead in RAM consumption.')

    parser.add_argument('-m', '--clustered_mem',
        action='store',
        type=float,
        dest='clustered_mem',
        default=15.0,
        help='in gigabytes'
    )

    parser.add_argument('-d', '--distributed', 
        action='store_true', 
        dest='distributed',
        default=False,
        help='if input chunks distributed on several disks.')

    return parser.parse_args()


def create_input_file(shape, dirname, file_manager):
    filename = f'{shape[0]}_{shape[1]}_{shape[2]}_original.hdf5'
    filepath = os.path.join(dirname, filename)

    # if not os.path.isfile(filepath):
    #     data = np.random.default_rng().random(size=shape, dtype='f')
    #     file_manager.write(filepath, data, shape, _slices=None)

    if not os.path.isfile(filepath):
        arr = da.random.random(size=shape)
        arr = arr.astype(np.float16)
        da.to_hdf5(filepath, '/data', arr, chunks=None, compression=None)

    return filepath


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)


def experiment(args):
    """
    Notes: 
    - data type is np.float16
    """
    paths = load_json(args.paths_config)

    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    from monitor.monitor import Monitor
    from repartition_experiments.exp_utils import create_empty_dir, verify_results
    from repartition_experiments.algorithms.baseline_algorithm import baseline_rechunk
    from repartition_experiments.algorithms.keep_algorithm import keep_algorithm, get_input_aggregate
    from repartition_experiments.algorithms.utils import get_file_manager
    from repartition_experiments.algorithms.clustered_reads import clustered_reads

    paths = load_json(args.paths_config)
    cases = load_json(args.cases_config)
    bpv = 2

    indir_path, outdir_path = os.path.join(paths["ssd_path"], 'indir'), os.path.join(paths["ssd_path"], 'outdir')
    create_empty_dir(outdir_path)

    if args.distributed:
        print(f"Distributed mode -> creating the output directories")
        for i in range(6):
            dirpath = '/disk' + str(i) + '/gtimothee'
            create_empty_dir(dirpath)
            create_empty_dir(os.path.join(dirpath, 'output'))

    fm = get_file_manager(args.file_format)
    if args.overwrite:
        fm.remove_all(paths["ssd_path"])
    
    # transform cases into tuples + perform sanity check
    case = cases[args.case_name]
    for run in case:
        R, O, I, B, volumestokeep = tuple(run["R"]), tuple(run["O"]), tuple(run["I"]), tuple(run["B"]), run["volumestokeep"]
        if args.case_name.split('_')[0] == "case 1":
            lambd = get_input_aggregate(O, I)
            B, volumestokeep = (lambd[0],lambd[1],lambd[2]), list(range(1,8))
            run["volumestokeep"] = volumestokeep
        
        run["R"] = R 
        run["O"] = O
        run["I"] = I
        run["B"] = B

        for shape_to_test in [O, I, B]:
            for dim in range(3):
                try:
                    assert R[dim] % shape_to_test[dim] == 0
                except Exception as e:
                    print(R, shape_to_test)
                    print(e)

    random.shuffle(case)
    results = list()
    R_prev, I_prev = (0,0,0), (0,0,0)
    for run in case:
        R, O, I, B, volumestokeep = run["R"], run["O"], run["I"], run["B"], run["volumestokeep"]
        ref = run["ref"]
        print(f'Case being processed: (ref: {ref}) {R}, {I}, {O}, {B}, {volumestokeep}')
        filename = f'{R[0]}_{R[1]}_{R[2]}_original.hdf5'
        origarr_filepath = os.path.join(paths["ssd_path"], filename)

        # resplit
        print("processing...")
        
        flush_cache()
        print(f"cache flushed")
        
        if args.model == "baseline":
            _monitor = Monitor(enable_print=False, enable_log=False, save_data=True)
            _monitor.disable_clearconsole()
            _monitor.set_delay(15)
            _monitor.start()
            t = time.time()
            tread, twrite, seeks_data = baseline_rechunk(indir_path, outdir_path, O, I, R, args.file_format, args.addition, args.distributed)
            t = time.time() - t 
            _monitor.stop()
            piles = _monitor.get_mem_piles()
            max_voxels = 0
            print(f"Processing time: {t}")
            print(f"Read time: {tread}")
            print(f"Write time: {twrite}")
            tpp = 0
            voxel_tracker = None
        elif args.model == "keep":
            print(f"Running keep...")
            t = time.time()                    
            tpp, tread, twrite, seeks_data, voxel_tracker, piles = keep_algorithm(R, O, I, B, volumestokeep, args.file_format, outdir_path, indir_path, args.addition, args.distributed)
            t = time.time() - t - tpp
            max_voxels = voxel_tracker.get_max()
            print(f"Processing time: {t}")
            print(f"Read time: {tread}")
            print(f"Write time: {twrite}")
        elif args.model == "clustered":
            tpp = 0
            m = args.clustered_mem * 1000000000 # one GIG
            
            _monitor = Monitor(enable_print=False, enable_log=False, save_data=True)
            _monitor.disable_clearconsole()
            _monitor.set_delay(15)
            _monitor.start()
            t = time.time()   
            tread, twrite, seeks_data = clustered_reads(outdir_path, R, I, bpv, m, args.file_format, indir_path)
            t = time.time() - t - tpp
            _monitor.stop()
            piles = _monitor.get_mem_piles()

            voxel_tracker = None
            max_voxels = 0

            print(f"Processing time: {t}")
            print(f"Read time: {tread}")
            print(f"Write time: {twrite}")
        else:
            raise ValueError("Bad model name")

        # verify and clean output
        print("verifying results....")
        if args.verify:
            split_merge = False
            if args.case_name == "case 3":
                split_merge = True 
            success = verify_results(outdir_path, origarr_filepath, R, O, args.file_format, args.addition, split_merge)
        else:
            success = True
        print("successful run: ", success)

        results.append([
            args.case_name,
            run["ref"],
            args.model, 
            t,
            tpp,
            tread,
            twrite,
            seeks_data[0],
            seeks_data[1],
            seeks_data[2],
            seeks_data[3],
            max_voxels,
            success
        ])
        create_empty_dir(outdir_path)
        R_prev, I_prev = R, I 

        write_memory_pile(piles[0], piles[1], run["ref"], args)
        if voxel_tracker != None:
            write_voxel_history(voxel_tracker, run["ref"], args)

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
        'read_time',
        'write_time',
        'outfile_openings',
        'outfile_seeks',
        'infile_openings',
        'infile_seeks',
        'max_voxels',
        'success'
    ]

    with open(csv_path, mode='w+') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(columns)
        for row in rows: 
            writer.writerow(row)

    return csv_path


def write_memory_pile(ram_pile, swap_pile, ref, args):
    case_name = args.case_name.replace(' ', '_')
    time_string = strftime("%b_%d_%Y_%H:%M:%S", gmtime(time.time()))
    filename = f"memorytrace_{case_name}_{ref}_{args.model}_{time_string}.csv"
    paths = load_json(args.paths_config)
    filepath = os.path.join(paths["outdir_path"], filename)

    columns = ['ram', 'swap']
    with open(filepath, "w+") as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(columns)
        for ram, swap in zip(ram_pile, swap_pile):
            writer.writerow([ram, swap])

    return filepath


def write_voxel_history(voxel_tracker, ref, args):
    case_name = args.case_name.replace(' ', '_')
    time_string = strftime("%b_%d_%Y_%H:%M:%S", gmtime(time.time()))
    filename = f"voxelstrace_{case_name}_{ref}_{args.model}_{time_string}.csv"
    paths = load_json(args.paths_config)
    filepath = os.path.join(paths["outdir_path"], filename)

    columns = ['nb_voxels']
    with open(filepath, mode='w+') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(columns)
        for data in voxel_tracker.get_history(): 
            writer.writerow([data])

    return filepath


if __name__ == "__main__":
    args = get_arguments()
    results = experiment(args)
    write_results(results, args)
    