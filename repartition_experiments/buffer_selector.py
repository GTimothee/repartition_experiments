import logging, json, sys, argparse


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
        help='')

    parser.add_argument('case_name', 
        action='store', 
        type=str, 
        help='')

    parser.add_argument('nb_gig', 
        action='store', 
        type=int,
        help='')

    parser.add_argument('nb_bytes_per_voxel', 
        action='store', 
        type=int, 
        default=2,
        help='')

    return parser.parse_args()


def find_best_buffer(m, case, nb_bytes_per_voxel):
    """
    parmis les buffers qui sont ok niveau mémoire, 
    calculer le nombre de seeks de chaque buffer 
    prendre le buffer qui créé le minimum de seeks

    Arguments: 
        m: main memory available for the buffer (in voxels)
    """

    R, O, I, buffer_candidates = get_buffer_candidates(case)

    best_buff = None
    min_seeks = -1
    max_try = 10
    nb_try = 0
    for B in buffer_candidates:
        print(f"New buffer candidate {B}")
        max_mem = compute_max_mem(R, B, O, nb_bytes_per_voxel)

        if max_mem <= m:
            print("Computing nb seeks...")
            nb_seeks, seek_time = compute_nb_seeks(B, O, R, I)

            print(f"Seek computation time: {seek_time} seconds.")
            if min_seeks != -1 and nb_seeks < min_seeks or min_seeks == -1:
                print(f"New optimal buffer shape found: {B} => {nb_seeks} seeks. Processing time: {seek_time} seconds.")
                min_seeks = nb_seeks
                best_buff = B
            else:
                print(f"Not better")
                nb_try += 1
                if nb_try == max_try:
                    break
        
    print(f"Best buffer shape found: {best_buff} for {min_seeks} seeks.")
    return best_buff, min_seeks
    


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)

if __name__ == "__main__":
    args = get_arguments()
    paths = load_json(args.paths_config)
    cases = load_json(args.cases_config)
    
    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)
            
    from repartition_experiments.seek_calculator import compute_nb_seeks, get_buffer_candidates
    from repartition_experiments.memory_estimation import compute_max_mem
    from repartition_experiments.algorithms.keep_algorithm import get_input_aggregate

    import logging.config
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
    })

    case = cases[args.case_name][0]
    print(f"Processing {args.case_name}")
    find_best_buffer(args.nb_gig * 1000000000 / args.nb_bytes_per_voxel, case, args.nb_bytes_per_voxel)