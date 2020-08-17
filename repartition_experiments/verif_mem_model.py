import math, argparse, json, sys, time

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

    return parser.parse_args()


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)


if __name__ == "__main__":
    args = get_arguments()
    paths = load_json(args.paths_config)
    cases = load_json(args.cases_config)
    case = cases[args.case_name][0]
    
    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    import logging
    import logging.config
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
    })

    # absolute imports
    compute_max_mem(R, B, O, nb_bytes_per_voxel)