import random, json, argparse, os, sys



def get_arguments():
    """ Get arguments from console command.
    """
    parser = argparse.ArgumentParser(description="")
    
    parser.add_argument('paths_config', 
        action='store', 
        type=str, 
        help='Path to configuration file containing paths of data directories.')


    return parser.parse_args()


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)



if __name__ == "__main__":
    args = get_arguments()
    paths = load_json(args.paths_config)
    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    from repartition_experiments.scripts_paper.baseline_seeks_model_remake import keep_model_seeks, get_volumes_to_keep, compute_keep_seeks_model
    from repartition_experiments.scripts_paper.keep_algorithm_simulator import keep_algorithm

    # A = (120,120,120)
    # B = (40,1,1)
    # O = (30,30,30)
    # I = (40,40,40)

    A, I, O, B = (135,135,135), (45, 45, 45), (27, 27, 27), (1, 15, 45)
    # model_total = keep_model_seeks(A, B, O, I)
    # volumestokeep = get_volumes_to_keep(A, B, O)
    # nb_outfile_openings, nb_outfile_inside_seeks, nb_infile_openings, nb_infile_inside_seeks = keep_algorithm(A, O, I, B, volumestokeep)

    # print(f"[REAL] all: {nb_outfile_openings}, {nb_outfile_inside_seeks}, {nb_infile_openings}, {nb_infile_inside_seeks}")
    # print(f"[REAL] Number outblock inside seeks : {nb_outfile_inside_seeks}")

    w0 = list(range(1,136,1))
    w1 = [15,27,30,45,54,60,75,81,90,105,108,120,135]
    w2 = [27,54,81,108,135]
    W = (w0, w1, w2)
    model_total = compute_keep_seeks_model(A, B, I, O, W, 0)
