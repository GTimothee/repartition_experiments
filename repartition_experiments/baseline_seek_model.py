import argparse, json, sys

def get_cuts(big_block, small_block):
    def get_cuts_by_dim(big_block, small_block, i):
        nb_max = int(big_block[i] / small_block[i])
        cuts = list(tuple([small_block[i]*j for j in range(nb_max+1)]))
        print(cuts)
        cuts.remove(0)
        return cuts

    return [get_cuts_by_dim(big_block, small_block, i) for i in range(3)]


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

    return parser.parse_args()


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

    for k, case in cases.items():
        case = case[0]
        print(f"\n-------Processing case {k}")
        R, I, O = case["R"], case["I"], case["O"]

        nb_infile_seeks = (R[0]/I[0])*(R[1]/I[1])*(R[2]/I[2])

        o_cuts = get_cuts(R, O)
        i_cuts = get_cuts(R, I)
        duplicates = list()
        for o, i in zip(o_cuts, i_cuts):
            tmp_list = list()
            for e in o:
                if e in i:
                    tmp_list.append(e)
            duplicates.append(tmp_list)
        for i, tmp_list in enumerate(duplicates):
            for e in tmp_list:
                o_cuts[i].remove(e)
                i_cuts[i].remove(e)
        
        d = [len(tuple(o+i)) for o, i in zip(o_cuts, i_cuts)]
        alpha = [1 if d_tmp > 0 else 0 for d_tmp in d]

        a = (d[2]+1)*R[0]*R[1]*alpha[2]
        b = (d[1]+1)*R[0]*(R[2]/I[2])*(1-alpha[2])*alpha[1]
        c = (d[0]+1)*(R[1]/I[1])*(R[2]/I[2])*(1-alpha[2])*(1-alpha[1])*alpha[0]
        nb_outfile_seeks = a + b + c

        print(f"alpha: {alpha}")
        print(f"parts: {a}, {b}, {c}")
        print(f"nb infile seeks: {nb_infile_seeks}")
        print(f"nb outfile seeks: {nb_outfile_seeks}")
        