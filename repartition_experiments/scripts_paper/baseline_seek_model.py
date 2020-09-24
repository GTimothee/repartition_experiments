import argparse, json, sys, os

""" Script used to compute the amount of seeks produced by the baseline algorithm.
"""

def get_cuts(big_block, small_block):
    """ Get the list of the shape mismatches between big block and small_blocks.
    The big block is partitioned by the small blocks.
    In particular, this function is used for computing the cuts of the input image by the buffers. 

    Arguments: 
    ----------  
        big_block: shape of big block
        small_block: shape of a small block
    """
    def get_cuts_by_dim(big_block, small_block, i):
        nb_max = int(big_block[i] / small_block[i])
        cuts = list(tuple([small_block[i]*j for j in range(nb_max+1)])) # stop before R[dim]
        cuts.remove(0) # remove 0
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
        help='Path to file containing the cases.')

    parser.add_argument('case_name', 
        action='store', 
        type=str, 
        help='Name of case to run from the cases_config file.')

    return parser.parse_args()


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)


def preprocess(buffer_cuts, block_cuts, block_shape):
    """ Algorithm that commputes the number of cuts (costly and non costly) in order to compute the number of seeks.

    Arguments: 
    ----------
        buffer_cuts: list of boundaries of the read buffers
        block_cuts: list of boundaries of the output blocks
        block_shape: shape of the output blocks

    Return:
        d: number of costly cuts, by dimension
        nb_nocostly: number of non costly cuts
    """
    nb_nocostly = [0,0,0]
    d = list()
    dim_index = 0
    for o, i in zip(block_cuts, buffer_cuts):
        d_dim = 0
        
        o = list(map(lambda x: (0, x), o))
        i = list(map(lambda x: (1, x), i))
        values = o+i
        values.sort(key=lambda x:x[1])

        # print(f"values: {values}")
        # print(f"len values {len(values)}")
        j = 0
        last_infile_cut_index = -1
        while j < len(values): # for each cut
            
            # test if not costly
            cond1 = (values[j][1] == values[j+1][1])
            lower_i = i[last_infile_cut_index][1] if last_infile_cut_index > -1 else 0
            cond2 = (values[j][0] == 0 and values[j][1] - block_shape[dim_index] >= lower_i)

            if cond2:
                nb_nocostly[dim_index] += 1
            else:
                d_dim += 1

            if values[j][0] == 1 or cond1:
                last_infile_cut_index += 1

            if cond1:
                j += 2
            else:
                j += 1  

        d.append(d_dim)
        dim_index += 1

    return d, nb_nocostly


if __name__ == "__main__":    
    args = get_arguments()
    paths = load_json(args.paths_config)
    indir_path, outdir_path = os.path.join(paths["ssd_path"], 'indir'), os.path.join(paths["ssd_path"], 'outdir')
    

    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    from repartition_experiments.algorithms.baseline_algorithm import baseline_rechunk

    # cases = load_json(args.cases_config) <- to use a file as input containing cases
    cases = { # cases used for the paper (table 1)
        "case 1_0": [{
                "R": [3500,3500,3500],
                "I": [500,500,875],
                "O": [500,500,500],
                "ref": 0
        }],
        "case 1_1": [{
                "R": [3500,3500,3500],
                "I": [500,875,500],
                "O": [500,500,500],
                "ref": 1
        }],
        "case 1_2": [{
                "R": [3500,3500,3500],
                "I": [875,500,500],
                "O": [500,500,500],
                "ref": 2
        }],
        "case 1_3": [{
                "R": [3500,3500,3500],
                "I": [875,875,500],
                "O": [500,500,500],
                "ref": 3
        }],
        "case 1_4": [{
                "R": [3500,3500,3500],
                "I": [875,875,875],
                "O": [500,500,500],
                "ref": 4
        }],
    }
    
    print(f"Selected case {args.case_name}")

    if not args.case_name in cases.keys():
        print("bad case name")
        sys.exit(0)

    for k, case in cases.items():
        if args.case_name == k:
            case = case[0]
            print(f"\n-------Processing case {k}")
            R, I, O = case["R"], case["I"], case["O"]
            print(f'Current run ------ \nR: {R},\nO: {O},\nI: {I}')

            nb_infile_seeks = (R[0]/I[0])*(R[1]/I[1])*(R[2]/I[2])

            o_cuts = get_cuts(R, O)
            i_cuts = get_cuts(R, I)

            d, nb_nocostly = preprocess(i_cuts, o_cuts, O)

            print(f"d: {d}")
            print(f"nb_nocostly: {nb_nocostly}")
            a = (d[2])*R[0]*R[1]
            b = (d[1])*R[0]*nb_nocostly[2]
            c = (d[0])*nb_nocostly[1]*nb_nocostly[2]
            nb_outfile_seeks = a + b + c

            print(f"nb outfile seeks: {nb_outfile_seeks}")

    print("finished.")
        