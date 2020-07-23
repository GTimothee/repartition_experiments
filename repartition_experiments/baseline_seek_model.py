import argparse, json, sys, os

def get_cuts(big_block, small_block):
    def get_cuts_by_dim(big_block, small_block, i):
        nb_max = int(big_block[i] / small_block[i])
        cuts = list(tuple([small_block[i]*j for j in range(nb_max)])) # stop before R[dim]
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
        help='')

    return parser.parse_args()


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)


if __name__ == "__main__":    
    args = get_arguments()
    paths = load_json(args.paths_config)
    indir_path, outdir_path = os.path.join(paths["ssd_path"], 'indir'), os.path.join(paths["ssd_path"], 'outdir')
    cases = load_json(args.cases_config)

    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    from repartition_experiments.algorithms.baseline_algorithm import baseline_rechunk

    for k, case in cases.items():
        case = case[0]
        print(f"\n-------Processing case {k}")
        R, I, O = case["R"], case["I"], case["O"]
        print(f'Current run ------ \nR: {R},\nO: {O},\nI: {I}')

        nb_infile_seeks = (R[0]/I[0])*(R[1]/I[1])*(R[2]/I[2])

        o_cuts = get_cuts(R, O)
        print(o_cuts)
        i_cuts = get_cuts(R, I)
        print(i_cuts)

        # remove duplicates (where there is match betw O and I)
        d = list()
        dim_index = 0
        for o, i in zip(o_cuts, i_cuts):
            d_dim = 0
            last_was_cut = False
            
            o = list(map(lambda x: (0, x), o))
            i = list(map(lambda x: (1, x), i))
            values = o+i
            values.sort(key=lambda x:x[1])

            print(f"values: {values}")
            print(f"len values {len(values)}")
            j = 0
            last_infile_cut_index = -1
            while j < len(values): 
                print(f"j: {j}")    
                if j == (len(values) - 1) or values[j][1] != values[j+1][1]:
                    print("last_infile_cut_index ", last_infile_cut_index)
                    print(len(i))

                    if last_infile_cut_index > -1:
                        lower_i = i[last_infile_cut_index][1]
                        upper_i = i[last_infile_cut_index][1]+I[dim_index]
                    else :
                        lower_i = 0
                        upper_i = i[0][1]

                    if values[j][0] == 0 and (values[j][1] - O[dim_index] >= lower_i or values[j][1] + O[dim_index] <= upper_i):
                        print("a")
                        last_was_cut = False
                        pass
                    else:
                        print("b")
                        if values[j][0] == 1:
                            last_infile_cut_index += 1
                    
                        if last_was_cut:
                            d_dim += 1
                        else:
                            d_dim += 2
                            last_was_cut = True
                    j += 1
                else:
                    print("c")
                    last_was_cut = False 
                    j += 2

            print(f"d_dim: {d_dim}")
            d.append(d_dim)
            dim_index += 1
        
        alpha = [1 if d_tmp > 0 else 0 for d_tmp in d]

        print(f"d: {d}")
        a = (d[2])*R[0]*R[1]*alpha[2]
        b = (d[1])*R[0]*(R[2]/I[2])*(1-alpha[2])*alpha[1]
        c = (d[0])*(R[1]/I[1])*(R[2]/I[2])*(1-alpha[2])*(1-alpha[1])*alpha[0]
        nb_outfile_seeks = a + b + c

        print(f"Running baseline algorithm...")
        t_read, t_write, seek_data = baseline_rechunk(indir_path, outdir_path, O, I, R, 'HDF5', False, debug_mode=False, clean_out_dir=False, dont_write=True)
        nb_outfile_openings_exp, nb_outfile_seeks_exp, nb_infile_openings_exp, nb_infile_seeks_exp = seek_data 

        print(f"alpha: {alpha}")
        print(f"parts: {a}, {b}, {c}")
        print(f"nb infile seeks: {nb_infile_seeks} (reality: {nb_infile_openings_exp}+{nb_infile_seeks_exp})")
        print(f"nb outfile seeks: {nb_outfile_seeks} (reality: {nb_outfile_openings_exp}+{nb_outfile_seeks_exp})")

        # if k == "case 1_1":
        #     sys.exit()
        assert nb_outfile_seeks == nb_outfile_seeks_exp
        