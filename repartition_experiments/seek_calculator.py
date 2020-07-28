import math, argparse, json, sys, time

def get_divisors(n) : 
    i = 1
    divisors = list()
    while i <= math.sqrt(n): 
        if (n % i == 0): 
            if (n / i == i): 
                divisors.append(int(n/i)) 
            else : 
                divisors.append(int(n/i))
                divisors.append(int(n/int(n/i))) 
        i = i + 1
    divisors.sort()
    return divisors


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




def compute_infile_seeks(R, B, I):

    b_cuts = get_cuts(R, B)
    i_cuts = get_cuts(R, I)

    d, nb_nocostly = preprocess(b_cuts, i_cuts)
    
    alpha = [1 if d_tmp > 0 else 0 for d_tmp in d]

    print(f"d: {d}")
    print(f"nb_nocostly: {nb_nocostly}")
    a = (d[2])*R[0]*R[1]
    b = (d[1])*R[0]*nb_nocostly[2]
    c = (d[0])*nb_nocostly[1]*nb_nocostly[2]

    return a + b + c


if __name__ == "__main__":
    args = get_arguments()
    paths = load_json(args.paths_config)
    
    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)
            
    from repartition_experiments.algorithms.policy import compute_zones
    from repartition_experiments.algorithms.keep_algorithm import get_input_aggregate
    from repartition_experiments.algorithms.utils import get_volumes, numeric_to_3d_pos, get_theta
    from repartition_experiments.baseline_seek_model import get_cuts

    import logging
    import logging.config
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
    })

    R = (1400,1400,1400)
    I = (70,70,70)
    O = (100,100,100)
    divisors = get_divisors(R[0])

    # buffer cannot be bigger than lambda
    lambd = get_input_aggregate(O, I)
    limit_j = divisors.index(lambd[1])
    limit_i = divisors.index(lambd[0])
    all_j = divisors[:limit_j+1]  
    all_i = divisors[:limit_i+1]
    all_j.sort(reverse=True)
    all_i.sort(reverse=True)
    print(f"all j: {all_j}")
    print(f"all i: {all_i}")

    # compute all possible buffers
    all_buffers = list()
    increasing_j = [(1, div, lambd[2]) for div in all_j]
    increasing_i = [(div, lambd[1], lambd[2]) for div in all_i]
    all_buffers.extend(increasing_i)
    all_buffers.extend(increasing_j)

    all_buffers = set(all_buffers)
    print(f"Number buffer shapes to test: {len(all_buffers)}")
    for B in all_buffers:
        print(f"\n--------------\ncase: {B}")

        # compute theta max
        buffers_partition, buffers = get_volumes(R, B)
        T_max = [0,0,0]
        for buffer_index in buffers.keys():
            _3d_index = numeric_to_3d_pos(buffer_index, buffers_partition, order='C')
            T, Cs = get_theta(buffers, buffer_index, _3d_index, O, B)
            for i in range(3):
                if T[i] > T_max[i]:
                    T_max[i] = T[i]
        print(f"Found theta max: {T_max}")

        # get volumes to keep
        volumestokeep = [1]
        if B[1] > T_max[1]:
            print(f"{B[1]} > {T_max[1]}")
            volumestokeep.extend([2,3])
        if B[0] > T_max[0]:
            print(f"{B[0]} > {T_max[0]}")
            volumestokeep.extend([4,5,6,7])
        print(f"volumes to keep: {volumestokeep}")

        outfiles_partition, outvolumes = get_volumes(R, O)
        t = time.time()
        _, _, nb_file_openings, nb_inside_seeks = compute_zones(B, O, R, volumestokeep, buffers_partition, outfiles_partition, buffers, outvolumes)
        t = time.time() - t
        print(f"processing time 1: {t}")
        t = time.time()
        nb_infile_seeks = compute_infile_seeks(R, B, I)
        t = time.time() - t
        print(f"processing time 2: {t}")
        print(f"nb outfiles seeks: {nb_file_openings + nb_inside_seeks}")
        print(f"nb infiles seeks: {nb_infile_seeks}")
        print(f"nb seeks: {nb_file_openings + nb_inside_seeks + nb_infile_seeks}")