import math, argparse, json, sys, time
from .algorithms.keep_algorithm import get_input_aggregate
from .algorithms.utils import get_volumes, numeric_to_3d_pos, get_theta
from .algorithms.policy_remake import compute_zones_remake
from .baseline_seek_model import get_cuts, preprocess

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

DEBUG=False

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


def compute_infile_seeks(R, B, I):
    b_cuts = get_cuts(R, B)
    i_cuts = get_cuts(R, I)

    d, nb_nocostly = preprocess(b_cuts, i_cuts, I)
    if DEBUG:
        print(f"d: {d}")
        print(f"nb_nocostly: {nb_nocostly}")

    a = (d[2])*R[0]*R[1]
    b = (d[1])*R[0]*nb_nocostly[2]
    c = (d[0])*nb_nocostly[1]*nb_nocostly[2]

    return a + b + c


def get_buffer_candidates(case):
    R, I, O  = case["R"], case["I"], case["O"]
    divisors = get_divisors(R[0]) # assuming R[0]=R[1]=R[2]

    # buffer cannot be bigger than lambda
    lambd = get_input_aggregate(O, I)
    limit_j = divisors.index(lambd[1])
    limit_i = divisors.index(lambd[0])
    all_j = divisors[:limit_j+1]  
    all_i = divisors[:limit_i+1]
    all_j.sort(reverse=True)
    all_i.sort(reverse=True)
    # print(f"all j: {all_j}")
    # print(f"all i: {all_i}")

    # compute all possible buffers
    all_buffers = list()
    increasing_j = [(1, div, lambd[2]) for div in all_j]
    increasing_i = [(div, lambd[1], lambd[2]) for div in all_i]
    all_buffers.extend(increasing_i)
    all_buffers.extend(increasing_j)
    return R, O, I, all_buffers


def compute_nb_seeks(B, O, R, I):
    # compute theta max
    buffers_partition, buffers = get_volumes(R, B)
    T_max = [0,0,0]
    for buffer_index in buffers.keys():
        _3d_index = numeric_to_3d_pos(buffer_index, buffers_partition, order='C')
        T, Cs = get_theta(buffers, buffer_index, _3d_index, O, B)
        for i in range(3):
            if T[i] > T_max[i]:
                T_max[i] = T[i]
    # print(f"Found theta max: {T_max}")

    # get volumes to keep
    volumestokeep = [1]
    if B[1] > T_max[1]:
        print(f"{B[1]} > {T_max[1]}")
        volumestokeep.extend([2,3])
    if B[0] > T_max[0]:
        print(f"{B[0]} > {T_max[0]}")
        volumestokeep.extend([4,5,6,7])
    # print(f"volumes to keep: {volumestokeep}")

    # compute outfiles seeks
    outfiles_partition, outvolumes = get_volumes(R, O)
    t1 = time.time()
    arrays_dict_new, _, nb_file_openings, nb_inside_seeks = compute_zones_remake(B, O, R, volumestokeep, outfiles_partition, outvolumes)
    t1 = time.time() - t1
    if DEBUG:
        print(f"processing time compute_zones_remake: {t1}")

    # compute infiles seeks
    t2 = time.time()
    nb_infile_seeks = compute_infile_seeks(R, B, I)
    t2 = time.time() - t2
    if DEBUG:
        print(f"processing time compute_infile_seeks: {t2}")

        print(f"nb outfiles seeks: {nb_file_openings + nb_inside_seeks}")
        print(f"nb infiles seeks: {nb_infile_seeks}")

    total_seeks = nb_file_openings + nb_inside_seeks + nb_infile_seeks
    if DEBUG:
        print(f"nb seeks: {total_seeks}")

    return total_seeks, t1+t2


if __name__ == "__main__":
    args = get_arguments()
    paths = load_json(args.paths_config)
    cases = load_json(args.cases_config)
    
    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)
            
    from repartition_experiments.algorithms.policy_remake import compute_zones_remake
    from repartition_experiments.algorithms.policy import compute_zones
    from repartition_experiments.algorithms.keep_algorithm import get_input_aggregate
    from repartition_experiments.algorithms.utils import get_volumes, numeric_to_3d_pos, get_theta
    from repartition_experiments.baseline_seek_model import get_cuts, preprocess

    import logging
    import logging.config
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
    })

    if not args.case_name in cases.keys():
        print("bad case name")
        sys.exit(0)
    case = cases[args.case_name][0]
    R, I, O  = case["R"], case["I"], case["O"]
    divisors = get_divisors(R[0]) # assuming R[0]=R[1]=R[2]

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

    # for b in all_buffers:
    #     print(b)

    # sys.exit(0)

    total_time_old = 0
    total_time_new = 0

    buff_treated = []
    print(f"Number buffer shapes to test: {len(tuple(all_buffers))}")
    for B in all_buffers:
        print(f"\n--------------\ncase: {B}")

        if B in buff_treated:
            continue
        else:
            buff_treated.append(B)

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
        arrays_dict_old, _, nb_file_openings, nb_inside_seeks = compute_zones(B, O, R, volumestokeep, buffers_partition, outfiles_partition, buffers, outvolumes)
        t = time.time() - t
        print(f"processing time old: {t}")
        total_time_old += t
        
        t = time.time()
        arrays_dict_new, _, nb_file_openings, nb_inside_seeks = compute_zones_remake(B, O, R, volumestokeep, outfiles_partition, outvolumes)
        t = time.time() - t
        print(f"processing time new: {t}")
        total_time_new += t

        # verification
        assert len(arrays_dict_old.keys()) == len(arrays_dict_new.keys())
        for k, old_list in arrays_dict_old.items():
            new_list = arrays_dict_new[k]
            for v_old in old_list:
                t_old = v_old.get_corners()
                is_in = False
                for v_new in new_list:
                    t_new = v_new.get_corners()
                    if t_old[0] == t_new[0] and t_old[1] == t_new[1]:
                        is_in = True
                        break
                assert is_in
        print("verification succeeded")

        t = time.time()
        nb_infile_seeks = compute_infile_seeks(R, B, I)
        t = time.time() - t
        print(f"processing time 2: {t}")
        print(f"nb outfiles seeks: {nb_file_openings + nb_inside_seeks}")
        print(f"nb infiles seeks: {nb_infile_seeks}")
        print(f"nb seeks: {nb_file_openings + nb_inside_seeks + nb_infile_seeks}")

    total_time = time.time() - total_time
    print(f"Total time: {total_time} seconds")