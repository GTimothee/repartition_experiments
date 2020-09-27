import random, json, argparse, os, sys


def get_random_array_shape():
    """ Generate a random original array shape.
    """
    PRIME_NUMBERS = [2, 3, 5, 7]
    POWERS = [1, 2]

    length = 0
    for _ in range(5):
        number = PRIME_NUMBERS[random.randint(0,3)]
        power = POWERS[random.randint(0,1)]
        if length == 0:
            length = number**power
        else:
            length *= number**power

    return (length, length, length)


def get_random_cases(limit, model):
    """ Get random cases (A,O,I,B) to test
    """
    A = get_random_array_shape()
    divisors = get_divisors(A[0])
    divisors.sort(reverse=True)
    divisors.remove(1)
    divisors = divisors[1:]
    if len(divisors) < 10:
        return []
        
    cases_list = list()
    print(f"Found A: {A}")
    print(f"Divisors: {divisors}")

    i = 0
    while i+1 < len(divisors):

        I = (divisors[i], divisors[i], divisors[i])
        O = (divisors[i+1], divisors[i+1], divisors[i+1])
        print(f"I: {I}")
        print(f"O: {O}")

        if model == "keep":
            _, _, _, candidates = get_buffer_candidates({"R": A, "I": I, "O": O})
            candidates_selected = list()

            print(f"Number candidates found: {len(candidates)}")
            _range = min(5, len(candidates))
            for i in range(_range):
                one_candidate = candidates[random.randint(0,len(candidates)-1)]

                if not type(one_candidate) is tuple:
                    print(one_candidate)
                    raise ValueError("")

                print(f"one candidate: {one_candidate}")
                cases_list.append((A, I, O, one_candidate))
                candidates_selected.append(one_candidate)
              
            print(f"Buffer candidates selected: {candidates_selected}")
        else:
            cases_list.append((A, I, O, I))

        # inverse I and O 
        O = (divisors[i], divisors[i], divisors[i])
        I = (divisors[i+1], divisors[i+1], divisors[i+1])
        print(f"I: {I}")
        print(f"O: {O}")

        if model == "keep":
            _, _, _, candidates = get_buffer_candidates({"R": A, "I": I, "O": O})
            candidates_selected = list()

            print(f"Number candidates found: {len(candidates)}")
            _range = min(5, len(candidates))
            for i in range(_range):
                one_candidate = candidates[random.randint(0,len(candidates)-1)]

                if not type(one_candidate) is tuple:
                    print(one_candidate)
                    raise ValueError("")

                print(f"one candidate: {one_candidate}")
                cases_list.append((A, I, O, one_candidate))
                candidates_selected.append(one_candidate)
            print(f"Buffer candidates selected: {candidates_selected}")
        else:
            cases_list.append((A, I, O, I))

        if len(cases_list) == limit:
            return cases_list
        elif len(cases_list) > limit:
            return cases_list[:limit]
        else:
            i += 2

    return cases_list


def get_arguments():
    """ Get arguments from console command.
    """
    parser = argparse.ArgumentParser(description="")
    
    parser.add_argument('paths_config', 
        action='store', 
        type=str, 
        help='Path to configuration file containing paths of data directories.')

    parser.add_argument('model', 
        action='store', 
        type=str, 
        help='',
        default="baseline")

    return parser.parse_args()


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)


def get_volumes_to_keep(A, B, O):
    # compute theta max
    buffers_partition, buffers = get_volumes(A, B)
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

    return volumestokeep


def keep_reading(B, I, R):
    """
    b: read buffer shape
    i: inblock shape
    r: original image shape
    """

    buffer_partition = get_blocks_shape(R, B)
    read_buffers = get_named_volumes(buffer_partition, B)
    
    infiles_partition = get_blocks_shape(R, I)
    inblocks = get_named_volumes(infiles_partition, I)

    nb_inblocks_openings = 0
    nb_inblocks_seeks = 0

    for buffer_index in sorted(read_buffers.keys()):
        read_buffer = read_buffers[buffer_index]

        for inblock in inblocks.values():
            if hypercubes_overlap(read_buffer, inblock):
                nb_inblock_seeks_tmp = write_buffer(read_buffer, inblock, I)
                nb_inblocks_seeks += nb_inblock_seeks_tmp
                nb_inblocks_openings += 1

    print(f"[Reality] Number inblocks opening: {nb_inblocks_openings}")
    print(f"[Reality] Number inblocks seeks: {nb_inblocks_seeks}")
    return nb_inblocks_openings + nb_inblocks_seeks


def keep_model_seeks(A, B, O):
    volumestokeep = get_volumes_to_keep(A, B, O)
    outfiles_partition = get_blocks_shape(A, O)
    outblocks = get_named_volumes(outfiles_partition, O)
    buffers = get_named_volumes(get_blocks_shape(A, B), B)
    arrays_dict, _, nb_file_openings, nb_inside_seeks = compute_zones_remake(B, O, A, volumestokeep, outfiles_partition, outblocks, buffers, False)

    W = [list(), list(), list()]
    for outblock_index, write_buffers in arrays_dict.items():
        for write_buff in write_buffers:
            p1, p2 = write_buff.get_corners()
            for d in range(3):
                if not p2[d] in W[d]:
                    W[d].append(p2[d])
    for d in range(3):
        W[d].sort()

    model_total = compute_keep_seeks_model(A, B, I, O, W)
    
    return model_total


if __name__ == "__main__":

    args = get_arguments()
    paths = load_json(args.paths_config)
    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    from repartition_experiments.algorithms.utils import get_partition, get_blocks_shape, get_named_volumes, numeric_to_3d_pos, Volume, get_volumes, get_theta, hypercubes_overlap
    from repartition_experiments.scripts_paper.baseline_seeks_model_remake import compute_baseline_seeks_model, compute_keep_seeks_model
    from repartition_experiments.scripts_exp.seek_calculator import get_buffer_candidates, get_divisors, compute_nb_seeks
    from repartition_experiments.scripts_paper.baseline_simulator import baseline_rechunk, write_buffer
    from repartition_experiments.algorithms.policy_remake import compute_zones_remake
    from repartition_experiments.scripts_paper.keep_algorithm_simulator import keep_algorithm

    # parameters
    seed = 25
    number_tests = 1000
    nb_case_per_A = 5
    model = args.model

    nb_tests = 0
    random.seed(seed)
    while nb_tests < number_tests:
        print(f"Number tests so far: {nb_tests}/{number_tests}")
        print(f"Computing new cases....")
        cases_list = get_random_cases(nb_case_per_A, model)
        print(f"End.")

        for case in cases_list:
            print(f"Case: {case}")
            A, I, O, B = case

            if model == "baseline":
                print(f"Computing with model...")
                nb_outfile_openings_model, nb_outfile_seeks_model, nb_infile_openings_model, nb_infile_seeks_model = compute_baseline_seeks_model(A, I, O)
                model_total = nb_outfile_openings_model + nb_outfile_seeks_model + nb_infile_openings_model + nb_infile_seeks_model

                print(f"Simulating baseline...")
                nb_outfile_openings, nb_outfile_seeks, nb_infile_openings, nb_infile_seeks = baseline_rechunk(O, I, A)
                reality_total = nb_outfile_openings + nb_outfile_seeks + nb_infile_openings + nb_infile_seeks

                print(f"Predicted: {model_total} seeks ({nb_outfile_openings_model} outfile openings, {nb_outfile_seeks_model} outfile seeks, {nb_infile_openings_model} infile openings, {nb_infile_seeks_model} infile seeks)")
                print(f"Reality: {reality_total} seeks ({nb_outfile_openings} outfile openings, {nb_outfile_seeks} outfile seeks, {nb_infile_openings} infile openings, {nb_infile_seeks} infile seeks)")

            else:  # model == "keep"

                # compute keep simulator
                volumestokeep = get_volumes_to_keep(A, B, O)
                nb_outfile_openings, nb_outfile_inside_seeks, nb_infile_openings, nb_infile_inside_seeks = keep_algorithm(A, O, I, B, volumestokeep)

                print(f"[Reality] total seeks due to read buffers: {nb_infile_openings + nb_infile_inside_seeks} ({nb_infile_openings} seeks from files openings, {nb_infile_inside_seeks} infile seeks)")
                print(f"[Reality] total seeks due to write buffers: {nb_outfile_openings + nb_outfile_inside_seeks} ({nb_outfile_openings} seeks from files openings, {nb_outfile_inside_seeks} infile seeks)")
                
                reality_total = nb_outfile_openings + nb_outfile_inside_seeks + nb_infile_openings + nb_infile_inside_seeks

                model_total = keep_model_seeks(A, B, O)

                print(f"Predicted: {model_total} seeks")
                print(f"Reality: {reality_total} seeks")

            if model_total != reality_total:
                print(f"ERROR---------")

        nb_tests += len(cases_list)