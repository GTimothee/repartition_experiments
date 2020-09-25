import random, json, argparse, os, sys


def get_random_array_shape():
    """ Generate a random original array shape.
    """
    PRIME_NUMBERS = [2, 3, 5, 7, 11]
    POWERS = [1, 2, 3]

    length = 0
    for _ in range(3):
        number = PRIME_NUMBERS[random.randint(0,4)]
        power = POWERS[random.randint(0,2)]
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
            candidates = get_buffer_candidates({"R": A, "I": I, "O": O})
            print(f"Number candidates found: {len(candidates)}")
            _range = min(5, len(candidates))
            for i in range(_range):
                cases_list.append((A, I, O, candidates[random.randint(0,len(candidates)-1)]))
              
            print(f"Buffer candidates selected: {candidates}")
        else:
            cases_list.append((A, I, O, I))

        # inverse I and O 
        O = (divisors[i], divisors[i], divisors[i])
        I = (divisors[i+1], divisors[i+1], divisors[i+1])
        print(f"I: {I}")
        print(f"O: {O}")

        if model == "keep":
            candidates = get_buffer_candidates({"R": A, "I": I, "O": O})
            print(f"Number candidates found: {len(candidates)}")
            _range = min(5, len(candidates))
            for i in range(_range):
                cases_list.append((A, I, O, candidates[random.randint(0,len(candidates)-1)]))
            print(f"Buffer candidates selected: {candidates}")
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

    from repartition_experiments.scripts_paper.baseline_seeks_model_remake import compute_nb_seeks_model
    from repartition_experiments.scripts_exp.seek_calculator import get_buffer_candidates, get_divisors
    from repartition_experiments.scripts_paper.baseline_simulator import baseline_rechunk

    # parameters
    seed = 42
    number_tests = 1000
    nb_case_per_A = 5
    model = "baseline"

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

            print(f"Computing with model...")
            nb_outfile_openings_model, nb_outfile_seeks_model, nb_infile_openings_model, nb_infile_seeks_model = compute_nb_seeks_model(A, I, O)
            model_total = nb_outfile_openings_model + nb_outfile_seeks_model + nb_infile_openings_model + nb_infile_seeks_model

            print(f"Simulating baseline...")
            nb_outfile_openings, nb_outfile_seeks, nb_infile_openings, nb_infile_seeks = baseline_rechunk(O, I, A)
            reality_total = nb_outfile_openings + nb_outfile_seeks + nb_infile_openings + nb_infile_seeks

            print(f"Predicted: {model_total} seeks ({nb_outfile_openings_model} outfile openings, {nb_outfile_seeks_model} outfile seeks, {nb_infile_openings_model} infile openings, {nb_infile_seeks_model} infile seeks)")
            print(f"Reality: {reality_total} seeks ({nb_outfile_openings} outfile openings, {nb_outfile_seeks} outfile seeks, {nb_infile_openings} infile openings, {nb_infile_seeks} infile seeks)")

            if model_total != reality_total:
                raise ValueError("Error")

        nb_tests += len(cases_list)