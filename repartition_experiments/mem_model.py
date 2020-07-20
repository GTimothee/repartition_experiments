from math import floor
import argparse, json, os, sys


def model(case, m):
    # WARNING: m is a number of voxels, not bytes
    print(f'\n--------Running new case--------')
    
    R, I, O = case["R"], case["I"], case["O"]

    partitionI = get_blocks_shape(R, I)  # partition of R by I
    n = partitionI[2]
    N = partitionI[1] * partitionI[2]

    Lambd = get_input_aggregate(O, I)
    B = [1, 1, Lambd[2]]

    for i in range(1):
        for j in range(1):
            for k in range(1):
                omega = [(i+1) * Lambd[0] % O[0], 
                         (j+1) * Lambd[1] % O[1],
                         (k+1) * Lambd[2] % O[2]]
                
                theta = [
                    Lambd[0] - omega[0],
                    Lambd[1] - omega[1],
                    Lambd[2] - omega[2]
                ]

                print(f'Omega: {omega}')
                print(f'Theta: {theta}')

                print(f'--------Storing F1--------')
                phi = floor(m / (omega[2] + Lambd[2]))
                print(f'Max value for Bj: {phi}')

                if phi >= theta[1]:
                    print(f'Setting Bj to thetaj')
                    B[1] = theta[1]
                else:
                    B[1] = phi if phi > 1 else 1
                    print("End of algorithm, Bj <- max_value")
                    return tuple(B), [1]

                print(f'--------Storing F2 and F3--------')
                phi2 = floor( (m - theta[1] * (omega[2] + Lambd[2])) / ((n+1) * Lambd[2]))
                print(f'Max value for lambdaj - thetaj: {phi2}')
                print(f'Max value for Bj: {B[1] + phi2}')

                if (B[1] + phi2) >= Lambd[1]:   
                    print(f'Setting Bj to lambdaj')
                    B[1] = Lambd[1]
                else:
                    B[1] = B[1] + phi2 
                    print("End of algorithm, Bj <- max_value")
                    return tuple(B), [1,2,3]

                print(f'--------Extending F1, F2 and F3--------')
                phi3 = floor( m / ( omega[2]*theta[1] + n*omega[1]*Lambd[2] + Lambd[1]*Lambd[2] ) )
                print(f'Max value for Bi: {phi3}')
                if phi3 >= theta[0]:
                    print(f'Setting Bi to thetai')
                    B[0] = theta[0]
                else:
                    B[0] = phi3 if phi3 > 1 else 1
                    print("End of algorithm, Bi <- max_value")
                    return tuple(B), [1,2,3]

                print(f'--------Storing F4, F5, F6, F7--------')
                phi4 = floor( (m - theta[0] * ( omega[2]*theta[1] + n*omega[j]*Lambd[2] + Lambd[1]*Lambd[2] )) / ((N+1) * Lambd[1] * Lambd[2]) )
                print(f'Max value for Bi: {B[0] + phi4}')
                if (B[0] + phi4) >= Lambd[0]:
                    print(f'Setting Bi to lambdai')
                    B[0] = Lambd[0]
                else:
                    B[0] = B[0] + phi4 
                    print("End of algorithm, Bi <- max_value")
                    return tuple(B), [1,2,3,4,5,6,7]

                return tuple(B), [1,2,3,4,5,6,7]


def get_arguments():
    """ Get arguments from console command.
    """
    parser = argparse.ArgumentParser(description="This experiment is referenced as experiment 3 in Guédon et al.")
    
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

    from repartition_experiments.algorithms.utils import get_blocks_shape
    from repartition_experiments.algorithms.keep_algorithm import get_input_aggregate

    ONE_GIG = 1000000000
    nb_gig = 16

    for k, case in cases.items():
        print(f"\n-------Processing case {k}")
        B, volumestokeep = model(case[0], nb_gig * ONE_GIG / 2) # WARNING: m is a number of voxels, not bytes
        print(f'Buffer shape for ref {case[0]["ref"]}: {B}')
        print(f'Volumes to keep for ref {case[0]["ref"]}: {volumestokeep}')