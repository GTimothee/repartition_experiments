from math import floor
import argparse, json, os, sys


def model(case):
    # WARNING: m is a number of voxels, not bytes
    print(f'\n--------Running new case--------')
    
    R, I, O, m = case["R"], case["I"], case["O"], case["m"]

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

    return parser.parse_args()


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)


if __name__ == "__main__":
    # WARNING: m is a number of voxels, not bytes

    # test cases below:
    # cases = [
    #     {   
    #         'R': (1,120,120),
    #         'I': (1,60,60),
    #         'O': (1,40,40),
    #         'm': 60*40 + (40*20), # buffer size + F1
    #         'Bexpected': (1,40,60)
    #     },{
    #         'R': (1,120,120),
    #         'I': (1,60,60),
    #         'O': (1,40,40),
    #         'm': 60*60 + 40*20 + n*60*20,  # buffer size + F1 + n(F2+F3)
    #         'Bexpected': (1,60,60)
    #     },{
    #         'R': (120,120,120),
    #         'I': (60,60,60),
    #         'O': (40,40,40),
    #         'm': 60*60*40 + 40*20*40 + n*60*20*40,  # buffer size + F1 + n(F2+F3)
    #         'Bexpected': (40,60,60)
    #     },{
    #         'R': (120,120,120),
    #         'I': (60,60,60),
    #         'O': (40,40,40),
    #         'm': 60*60 + 40*20 + n*60*20,  # buffer size + F1 + n(F2+F3)
    #         'Bexpected': (1,60,60)
    #     },{
    #         'R': (120,120,120),
    #         'I': (60,60,60),
    #         'O': (40,40,40),
    #         'm': 60*60*60 + 40*20*40 + n*60*20*40 + N*20*60*60,  # buffer size + F1 + n(F2+F3) + N(F4+F5+F6+F7)
    #         'Bexpected': (60,60,60)
    #     }
    # ]
    # for case in cases:
    #     B = model(case, n, N)
    #     print(f'Final buffer shape: {B}')
    #     try:
    #         assert case["Bexpected"] == B
    #         print("Success.")
    #     except:
    #         print('Bad output.')     
    
    args = get_arguments()
    paths = load_json(args.paths_config)

    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    from repartition_experiments.algorithms.utils import get_blocks_shape
    from repartition_experiments.algorithms.keep_algorithm import get_input_aggregate

    ONE_GIG = 1000000000
    nb_gig = 5
    cases = [
        {
            'R': (3500,3500,3500),
            'I': (875,875,875),
            'O': (3500,3500,3500),
            'm': nb_gig*ONE_GIG/2,
            'ref': 0
        },{
            'R': (3500,3500,3500),
            'I': (350,350,350),
            'O': (3500,3500,3500),
            'm': nb_gig*ONE_GIG/2,
            'ref': 1
        },{
            'R': (3500,3500,3500),
            'I': (875,875,875),
            'O': (875,875,3500),
            'm': nb_gig*ONE_GIG/2,
            'ref': 2
        },{
            'R': (3500,3500,3500),
            'I': (350,350,350),
            'O': (350,350,3500),
            'm': nb_gig*ONE_GIG/2,
            'ref': 3
        },{
            'R': (3500,3500,3500),
            'I': (875,875,875),
            'O': (875,3500,875),
            'm': nb_gig*ONE_GIG/2,
            'ref': 4
        },{
            'R': (3500,3500,3500),
            'I': (350,350,350),
            'O': (350,3500,350),
            'm': nb_gig*ONE_GIG/2,
            'ref': 5
        },{
            'R': (3500,3500,3500),
            'I': (875,875,875),
            'O': (3500,875,875),
            'm': nb_gig*ONE_GIG/2,
            'ref': 6
        },{
            'R': (3500,3500,3500),
            'I': (350,350,350),
            'O': (3500,350,350),
            'm': nb_gig*ONE_GIG/2,
            'ref': 7
        }
    ]

    for case in cases:
        B, volumestokeep = model(case)
        print(f'Buffer shape for ref {case["ref"]}: {B}')
        print(f'Volumes to keep for ref {case["ref"]}: {volumestokeep}')