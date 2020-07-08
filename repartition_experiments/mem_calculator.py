import sys, json, argparse

DEBUG = False

def get_theta(buffers_volumes, buffer_index, _3d_index, O, B):
    T = list()
    Cs = list()
    for dim in range(len(buffers_volumes[buffer_index].p1)):
        if B[dim] < O[dim]:
            C = 0 
        else:            
            C = ((_3d_index[dim]+1) * B[dim]) % O[dim]
            if C == 0 and B[dim] != O[dim]:  # particular case 
                C = O[dim]

        if C < 0:
            raise ValueError("modulo should not return negative value")

        Cs.append(C)
        T.append(B[dim] - C)   
    
    if DEBUG: 
        print(f'\nProcessing buffer {buffer_index}')
        print(f'C: {Cs}')
        print(f'theta: {T}')

    return T, Cs


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
        help='Path to configuration file containing experiment cases.')

    return parser.parse_args()


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)


def compute_memory(R, O, I, B, volumestokeep):
    buffers_partition = get_blocks_shape(R, B)
    buffers_volumes = get_named_volumes(buffers_partition, B)

    # find omega and theta max
    omega_max = [0,0,0]
    T_max = [0,0,0]
    for buffer_index in buffers_volumes.keys():
        _3d_index = numeric_to_3d_pos(buffer_index, buffers_partition, order='C')
        T, Cs = get_theta(buffers_volumes, buffer_index, _3d_index, O, B)

        for i in range(3):
            if Cs[i] > omega_max[i]:
                omega_max[i] = Cs[i]
            if T[i] > T_max[i]:
                T_max[i] = T[i]

    print("omega max found:", omega_max)
    print("theta max found:", T_max)

    nb_bytes_per_voxel = 2
    buffersize = B[0]*B[1]*B[2]
    n = R[2]/B[2]
    N = R[1]/B[1] * R[2]/B[2]

    i, j, k = 0, 1, 2
    F1 = omega_max[k] * T_max[j] * T_max[i]
    F2 = T_max[k] * omega_max[j] * T_max[i]
    F3 = omega_max[k] * omega_max[j] * T_max[i]
    F4 = T_max[k] * T_max[j] * omega_max[i]
    F5 = omega_max[k] * T_max[j] * omega_max[i]
    F6 = T_max[k] * omega_max[1] * omega_max[i]
    F7 = omega_max[k] * omega_max[j] * omega_max[i]

    print('F1:', F1)
    print('F2:', F2)
    print('F3:', F3)
    print('F4:', F4)
    print('F5:', F5)
    print('F6:', F6)
    print('F7:', F7)

    print('buffer size: ', buffersize*nb_bytes_per_voxel, " bytes")
    max_mem = (F1 + n*(F2 + F3) + N*(F4 + F5 + F6 + F7) + buffersize) * nb_bytes_per_voxel
    print("max_mem: ", max_mem, " bytes")


def compute_for_case(args):
    cases = load_json(args.cases_config)
    for case_name, case in cases.items():
        print(f'Processing case {case_name}...')

        for run in case:
            R, O, I, B, volumestokeep = tuple(run["R"]), tuple(run["O"]), tuple(run["I"]), tuple(run["B"]), run["volumestokeep"]

            if case_name.split('_')[0] == "case 1":
                lambd = get_input_aggregate(O, I)
                B, volumestokeep = (lambd[0],lambd[1],lambd[2]), list(range(1,8))
                
            print(f'Current run ------ \nR: {R},\nO: {O},\nI: {I}\nB: {B}\nvolumestokeep: {volumestokeep}')
            print(f'ref: {run["ref"]}')

            compute_memory(R, O, I, B, volumestokeep)


if __name__ == "__main__":

    args = get_arguments()
    paths = load_json(args.paths_config)
    
    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    from repartition_experiments.algorithms.utils import get_named_volumes, get_blocks_shape, numeric_to_3d_pos
    from repartition_experiments.algorithms.keep_algorithm import get_input_aggregate

    if args.cases_config != "none":
        compute_for_case(args)
    else:
        # run = {
        #     "R": [1400,1400,1400],
        #     "I": [140,140,140],
        #     "O": [100,100,100],
        #     "B": [],
        #     "volumestokeep": [],
        #     "ref": 3
        # }
        
        runs = [{
            "R": [3500,3500,3500],
            "I": [350,350,350],
            "O": [500,500,500],
            "B": [500,700,700],
            "volumestokeep": [1,2,3],
            "ref": -1
        },{
            "R": [3500,3500,3500],
            "I": [350,350,350],
            "O": [500,500,500],
            "B": [350,700,700],
            "volumestokeep": [1,2,3],
            "ref": -1
        },
        {
            "R": [3500,3500,3500],
            "I": [350,350,350],
            "O": [500,500,500],
            "B": [175,700,700],
            "volumestokeep": [1,2,3],
            "ref": -1
        },
        {
            "R": [3500,3500,3500],
            "I": [350,350,350],
            "O": [500,500,500],
            "B": [1,500,700],
            "volumestokeep": [1],
            "ref": -1
        }]
        for run in runs:
            R, O, I, B, volumestokeep = tuple(run["R"]), tuple(run["O"]), tuple(run["I"]), tuple(run["B"]), run["volumestokeep"]
            # lambd = get_input_aggregate(O, I)
            # B, volumestokeep = (lambd[0],lambd[1],lambd[2]), list(range(1,8))
            compute_memory(R, O, I, B, volumestokeep)
    