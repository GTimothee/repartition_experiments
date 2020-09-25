import math

def shape_to_end_coords(M, A, d=3):
    '''
    M: block shape M=(M1, M2, M3). Example: (500, 500, 500)
    A: input array shape A=(A1, A2, A3). Example: (3500, 3500, 3500)
    Return: end coordinates of the blocks, in each dimension. Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    '''
    return [ [ (j+1)*M[i] for j in range(int(A[i]/M[i])) ] for i in range(d)]

def seeks(A, M, D):
    '''
    A: shape of the large array. Example: (3500, 3500, 3500)
    M: coordinates of memory block ends (read or write). Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    D: coordinates of disk block ends (input or output). Example: ([500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500],
                                                                   [500, 1000, 1500, 2000, 2500, 3000, 3500])
    Returns: number of seeks required to write M blocks into D blocks. This number is also the number of seeks
             to read D blocks into M blocks.
    '''

    c = [ 0 for i in range(len(A))] # number of cuts in each dimension
    m = [] # number of matches in each dimension

    n = 0 # Total number of disk blocks
    for i in range(len(A)):
        n *= len(D[i])

    for d in range(len(A)): # d is the dimension index
        
        nd = len(D[d])
        Cd = [ ]  # all the cut coordinates (for debugging and visualization)
        for i in range(nd): # for each output block, check how many pieces need to be written
            if i == 0:
                Cid = [ m for m in M[d] if 0 < m and m < D[d][i] ]  # number of write block endings in the output block
            else:               
                Cid = [ m for m in M[d] if D[d][i-1] < m and m < D[d][i] ]  # number of write block endings in the output block
            if len(Cid) == 0:
                continue
            c[d] += len(Cid) + 1
            Cd += Cid

        m.append(len(set(M[d]).union(set(D[d]))) - c[d])

    s = A[0]*A[1]*c[2] + A[0]*c[1]*m[2] + c[0]*m[1]*m[2] + n

    return s


def compute_nb_seeks_model(A, I, O):
    ni = int(A[0]/I[0] * A[1]/I[1] * A[2]/I[2])
    nb_outfile_openings = 0
    nb_outfile_seeks = seeks(A, shape_to_end_coords(I, A), shape_to_end_coords(O, A))
    nb_infile_openings = ni 
    nb_infile_seeks = 0
    return [nb_outfile_openings, nb_outfile_seeks, nb_infile_openings, nb_infile_seeks]