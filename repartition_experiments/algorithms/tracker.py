""" This class has been used to know if all parts of an output block have been written. 
Indeed, manipulating Volume objects (see algorithms/utils.py) we can use Tracker to add volumes 
and ask if all parts have been written using the is_complete() method.
For example, if you want to know if a list of Volumes is a partition of an output block, use is_complete.
"""

class Tracker:
    def __init__(self):
        self.i_dict = dict()


    def add_volume(self, v):
        p1, p2 = v.get_corners()
        slices = ((p1[0], p2[0]), (p1[1], p2[1]), (p1[2], p2[2]))

        if not slices[0] in self.i_dict.keys():
            self.i_dict[slices[0]] = dict()
        
        if not slices[1] in self.i_dict[slices[0]].keys():
            self.i_dict[slices[0]][slices[1]] = dict()

        if not slices[2] in self.i_dict[slices[0]][slices[1]].keys():
            self.i_dict[slices[0]][slices[1]][slices[2]] = dict() 


    def is_complete(self, corners):
        """ corners: corners of the output block -> output of the Volume.get_corners() method
        """
        p1, p2 = corners
        return self._iscomplete(self.i_dict, 0, p1, p2)


    def print(self):
        print("\n Tracker -----")
        for i, v in self.i_dict.items():
            print(f'i: {i}')
            for j, v2 in v.items():
                print(f'j: {j}')
                for k, v3 in v2.items():
                    print(f'k: {k}')


    def _iscomplete(self, d, index, p1, p2):
        keys = list(d.keys())

        l = list()  # test if keys start and end at the same points than corners
        for key in keys:
            start, end = key
            l.extend(list(range(start, end+1)))
        l.sort()
        start, end = l[0], l[-1]
        if not start == p1[index] or not end == p2[index]:
            # print(f"extremity points not matching: {(start, end)}!={(p1[index], p2[index])}")
            return False 
        
        s = list(set(l))
        expected = list(range(p1[index], p2[index]+1))
        s.sort()
        if not s == expected:
            # print(f"missing points {s}!={expected}")
            return False 

        last_dim = len(p1)-1
        if not index == last_dim:  
            for k, v in d.items():
                if not self._iscomplete(v, index + 1, p1, p2):
                    return False 
        
        return True