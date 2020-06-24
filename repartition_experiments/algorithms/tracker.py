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


    def is_complete(self, expected_shape):
        return self._iscomplete(self.i_dict, 0, expected_shape)


    def _iscomplete(self, d, dim_index, expected_shape):
        if not self.is_dimension_complete(d, dim_index, expected_shape):
            return False

        if not dim_index == len(expected_shape) -1:
            for d2 in d.values():
                if not self._iscomplete(d2, dim_index + 1, expected_shape):
                    return False

        return True
        
        
    def is_dimension_complete(self, d, dim_index, expected_shape):
        if len(list(d.keys())) == 0:
            return False

        all_ranges = list(d.keys())
        l = list()
        for _range in all_ranges:
            l.append(_range[0])
            l.append(_range[1])

        l.sort()
        if not min(l) == 0 or not max(l) == expected_shape[dim_index]:
            return False 

        l.remove(min(l))
        l.remove(max(l))
        if len(l) > 0:
            # all i must be present twice
            _set = set(l)
            for e in _set:
                l.remove(e)
                if not e in l:
                    return False 
        
        return True

