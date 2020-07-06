class VoxelTracker:
    def __init__(self):
        self.nb_voxels = 0 # nb_voxels actually in RAM
        self.max = 0  # max nb voxels reached 
        self.history = list()


    def add_voxels(self, x):
        self.nb_voxels += x 
        if self.nb_voxels > self.max:
            self.max = self.nb_voxels
        self.history.append(self.nb_voxels)


    def remove_voxels(self, x):
        self.nb_voxels -= x 
        self.history.append(self.nb_voxels)


    def get_history(self):
        return self.history


    def get_max(self):
        return self.max