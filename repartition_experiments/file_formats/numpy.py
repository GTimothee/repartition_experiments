

class NUMPY_manager:
    def __init__(self):
        self.filename_regex = "[0-9]*_[0-9]*_[0-9]*.npy"
        self.ext = '.npy'

    def remove_all(self, dirpath):
        workdir = os.getcwd()
        os.chdir(dirpath)
        for filename in glob.glob("*" + self.ext):
            os.remove(filename)
        os.chdir(workdir)
        

    def clean_directory(self, dirpath):
        """ Remove intermediary files from split/rechunk from a directory (matching chunks regex).
        See __init__ for regex
        """
        workdir = os.getcwd()
        os.chdir(dirpath)
        for filename in glob.glob(self.filename_regex):
            os.remove(filename)
        os.chdir(workdir)
    

    def get_input_files(self, input_dirpath):
        """ Return a list of input files paths matching chunks regex
        See __init__ for regex
        """
        workdir = os.getcwd()
        os.chdir(input_dirpath)
        infiles = list()
        for filename in glob.glob(self.filename_regex):
            infiles.append(os.path.join(input_dirpath, filename))
        os.chdir(workdir)
        return infiles


    def get_filepath(self, i, j, k, dirpath):
        filename = f'{i}_{j}_{k}' + self.ext
        return os.path.join(dirpath, filename)


    def read_data(self, i, j, k, dirpath, slices):
        """ Read part of a chunk
        """
        filepath = self.get_filepath(i, j, k, dirpath)
        return self.read_data_from_fp(filepath, slices)


    def read_data_from_fp(self, filepath, slices):
        """ Read part of a chunk from filepath
        """
        if slices == None:
            data = np.load(input_file)
        else:
            data = np.load(input_file, mmap_mode='r')
            s = slices
            data = data[s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]] 
        return data


    def get_dataset(self, filepath):
        return np.load(filepath, mmap_mode='r')


    def write_data(self, i, j, k, outdir_path, data, s2, O, dtype=np.float16):
        """ Write data at region _slices in outfilepath
        Used to create a file of shape O and write data into a part of that file
        """
        outfilepath = filepath = self.get_filepath(i, j, k, outdir_path)

        if os.path.isfile(outfilepath):
            mode = 'r+'
        else:
            mode = 'w'

        empty_dataset = False
        
        if not "/data" in f.keys():
            if O != data.shape:
                    #print(f"O != data.shape: {O} != {data.shape}")
                null_arr = np.zeros(O, dtype=dtype)
                outdset = f.create_dataset("/data", O, data=null_arr, dtype=dtype)  # initialize an empty dataset
                outdset[s2[0][0]:s2[0][1],s2[1][0]:s2[1][1],s2[2][0]:s2[2][1]] = data
            else:
                np.save(outfilepath, data)

            empty_dataset = True
        else:
            outdset = f["/data"]
            outdset[s2[0][0]:s2[0][1],s2[1][0]:s2[1][1],s2[2][0]:s2[2][1]] = data

        return empty_dataset


    def write(self, outfilepath, data, cs, _slices=None, dtype=np.float16): 
        """ Write data in file, data.shape == file.shape
        Used to write original array or complete file
        """
        if os.path.isfile(outfilepath):
            mode = 'r+'
        else:
            mode = 'w'

        with h5py.File(outfilepath, mode) as f:

            if _slices != None:
                if not "/data" in f.keys():
                    null_arr = np.zeros(cs, dtype=dtype)
                    outdset = f.create_dataset("/data", cs, data=null_arr, dtype=dtype) 
                else:
                    outdset = f["/data"]

                outdset[_slices[0][0]:_slices[0][1],_slices[1][0]:_slices[1][1],_slices[2][0]:_slices[2][1]] = data
            else:
                f.create_dataset("/data", cs, data=data, dtype=dtype)            


    def test_write(self, outfile_path, s, subarr_data):
        """ Used in baseline for verifying subarray writing
        """
        with h5py.File(outfile_path, 'r') as f:
            stored = f['/data'][s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]]
            if np.allclose(stored, subarr_data):
                logger.debug("[success] data successfully stored.")
            else:
                logger.debug("[error] in data storage")


    def close_infiles(self):
        clean_files()


    def read_all(self, filepath):
        """ Read all the file and return the whole array
        """
        dset = self.get_dataset(filepath, '/data')
        return dset[()]