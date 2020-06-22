import atexit, os, h5py, glob, logging
import numpy as np
from h5py import Dataset

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__ + 'hdf5_filemanager')

SOURCE_FILES = list() # opened files to be closed after processing


@atexit.register
def clean_files():
    """ Clean the global list of opened files that are being used to create dask arrays. 
    """
    for f in SOURCE_FILES:
        f.close()


def file_in_list(file_pointer, file_list):
    """ Check if a file pointer points to the same file than another file pointer in the file_list.
    """
    for fp in file_list:
        if file_pointer == fp:
            return True 

    return False


class HDF5_manager:
    def __init__(self):
        self.filename_regex = "[0-9]*_[0-9]*_[0-9]*.hdf5"


    def clean_directory(self, dirpath):
        """ Remove intermediary files from split/rechunk.
        """
        workdir = os.getcwd()
        os.chdir(dirpath)
        for filename in glob.glob(self.filename_regex):
            os.remove(filename)
        os.chdir(workdir)
    

    def get_input_files(self, input_dirpath):
        """ Return a list of input files paths
        """
        workdir = os.getcwd()
        os.chdir(input_dirpath)
        infiles = list()
        for filename in glob.glob(self.filename_regex):
            infiles.append(os.path.join(input_dirpath, filename))
        os.chdir(workdir)
        return infiles


    def write_data(self, i, j, k, outdir_path, data, s2, O, dtype=np.float16):
        """ File must not exist
        """
        out_filename = f'{i}_{j}_{k}.hdf5'
        outfilepath = os.path.join(outdir_path, out_filename)

        if os.path.isfile(outfilepath):
            mode = 'r+'
        else:
            mode = 'w'

        with h5py.File(outfilepath, mode) as f:

            # if no datasets, create one
            # print("KEYS", list(f.keys()))
            if not "/data" in f.keys():
                # print('[debug] No dataset, creating dataset')
                null_arr = np.zeros(O)
                outdset = f.create_dataset("/data", O, data=null_arr, dtype=dtype)  # initialize an empty dataset
            else:
                # print('[debug] Dataset exists')
                outdset = f["/data"]

            outdset[s2[0][0]:s2[0][1],s2[1][0]:s2[1][1],s2[2][0]:s2[2][1]] = data


    def write(self, outfilepath, data, cs, _slices=None, dtype=np.float16): 
        """ Write arr into a file.
        File must not exist
        """
        if os.path.isfile(outfilepath):
            mode = 'r+'
        else:
            mode = 'w'

        with h5py.File(outfilepath, mode) as f:

            if _slices != None:
                if not "/data" in f.keys():
                    null_arr = np.zeros(cs)
                    outdset = f.create_dataset("/data", cs, data=null_arr, dtype=dtype) 
                else:
                    outdset = f["/data"]

                outdset[_slices[0][0]:_slices[0][1],_slices[1][0]:_slices[1][1],_slices[2][0]:_slices[2][1]] = data
            else:
                f.create_dataset("/data", cs, data=data, dtype=dtype)            


    def test_write(self, outfile_path, s, subarr_data):
        with h5py.File(outfile_path, 'r') as f:
            stored = f['/data'][s[0][0]:s[0][1],s[1][0]:s[1][1],s[2][0]:s[2][1]]
            if np.allclose(stored, subarr_data):
                logger.debug("[success] data successfully stored.")
            else:
                logger.debug("[error] in data storage")


    def close_infiles(self):
        clean_files()


    def read_all(self, filepath):
        f = h5py.File(filepath, 'r') 
        if not file_in_list(f, SOURCE_FILES):
            SOURCE_FILES.append(f)
        return f["/data"][()]


    def read(self, input_file):
        return self.get_dataset(input_file, '/data')


    def inspect_h5py_file(self, f):
        print(f'Inspecting h5py file...')
        for k, v in f.items():
            print(f'\tFound object {v.name} at key {k}')
            if isinstance(v, Dataset):
                print(f'\t - Object type: dataset')
                print(f'\t - Physical chunks shape: {v.chunks}')
                print(f'\t - Compression: {v.compression}')
                print(f'\t - Shape: {v.shape}')
                print(f'\t - Size: {v.size}')
                print(f'\t - Dtype: {v.dtype}')
            else:
                print(f'\t - Object type: group')


    def get_dataset(self, file_path, dataset_key):
        """ Get dataset from hdf5 file 
        """

        def check_extension(file_path, ext):
            if file_path.split('.')[-1] != ext:
                return False 
            return True

        if not os.path.isfile(file_path):
            raise FileNotFoundError()

        if not check_extension(file_path, 'hdf5'):
            raise ValueError("This is not a hdf5 file.") 

        f = h5py.File(file_path, 'r')

        if not f.keys():
            raise ValueError('No dataset found in the input file. Aborting.')

        if not file_in_list(f, SOURCE_FILES):
            SOURCE_FILES.append(f)

        print("Loading file...")
        self.inspect_h5py_file(f)

        return f[dataset_key]