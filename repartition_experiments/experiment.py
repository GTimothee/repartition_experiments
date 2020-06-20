from exp_utils import create_input_chunks, create_empty_dir
from .algorithms.baseline_algorithm import baseline_rechunk

def flush_cache():
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches') 


if __name__ == "__main__":
    data_dir = ""
    R, I, O = (1,120,120), (1,60,60), (1,40,40)
    indir_path = os.path.join(data_dir, 'input_dir'), 
    outdir_path = os.path.join(data_dir, 'output_dir'), 
    file_format = 'HDF5'
    
    create_empty_dir(indir_path)
    create_empty_dir(outdir_path)

    create_input_chunks(I, indir_path, file_format)
    baseline_rechunk(indir_path, outdir_path, O, I, R, file_format, True)