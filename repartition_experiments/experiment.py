def flush_cache():
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches') 


if __name__ == "__main__":
    data_dir = ""
    R, I, O = (1,120,120), (1,60,60), (1,40,40)
    indir_path = os.path.join(data_dir, 'input_dir'), 
    outdir_path = os.path.join(data_dir, 'output_dir'), 
    file_format = 'HDF5'
    
    if not os.path.isdir(indir_path):
        os.mkdir(indir_path)
    if not os.path.isdir(outdir_path):
        os.mkdir(outdir_path)
    if not os.path.isdir(indir_path) or not os.path.isdir(outdir_path):
        raise OSError()

    create_input_chunks(I, indir_path, file_format)
    baseline_rechunk(indir_path, outdir_path, O, I, R, file_format, True)