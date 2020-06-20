def flush_cache():
    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches') 


if __name__ == "__main__":
    data_dir = ""
    R, I, O = (1,120,120), (1,60,60), (1,40,40)
    indir_path, outdir_path, file_format = './input_dir', './output_dir', 'HDF5'
    os.mkdir(indir_path)
    os.mkdir(outdir_path)

    if not os.path.isdir(indir_path) or not os.path.isdir(outdir_path):
        raise OSError()

    create_input_chunks(I, O, indir_path, file_format)
    baseline_rechunk(indir_path, outdir_path, O, I, R, file_format, True)