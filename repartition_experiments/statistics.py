import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse, glob


def get_arguments():
    """ Get arguments from console command.
    """
    parser = argparse.ArgumentParser(description="")
    
    parser.add_argument('outdir_path',
        action='store',
        type=str,
        help='')

    parser.add_argument('indir_path', 
        action='store',
        type=str,
        help='')

    return parser.parse_args()


def compute_graph_keep(voxels_filepath, memory_filepath, out_filepath):
    vox_data = pd.read_csv(voxels_filepath)
    vox_data = vox_data.apply(lambda x: x*2/1000000, axis=1)

    mem_data = pd.read_csv(memory_filepath)
    mem_data = mem_data.apply(np.round, axis=1)
    start_ram = mem_data.iloc[0][0]
    mem_data['ram'] = mem_data['ram'].apply(lambda x: x - start_ram)

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 10), sharex=True)
    plt.subplot(2,1,1)
    vox_data.plot(title='cache RAM consumption', ax=plt.gca())
    plt.gca().set(xlabel='time', ylabel='RAM used (MB)')
    mem_data.plot(title='virtual memory consumption', ax=axes[1], kind='bar')
    axes[1].set(xlabel='time (5s interval)', ylabel='RAM used (MB)')

    fig.savefig(out_filepath)


def compute_graph_baseline(memory_filepath, out_filepath):
    
    mem_data = pd.read_csv(memory_filepath)
    mem_data = mem_data.apply(np.round, axis=1)
    start_ram = mem_data.iloc[0][0]
    mem_data['ram'] = mem_data['ram'].apply(lambda x: x - start_ram)

    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(10, 5), sharex=True)
    
    mem_data.plot(title='virtual memory consumption', kind='bar')
    plt.gca().set(xlabel='time (5s interval)', ylabel='RAM used (MB)')

    fig.savefig(out_filepath)


if __name__ == "__main__":
    args = get_arguments()

    if not os.path.isdir(args.indir_path) or not os.path.isdir(args.outdir_path):
        raise ValueError()
    
    memory_files = dict()
    voxels_files = dict()
    workdir = os.getcwd()
    os.chdir(args.indir_path)
    for filename in glob.glob("*.csv"):
        if "memorytrace" in filename:
            print("found ", filename)
            data = filename.split('_')
            model = data[5]
            key = (data[2],data[3],data[4],model)
            memory_files[key] = os.path.join(args.indir_path, filename)
        elif "voxelstrace" in filename:
            print("found ", filename)
            data = filename.split('_')
            model = data[5]
            key = (data[2],data[3],data[4],model)
            voxels_files[key] = os.path.join(args.indir_path, filename)
    os.chdir(workdir)

    for key in list(memory_files.keys()):
        memory_filepath = memory_files[key]
        
        if key in voxels_files.keys():
            voxels_filepath = voxels_files[key]
            out_filepath = os.path.join(args.outdir_path, f'graph_{key[0]}_{key[1]}_{key[2]}_{key[3]}.png')
            print("creating ", out_filepath)
            compute_graph_keep(voxels_filepath, memory_filepath, out_filepath)
        else:
            out_filepath = os.path.join(args.outdir_path, f'graph_{key[0]}_{key[1]}_{key[2]}_{key[3]}.png')
            print("creating ", out_filepath)
            compute_graph_baseline(memory_filepath, out_filepath)

    