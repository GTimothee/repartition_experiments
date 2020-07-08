import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse


def get_arguments():
    """ Get arguments from console command.
    """
    parser = argparse.ArgumentParser(description="")
    
    parser.add_argument('voxels_filepath', 
        action='store', 
        type=str, 
        help='')

    parser.add_argument('memory_filepath',
        action='store',
        type=str,
        help='')

    parser.add_argument('out_filepath',
        action='store',
        type=str,
        help='')

    return parser.parse_args()


if __name__ == "__main__":
    args = get_arguments()

    vox_data = pd.read_csv(args.voxels_filepath)
    vox_data = vox_data.apply(lambda x: x*2/1000000, axis=1)

    mem_data = pd.read_csv(args.memory_filepath)
    mem_data = mem_data.apply(np.round, axis=1)
    start_ram = mem_data.iloc[0][0]
    mem_data['ram'] = mem_data['ram'].apply(lambda x: x - start_ram)

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 10), sharex=True)
    plt.subplot(2,1,1)
    vox_data.plot(title='cache RAM consumption', ax=plt.gca())
    plt.gca().set(xlabel='time', ylabel='RAM used (MB)')
    mem_data.plot(title='virtual memory consumption', ax=axes[1], kind='bar')
    axes[1].set(xlabel='time (5s interval)', ylabel='RAM used (MB)')

    fig.savefig(args.out_filepath)