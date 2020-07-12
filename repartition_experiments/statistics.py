import os, csv, math
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

    parser.add_argument('title_main', 
        action='store',
        type=str,
        help='')

    parser.add_argument('title_seeks', 
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

    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True)
    plt.subplot(2,1,1)
    vox_data.plot(title='cache RAM consumption', ax=plt.gca())
    plt.gca().set(xlabel='time', ylabel='RAM used (MB)')
    # plt.gca().set_xticks(plt.gca().get_xticks()[::2])
    # plt.gca().set_xticklabels(plt.gca().get_xticks())

    mem_data.plot(title='virtual memory consumption', ax=axes[1], kind='bar')
    axes[1].set(xlabel='time (5s interval)', ylabel='RAM used (MB)')
    interval = math.floor(len(mem_data.index)/10)
    axes[1].set_xticks(axes[1].get_xticks()[::interval])
    axes[1].set_xticklabels(axes[1].get_xticks(), rotation=0)

    fig.tight_layout()
    fig.savefig(out_filepath)


def compute_graph_baseline(memory_filepath, out_filepath):
    
    mem_data = pd.read_csv(memory_filepath)
    mem_data = mem_data.apply(np.round, axis=1)
    start_ram = mem_data.iloc[0][0]
    mem_data['ram'] = mem_data['ram'].apply(lambda x: x - start_ram)

    fig, axes = plt.subplots(nrows=1, ncols=1, sharex=True)
    plt.subplot(1,1,1)
    mem_data.plot(title='virtual memory consumption', ax=plt.gca(), kind='bar')
    plt.gca().set(xlabel='time (5s interval)', ylabel='RAM used (MB)')
    interval = math.floor(len(mem_data.index)/10)
    plt.gca().set_xticks(plt.gca().get_xticks()[::interval])
    plt.gca().set_xticklabels(plt.gca().get_xticks(), rotation=0)

    fig.tight_layout()
    fig.savefig(out_filepath)


def compute_graph_results(results_path, outdir_path, title_main, title_seeks):
    df = pd.read_csv(results_path)

    # preprocessing
    df = df.drop(columns=['max_voxels', 'success', 'case_name', 'Unnamed: 0'])
    df['nb_seeks'] = df['outfile_openings'] + df['outfile_seeks'] + df['infile_openings'] + df['infile_seeks']
    df = df.drop(columns=['outfile_openings', 'outfile_seeks', 'infile_openings', 'infile_seeks'])
    df['overhead'] = df['process_time'] - df['preprocess_time'] - df['read_time'] - df['write_time']

    references = df["run_ref"].unique()
    models = df["model"].unique()

    # df splits
    df = df.sort_values(["model", "run_ref"])

    df_keep = df.loc[df["model"]=="keep"]
    keep_means = df_keep.groupby('run_ref').mean()
    keep_stds = df_keep.groupby('run_ref').std()

    df_baseline = df.loc[df["model"]=="baseline"]
    baseline_means = df_baseline.groupby('run_ref').mean()
    baseline_stds = df_baseline.groupby('run_ref').std()

    # data for graph
    x = np.arange(len(references))  # the label locations
    width = 0.20  # the width of the bars

    baseline_prepross_bottom = baseline_means["write_time"] + baseline_means['read_time']
    baseline_overhead_bottom = baseline_prepross_bottom + baseline_means['preprocess_time']
    keep_prepross_bottom = keep_means["write_time"] + keep_means['read_time']
    keep_overhead_bottom = keep_prepross_bottom + keep_means['preprocess_time']

    # graph
    fig, ax = plt.subplots(figsize=(10, 5))
    _ = ax.bar(x - width/2, baseline_means["read_time"], width, yerr=baseline_stds['read_time'], label='read time (baseline)', color=['tab:blue'])
    _ = ax.bar(x - width/2, baseline_means["write_time"], width, bottom=baseline_means["read_time"], yerr=baseline_stds['write_time'], label='write time (baseline)', color=['tab:green'])
    _ = ax.bar(x - width/2, baseline_means["preprocess_time"], width, bottom=baseline_prepross_bottom, yerr=baseline_stds['preprocess_time'], label='preprocessing time (baseline)', color=['tab:orange'])
    _ = ax.bar(x - width/2, baseline_means["overhead"], width, bottom=baseline_overhead_bottom, yerr=baseline_stds['overhead'], label='overhead time (baseline)', color=['tab:red'])

    _ = ax.bar(x + width/2, keep_means["read_time"], width, yerr=keep_stds['read_time'], label='read time (keep)', color=['tab:blue'], hatch='//')
    _ = ax.bar(x + width/2, keep_means["write_time"], width, bottom=keep_means['read_time'], yerr=keep_stds['write_time'], label='write time (keep)', color=['tab:green'], hatch='//')
    _ = ax.bar(x + width/2, keep_means["preprocess_time"], width, bottom=keep_prepross_bottom, yerr=keep_stds['preprocess_time'], label='preprocessing time (keep)', color=['tab:orange'], hatch='//')
    _ = ax.bar(x + width/2, keep_means["overhead"], width, bottom=keep_overhead_bottom, yerr=keep_stds['overhead'], label='overhead time (keep)', color=['tab:red'], hatch='//')
            
    ax.set_ylabel('Processing time (s)')
    ax.set_xlabel('run reference')
    ax.set_title(title_main)
    ax.set_xticks(x)
    ax.set_xticklabels(sorted(references))
    ax.legend()

    fig.tight_layout()
    fig.savefig(os.path.join(outdir_path, 'results.png'))

    # preprocess seeks
    baseline_seeks = df_baseline.groupby(["run_ref"]).mean()['nb_seeks']
    keep_seeks =  df_keep.groupby(["run_ref"]).mean()['nb_seeks']

    # graph seeks
    fig, ax = plt.subplots(figsize=(10, 5))
    _ = ax.bar(x - width/2, baseline_seeks, width, label='baseline', color=['tab:blue'])
    _ = ax.bar(x + width/2, keep_seeks, width, label='keep', color=['tab:orange'], hatch='//')
    plt.yscale('log')

    ax.set_ylabel('number of seeks')
    ax.set_xlabel('run reference')
    ax.set_title(title_seeks)
    ax.set_xticks(x)
    ax.set_xticklabels(sorted(references))
    ax.legend()

    fig.tight_layout()
    fig.savefig(os.path.join(outdir_path, 'results_seeks.png'))


if __name__ == "__main__":
    """ example command:
    
    python repartition_experiments/statistics.py /home/user/Desktop/results_cluster/run_3/case_1/graphs /home/user/Desktop/results_cluster/run_3/case_1/ "Experiment 1 - Keep algorithm efficacity in the ideal case (B=Lambda)" "Experiment 1 - Seeks (log scale)"
    """
    args = get_arguments()

    if not os.path.isdir(args.indir_path) or not os.path.isdir(args.outdir_path):
        raise ValueError()
    
    memory_files = dict()
    voxels_files = dict()
    results = list()
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
        elif "results" in filename:
            results.append(os.path.join(args.indir_path, filename))

    os.chdir(workdir)

    # graphs generation
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

    # results
    header = None
    rows = None
    for filepath in results:
        df = pd.read_csv(filepath)
        if not isinstance(rows, pd.DataFrame):
            rows = df
        else:
            rows = rows.append(df)

    outfilepath = os.path.join(args.outdir_path, 'results.csv')
    rows.to_csv(outfilepath)

    compute_graph_results(outfilepath, args.outdir_path, args.title_main, args.title_seeks)