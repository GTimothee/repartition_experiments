import os, csv, math
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse, glob

""" Script to get graphs from the output files. 

    Example usage:
    --------------
        python repartition_experiments/statistics.py /home/user/Desktop/results_cluster/run_3/case_1/graphs /home/user/Desktop/results_cluster/run_3/case_1/ "Experiment 1 - Keep algorithm efficacity in the ideal case (B=Lambda)" "Experiment 1 - Seeks (log scale)"
"""

def get_arguments():
    """ Get arguments from console command.
    """
    parser = argparse.ArgumentParser(description="")
    
    parser.add_argument('outdir_path',
        action='store',
        type=str,
        help='Output directory where the graphs will be stored.')

    parser.add_argument('indir_path', 
        action='store',
        type=str,
        help='Input directory containing the results files.')

    parser.add_argument('title_main', 
        action='store',
        type=str,
        help='Title of the graph showing the results in terms of processing time.')

    parser.add_argument('title_seeks', 
        action='store',
        type=str,
        help='Title of the graph showing the results in terms of seeks.')

    return parser.parse_args()


def compute_graph_results(results_path, outdir_path, title_main, title_seeks):
    df = pd.read_csv(results_path)
    df_seeks = pd.read_csv(results_path)

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

    # COMPUTE GRAPH PROCESSING TIME
    
    x = np.arange(len(references))  # the label locations
    width = 0.20  # the width of the bars

    baseline_prepross_bottom = baseline_means["write_time"] + baseline_means['read_time']
    baseline_overhead_bottom = baseline_prepross_bottom + baseline_means['preprocess_time']
    keep_prepross_bottom = keep_means["write_time"] + keep_means['read_time']
    keep_overhead_bottom = keep_prepross_bottom + keep_means['preprocess_time']

    # graph
    width_step = [width /2, width /2]

    fig, ax = plt.subplots(figsize=(10, 5))
    _ = ax.bar(x - width_step[0], baseline_means["read_time"], width, yerr=baseline_stds['read_time'], label='read time (baseline)', color=['tab:blue'])
    _ = ax.bar(x - width_step[0], baseline_means["write_time"], width, bottom=baseline_means["read_time"], yerr=baseline_stds['write_time'], label='write time (baseline)', color=['tab:green'])
    _ = ax.bar(x - width_step[0], baseline_means["preprocess_time"], width, bottom=baseline_prepross_bottom, yerr=baseline_stds['preprocess_time'], label='preprocessing time (baseline)', color=['tab:orange'])
    _ = ax.bar(x - width_step[0], baseline_means["overhead"], width, bottom=baseline_overhead_bottom, yerr=baseline_stds['overhead'], label='overhead time (baseline)', color=['tab:red'])

    _ = ax.bar(x + width_step[1], keep_means["read_time"], width, yerr=keep_stds['read_time'], label='read time (keep)', color=['tab:blue'], hatch='//')
    _ = ax.bar(x + width_step[1], keep_means["write_time"], width, bottom=keep_means['read_time'], yerr=keep_stds['write_time'], label='write time (keep)', color=['tab:green'], hatch='//')
    _ = ax.bar(x + width_step[1], keep_means["preprocess_time"], width, bottom=keep_prepross_bottom, yerr=keep_stds['preprocess_time'], label='preprocessing time (keep)', color=['tab:orange'], hatch='//')
    _ = ax.bar(x + width_step[1], keep_means["overhead"], width, bottom=keep_overhead_bottom, yerr=keep_stds['overhead'], label='overhead time (keep)', color=['tab:red'], hatch='//')
            
    ax.set_ylabel('Processing time (s)')
    ax.set_xlabel('run reference')
    ax.set_title(title_main)
    ax.set_xticks(x)
    ax.set_xticklabels(sorted(references))
    # ax.set_ylim(top=1600) -> for 3500 img results
    ax.legend()

    fig.tight_layout()
    fig.savefig(os.path.join(outdir_path, 'results.pdf'))

    # COMPUTE GRAPH SEEKS
    
    df_seeks = df_seeks.drop(columns=['Unnamed: 0', 'case_name', 'process_time', 'preprocess_time', 'read_time', 'write_time', 'max_voxels', 'success'])
    df_keep = df_seeks.loc[df_seeks["model"]=="keep"]
    df_baseline = df_seeks.loc[df_seeks["model"]=="baseline"]

    keep_means = df_keep.groupby('run_ref').mean()
    keep_stds = df_keep.groupby('run_ref').std()
    baseline_means = df_baseline.groupby('run_ref').mean()
    baseline_stds = df_baseline.groupby('run_ref').std()

    outseeks_bottom_k = keep_means['outfile_openings']
    inops_bottom_k = keep_means['outfile_openings'] + keep_means['outfile_seeks']
    inseeks_bottom_k = keep_means['outfile_openings'] + keep_means['outfile_seeks'] + keep_means['infile_openings']

    outseeks_bottom_b = baseline_means['outfile_openings']
    inops_bottom_b = baseline_means['outfile_openings'] + baseline_means['outfile_seeks']
    inseeks_bottom_b = baseline_means['outfile_openings'] + baseline_means['outfile_seeks'] + baseline_means['infile_openings']

    fig, ax = plt.subplots(figsize=(10, 5))
    _ = ax.bar(x - width/2, baseline_means['outfile_openings'], width, yerr=baseline_stds['outfile_openings'], label='outfile_openings (baseline)', color=['tab:blue'])
    _ = ax.bar(x - width/2, baseline_means['outfile_seeks'], width, bottom=outseeks_bottom_b, yerr=baseline_stds['outfile_seeks'], label='outfile_seeks (baseline)', color=['tab:green'])
    _ = ax.bar(x - width/2, baseline_means['infile_openings'], width, bottom=inops_bottom_b, yerr=baseline_stds['infile_openings'], label='infile_openings (baseline)', color=['tab:red'])
    _ = ax.bar(x - width/2, baseline_means['infile_seeks'], width, bottom=inseeks_bottom_b, yerr=baseline_stds['infile_seeks'], label='infile_seeks (baseline)', color=['tab:orange'])

    _ = ax.bar(x + width/2, keep_means['outfile_openings'], width, yerr=keep_stds['outfile_openings'], label='outfile_openings (keep)', color=['tab:blue'], hatch='//')
    _ = ax.bar(x + width/2, keep_means['outfile_seeks'], width, bottom=outseeks_bottom_k, yerr=keep_stds['outfile_seeks'], label='outfile_seeks (keep)', color=['tab:green'], hatch='//')
    _ = ax.bar(x + width/2, keep_means['infile_openings'], width, bottom=inops_bottom_k, yerr=keep_stds['infile_openings'], label='infile_openings (keep)', color=['tab:red'], hatch='//')
    _ = ax.bar(x + width/2, keep_means['infile_seeks'], width, bottom=inseeks_bottom_k, yerr=keep_stds['infile_seeks'], label='infile_seeks (keep)', color=['tab:orange'], hatch='//')

    plt.yscale('log')

    ax.set_ylabel('number of seeks')
    ax.set_xlabel('run reference')
    ax.set_title(title_seeks)
    ax.set_xticks(x)
    ax.set_xticklabels(sorted(references))
    ax.set_ylim(top=1000000000)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    fig.tight_layout()
    fig.savefig(os.path.join(outdir_path, 'results_seeks.pdf'))


if __name__ == "__main__":
    args = get_arguments()

    if not os.path.isdir(args.indir_path) or not os.path.isdir(args.outdir_path):
        raise ValueError()

    # Find results files
    results = list()
    workdir = os.getcwd()
    os.chdir(args.indir_path)
    for filename in glob.glob("*.csv"):
        if "results" in filename:
            results.append(os.path.join(args.indir_path, filename))
    os.chdir(workdir)

    # load files' content
    rows = None
    for filepath in results:
        df = pd.read_csv(filepath)
        if not isinstance(rows, pd.DataFrame):
            rows = df
        else:
            rows = rows.append(df)

    outfilepath = os.path.join(args.outdir_path, 'results.csv')
    rows.to_csv(outfilepath)

    # compute graphs
    compute_graph_results(outfilepath, args.outdir_path, args.title_main, args.title_seeks)