import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.lines import Line2D

import sys, argparse, json, os
import numpy as np

from copy import copy

plt.style.use('classic')


def get_chunks(cs, c, R, linewidth):
    """ Get the list of all rectangles representing the chunks.
    Used to print each chunk on the Figure. 

    Arguments: 
    ----------
        cs: chunk shape
        c: color
    """
    x_range = np.arange(start=0, stop=R[0], step=cs[0])
    y_range = np.arange(start=0, stop=R[1], step=cs[1])
    points = np.dstack(np.meshgrid(x_range, y_range)).reshape(-1, 2)
    points = list(map(lambda pt: Point(pt[0], pt[1]), points))
    
    rectangles = list()
    for point in points:
        rect = CustomRectangle(point, width=cs[0], height=cs[1], color=c, fill=False, linewidth=linewidth)
        rectangles.append(rect)

    return rectangles


def print_rectangles(rectangles):
    """ Create a figure and print each rectangle from "rectangles" argument.
    """
    for rect in rectangles:
        copyRect = copy(rect)
        copyRect.setFig(fig)
        ax.add_patch(copyRect)


def get_cuts_lists(R, O, I):
    block_cuts = get_cuts(R, O)
    buffer_cuts = get_cuts(R, I)
    block_shape = O

    nocostly = [list(), list(), list()]
    costly = [list(), list(), list()]
    match = [list(), list(), list()]
    dim_index = 0
    for o, i in zip(block_cuts, buffer_cuts):
        d_dim = 0
        
        o = list(map(lambda x: (0, x), o))
        i = list(map(lambda x: (1, x), i))
        values = o+i
        values.sort(key=lambda x:x[1])

        j = 0
        last_infile_cut_index = -1
        while j < len(values): 
            
            cond1 = (values[j][1] == values[j+1][1])
            lower_i = i[last_infile_cut_index][1] if last_infile_cut_index > -1 else 0
            cond2 = (values[j][0] == 0 and values[j][1] - block_shape[dim_index] >= lower_i)

            if cond1:
                match[dim_index].append(values[j][1])
            elif cond2:
                nocostly[dim_index].append(values[j][1])
            else:
                costly[dim_index].append(values[j][1])

            if values[j][0] == 1 or cond1:
                last_infile_cut_index += 1

            if cond1:
                j += 2
            else:
                j += 1  

        dim_index += 1
    return costly, nocostly, match


def get_arguments():
    """ Get arguments from console command.
    """
    parser = argparse.ArgumentParser(description="")
    
    parser.add_argument('paths_config', 
        action='store', 
        type=str, 
        help='Path to configuration file containing paths of data directories.')

    # parser.add_argument('R', 
    #     action='store', 
    #     type=tuple, 
    #     help='')

    # parser.add_argument('O', 
    #     action='store', 
    #     type=tuple, 
    #     help='')

    # parser.add_argument('I', 
    #     action='store', 
    #     type=tuple, 
    #     help='')

    return parser.parse_args()


def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)


if __name__ == "__main__":  

    args = get_arguments()
    paths = load_json(args.paths_config)
    
    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    from repartition_experiments.scripts_paper.drawer.point import Point
    from repartition_experiments.scripts_paper.drawer.customRectangle import CustomRectangle
    from repartition_experiments.scripts_paper.baseline_seek_model import get_cuts

    R = (120, 120, 120)
    I = (30, 30, 30)
    O = (20, 20, 20)

    # create figure
    fig = plt.figure()
    ax = fig.add_subplot(111, aspect='equal')
    ax.set(xlim=(0, R[0]), ylim=(0, R[1]))
    ax.set_xlabel('X_1')
    ax.set_ylabel('X_2')

    # print
    costly, nocostly, match = get_cuts_lists(R, O, I)
    print("Costly: ", costly[0])
    print("Not costly: ", nocostly[0])
    
    color_input = "tab:blue"
    color_output = "tab:gray"
    color_cuts = "tab:red"
    color_match = "tab:green"
    color_nocostly = 'tab:orange'

    sample_inblock = None
    dim = 1
    x = 0
    while x != R[dim]:
        x += I[dim]
        line = Line2D([0,R[dim]], [x,x], color=color_input, linewidth=10)
        ax.add_line(line)
        if sample_inblock == None:
            sample_inblock = line
    dim = 2
    x = 0
    while x != R[dim]:
        x += I[dim]
        line = Line2D([x,x], [0,R[dim]], color=color_input, linewidth=10)
        ax.add_line(line)

    sample_outblock = None
    dim = 1
    x = 0
    while x != R[dim]:
        x += O[dim]
        line = Line2D([0,R[dim]], [x,x], color=color_output, linewidth=8)
        ax.add_line(line)
        if sample_outblock == None:
            sample_outblock = line
    dim = 2
    x = 0
    while x != R[dim]:
        x += O[dim]
        line = Line2D([x,x], [0,R[dim]], color=color_output, linewidth=8)
        ax.add_line(line)

    sample_cut = None
    for dim in range(3): 
        for c in costly[0]:
            line = Line2D([c,c], [0,R[0]], color=color_cuts, linewidth=1)
            ax.add_line(line)
            if sample_cut == None:
                sample_cut = line
        for c in costly[1]:
            line = Line2D([0,R[0]], [c,c], color=color_cuts, linewidth=1)
            ax.add_line(line)

    sample_match = None
    for dim in range(3): 
        for c in match[0]:
            line = Line2D([c,c], [0,R[0]], color=color_match, linewidth=1)
            ax.add_line(line)
            if sample_match == None:
                sample_match = line
        for c in match[1]:
            line = Line2D([0,R[0]], [c,c], color=color_match, linewidth=1)
            ax.add_line(line)


    sample_nocostly = None
    for dim in range(3): 
        for c in nocostly[0]:
            line = Line2D([c,c], [0,R[0]], color=color_nocostly, linewidth=1)
            ax.add_line(line)
            if sample_nocostly == None:
                sample_nocostly = line
        for c in nocostly[1]:
            line = Line2D([0,R[0]], [c,c], color=color_nocostly, linewidth=1)
            ax.add_line(line)

    plt.legend([sample_inblock, sample_outblock, sample_cut, sample_match, sample_nocostly], ('input block cut', 'output block cut', 'cut', 'shape match', 'nocostly cut'))
    plt.show()