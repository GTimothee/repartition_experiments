import os, csv, sys

from ..experiment import experiment, write_results, load_json


class Args:
    def __init__(self, case_name, model):
        test_dir = os.path.dirname(os.path.abspath(__file__))
        self.paths_config = os.path.join(test_dir, 'paths_config.json')
        self.cases_config = os.path.join(test_dir, 'cases_config.json')
        self.file_format = 'HDF5'
        self.overwrite = True

        self.model = model
        self.case_name = case_name


def test_experiment_keep():
    args = Args("case 1_1", "keep")
    paths = load_json(args.paths_config)
    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)
    from repartition_experiments.exp_utils import create_empty_dir, verify_results
    from repartition_experiments.algorithms.baseline_algorithm import baseline_rechunk
    from repartition_experiments.algorithms.keep_algorithm import keep_algorithm, get_input_aggregate
    from repartition_experiments.algorithms.clustered_writes import clustered_writes
    from repartition_experiments.algorithms.utils import get_file_manager

    results = experiment(Args("case 1_1", "keep"))
    for r in results:
        success = r[-1]
        assert success

    csv_path = write_results(results, Args("case 1", "keep"))
    nb_lines = 0
    with open(csv_path, "r") as f:
        reader = csv.reader(f, delimiter=",")
        for i, line in enumerate(reader):
            nb_lines += 1
    assert nb_lines == (len(results) + 1)

    results = experiment(Args("case 2", "keep"))
    for r in results:
        success = r[-1]
        assert success

    csv_path = write_results(results, Args("case 2", "keep"))
    nb_lines = 0
    with open(csv_path, "r") as f:
        reader = csv.reader(f, delimiter=",")
        for i, line in enumerate(reader):
            nb_lines += 1
    assert nb_lines == (len(results) + 1)


def test_experiment_baseline():
    args = Args("case 1_1", "baseline")
    paths = load_json(args.paths_config)
    for k, v in paths.items():
        if "PYTHONPATH" in k:
            sys.path.insert(0, v)

    results = experiment(Args("case 1_1", "baseline"))
    for r in results:
        success = r[-1]
        assert success

    results = experiment(Args("case 2", "baseline"))
    for r in results:
        success = r[-1]
        assert success