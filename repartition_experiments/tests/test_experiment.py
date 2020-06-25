import os, csv

from ..experiment import experiment, write_results


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
    results = experiment(Args("case 1_1", "baseline"))
    for r in results:
        success = r[-1]
        assert success

    results = experiment(Args("case 2", "baseline"))
    for r in results:
        success = r[-1]
        assert success