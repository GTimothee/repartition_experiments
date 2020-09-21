# Repartitioning large multi-dimensional arrays: a sequential algorithm
This repository contains all the code used for the experiments presented in our paper. 

## Directories
- algorithms: Python files of the algorithms used. 
- cases_config: JSON files describing the different experiments and paths to the required packages.
- file_formats: Objects for managing different file formats. 
- scripts_exp: Scripts to run in order to run the experiment.
- scripts_paper: Scripts used specifically for the paper, like one to compute graphs from the results.
- tests: Tests using pytest.

## Third-party libraries
Use requirements.txt or requirements_conda.txt to install the dependencies.

For a conda environment:
```
conda create --name <env> --file requirements_conda.txt
```
For a pip environment: 
```
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

### If there are missing dependencies when creating the conda environment
- 1) Create a new conda environement ``` conda create --name <envname> ```
- 2) activate the environment ``` conda activate <envname> ```
- 3) 'cd' inside the dask_io directory (where the requirements_conda.txt file is)
- 4) install the dependencies that are not missing ``` while read requirement; do conda install --yes $requirement; done < requirements_conda.txt ```

### Note for developers
To create the requirements_conda file:
```
conda list -e > requirements_conda.txt
``` 
To create the ``requirements.txt" file from conda environment:
```
pip freeze > requirements.txt
``` 
Remove mkl dependencies from ``requirements.txt" to get rid of compatibility issues.

## Custom packages
One custom package called Monitor (https://github.com/GTimothee/monitor.git) has been used to track the memory usage of the experiment using a different process.

## File formats supported
For now the only file format supported is the HDF5 file format using the h5py package.
More file formats can be added by implementing a new object in the file_formats directory and implementing the same methods. 

## Running the experiment
First, create two JSON files: 
- a case file containing information about the cases to run
    - When the case name begins with "1_", then the algorithm uses the input aggregate shape by default
    - in the other cases, always mention the buffer shape
    - in order to find the best buffer shape to use for a case, run the "buffer_selector.py" script from scripts_exp

- a paths file containing the absolute paths to 
    - a SSD to write the input blocks
    - an output directory to write the output blocks
    - this repository
    - the Monitor repository

Examples of such files can be found in the cases_config directory.

Then, 
1) run the create_case.py algorithm to create the input blocks. By default it writes an entire image, then breaks it into input blocks using the "clustered strategy", but you can write only the input blocks by using the -s option.
2) run the experiment.py script.

To get the graphs from the experiment, create a directory named "graphs" into your results directory and run the "statistics.py" script. 