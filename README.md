# Repartitioning large multi-dimensional arrays: a sequential algorithm
This repository contains all the code used for the experiments presented in our paper. 

## Directories
- algorithms 
- cases_config
- file_formats
- scripts_exp: scripts to run in order to run the experiment
- scripts_paper: scripts used specifically for the paper, like one to compute graphs from the results
- tests: tests using pytest

## File formats supported
For now the only file format supported is the HDF5 file format using the h5py package.
More file formats can be added by implementing a new object in the file_formats directory and implementing the same methods. 