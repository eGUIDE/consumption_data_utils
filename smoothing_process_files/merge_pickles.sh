#!/bin/bash
#
#SBATCH --job-name=final_smoothed
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=15
#SBATCH --mem-per-cpu=10000
#SBATCH --partition=defq

python merge_pickles.py


