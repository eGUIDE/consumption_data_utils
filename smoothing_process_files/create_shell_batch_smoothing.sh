#!/bin/bash
#
#SBATCH --job-name=create_shell_batch_smoothing
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1000
#SBATCH --partition=defq

python create_shell_batch_smoothing.py


