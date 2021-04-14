#!/bin/bash
#
#SBATCH --job-name=grouped_chuncks
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=15
#SBATCH --mem-per-cpu=10000
#SBATCH --partition=defq


srun python grouped_data_chunking.py

