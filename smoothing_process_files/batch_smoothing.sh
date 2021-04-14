#!/bin/bash
#
#SBATCH --job-name=batch_smoothing
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=12
#SBATCH --mem-per-cpu=10000
#SBATCH --time=08:00:00
#SBATCH --partition=defq


srun python batch_smoothing.py $1

