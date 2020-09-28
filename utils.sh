#!/bin/bash
#
#SBATCH --job-name=cons_utils_test
#SBATCH --output=cons_utils_%j.out  # output file
#SBATCH -e res_%j.err        # File to which STDERR will be written
#SBATCH --partition=longq    # Partition to submit to 
#
#SBATCH --cores=4
#SBATCH --mem-per-cpu=5000

srun python utils.py

