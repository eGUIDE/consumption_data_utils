#!/bin/bash
#
#SBATCH --job-name=smoothing_reg
#SBATCH --output=smoothing_%j.out  # output file
#SBATCH -e res_%j.err        # File to which STDERR will be written
#SBATCH --partition=m40-short    # Partition to submit to 
#
#SBATCH --mem=5000

srun python batch_smoothing.py $1

