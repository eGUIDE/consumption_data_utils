#!/bin/bash
#
#SBATCH --job-name=trends_reg
#SBATCH --output=trends_%j.out  # output file
#SBATCH -e res_%j.err        # File to which STDERR will be written
#SBATCH --partition=m40-short    # Partition to submit to 
#
#SBATCH --mem=100000

srun python consumption_trends.py

