#!/bin/bash
#
#SBATCH --job-name=data_chunks_reg
#SBATCH --output=data_chunks_reg_%j.out  # output file
#SBATCH -e res_%j.err        # File to which STDERR will be written
#SBATCH --partition=m40-short    # Partition to submit to 
#
#SBATCH --mem=100000

srun python grouped_data_chunking.py

