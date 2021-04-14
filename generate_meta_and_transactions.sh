#!/bin/bash
#
#SBATCH --job-name=REG_data
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=12
#SBATCH --mem-per-cpu=10000
#SBATCH --partition=defq
#SBATCH --time=08:00:00

python generate_meta_and_transactions.py


