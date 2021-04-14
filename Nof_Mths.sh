#!/bin/bash
#
#SBATCH --job-name=Kenya_A1
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=bmuhwezi@umass.edu
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=15
#SBATCH --mem-per-cpu=10000
#SBATCH --partition=defq

python Nof_Mths_after_connection_trends.py


