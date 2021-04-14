#!/bin/bash
#
#SBATCH --job-name=Kenya_A1
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=bmuhwezi@umass.edu
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=15
#SBATCH --mem-per-cpu=10000
#SBATCH --partition=defq

python merge_pickles.py 'smoothed_monthly_data' './REG_smoothed_data_Feb_28_2021.pck'


