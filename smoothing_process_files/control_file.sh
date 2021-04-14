#!/bin/bash



sbatch grouped_data_chunks.sh
sbatch create_shell_batch_smoothing.sh
bash batch_process_smoothing.sh
sbatch merge_pickles.sh
