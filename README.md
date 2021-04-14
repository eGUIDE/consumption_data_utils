# About
This repo holds utility functions for analyzing electricity consumption data.

# Requirements
* Access to [SWARM2](https://people.cs.umass.edu/~swarm/index.php?n=Main.NewSwarmDoc)
* python3
# work flow
## Creating Meta and transactions files
* We start by creating separate "meta" and "transactions" files. The meta file consists of unique customer number, date on which they recieved an electricity connection as well as their vending category ie residential or non residential. The transactions file consists of all prepaid transactions and the dates on which they occur. We generate the two files by running the terminal command
   *  __`$ sbatch generate_meta_and_transactions.sh`__
## Creating smoothed transactions
* After generating the meta and transactions files, we smooth out electricity consumption for each customer so as to get a cleaner aggregation for monthly consuption. To do this, we need to change directory and move to the `smoothing_process_files` directory. There, we can enter the directory paths we need to read from or save to. We then run commands:
    *  __`$ sbatch grouped_data_chunks.sh`__ : This splits the data into chuncks that will be run in parallel. it will take a few minutes to run.
    *  __`$ sbatch create_shell_batch_smoothing.sh`__ : This command does the actual smoothing of data takes a few hours to run.
    *  __`$ sbatch merge_pickles.sh`__ :   This merges all the data chunks that were run in parallel to generate a single file of smoothed transactions on which we run our analysis.
## Analysis
