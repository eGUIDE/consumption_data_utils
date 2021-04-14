# About
This repo holds utility functions for analyzing electricity consumption data.

# Requirements
* Access to [SWARM2](https://people.cs.umass.edu/~swarm/index.php?n=Main.NewSwarmDoc)
* python3
# work flow
* We start by creating separate "meta" and "transactions" files. The meta file consists of unique customer number, date on which they recieved an electricity connection as well as their vending category ie residential or non residential. The transactions file consists of all prepaid transactions and the dates on which they occur. We generate the two files by running the terminal command
   `$ sbatch generate_meta_and_transactions.sh`
