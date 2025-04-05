#!/bin/bash
# This script is used to run one of the caom2 usage python scripts.

working_dir=$HOME/work/caom2usage
script_dir=$HOME/work/git/cadc-tools

# check if the caom2usage directory exists
if [ ! -d "$working_dir" ]; then
    echo "Directory $working_dir does not exist!"
    exit 1
fi

# check if the git/cadc-tools directory exists
if [ ! -d "$script_dir" ]; then
    echo "Directory $script_dir does not exist!"
    exit 1
fi

# Check if the script name is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <script_name> [script_args]"
    echo "Example: $0 usageGenCollInstr.py [script_args]"
    echo "Example: $0 usageGenCollection.py [script_args]"
    echo "Example: $0 usagePrep.py"
    echo "Example: $0 usageSumCollInstr.py"
    echo "Example: $0 usageSumCollection.py"
    exit 1
fi 

# Check if the script exists
if [ ! -f "$script_dir/$1" ]; then
    echo "Script $1 not found!"
    exit 1
fi     

cd $working_dir
python3 $script_dir/$1 $2 $3 $4 $5 $6 $7 $8 $9
# Check if the script ran successfully
if [ $? -ne 0 ]; then
    echo "Script $1 failed!"
    exit 1
fi  