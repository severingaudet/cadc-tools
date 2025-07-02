from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import sys
import os

## Usage message for the script.
def print_usage():
    print(f"Usage: {sys.argv[0]} <directory_name> e.g. siCopies-2025-06-12T16-15-16")
    print(f"       {sys.argv[0]} <-h || -help> for help")

## Main function to execute the script.
## It initializes the data structures for pre-generated namespaces and sites.
## It then loops through the list of namespaces and and merges the values to a single file.
## The script will exit with a status code of 0 if successful, or 1 if an error occurs.

if __name__ == "__main__":

    ## Determine where the siMonitoring directory is located and change to that directory.
    if os.path.isdir("/Users/gaudet_1/work/siMonitoring"):
        os.chdir("/Users/gaudet_1/work/siMonitoring")
    elif os.path.isdir("/arc/projects/CADC/siMonitoring"):
        os.chdir("/arc/projects/CADC/siMonitoring")
    else:
        print("Unable to determine the location of the siMonitoring directory.")
        exit(1)

    ## Check the first argument to determine the mode of operation.
    if len(sys.argv) != 2:
        print_usage()
        exit(1)

    if sys.argv[1] in ['--help', '-h']:
        print_usage()
        exit(0)

    
    ## Read static configuration files for namespaces and sites.
    namespace_filename = "config/siNamespaces.csv"
    sites_filename = "config/siSites.csv"
    try:
        namespaces = pd.read_csv(namespace_filename)
        sites = pd.read_csv(sites_filename)
    except FileNotFoundError as e:
        print(f"Error reading configuration files: {e}")
        exit(1)

    ## Change to the output directory provided as an argument.
    directory_name = sys.argv[1]
    if os.path.isdir(directory_name):
        os.chdir(directory_name)
    else:
        print(f"Unable to change to directory {directory_name}")
        exit(1)

   ## Create the output file for the merged results.
    output_filename = f"{directory_name}_merged.csv"
    try:
        with open(output_filename, 'w') as f:

            ## Loop through the sites to create csv header.
            f.write(f"datestamp,namespace")
            for site in sites['site_name']:
                f.write(f",{site}_count,{site}_duration")
            f.write("\n")

            ## Loop through the namespaces and sites to extract values and write to merged file.
            for namespace in namespaces['namespace']:
                print(f"Merging namespace: {namespace}")
                f.write(f"{namespace}")
                first_site = True
                for site in sites['site_name']:
                    filename = f"{directory_name}_{namespace}_{site}.csv"
                    if os.path.exists(filename):
                        df = pd.read_csv(filename)
                        if first_site:
                            datestamp = df['datestamp'].iloc[0] if 'datestamp' in df.columns else ''
                            f.write(f",{datestamp}")
                            first_site = False
                        count = df[f"{site}_count"].iloc[0] if f"{site}_count" in df.columns else ''
                        duration = df[f"{site}_duration"].iloc[0] if f"{site}_duration" in df.columns else ''
                        f.write(f",{count},{duration:.0f}")
                    else:
                        f.write(",,")
                        print(f"File {filename} does not exist, skipping.")
                f.write("\n")
    except Exception as e:
        print(f"Error creating output file {output_filename}: {e}")
        exit(1)

    print("Script completed successfully.")
