from astroquery.cadc import Cadc
from datetime import datetime, timezone
from pathlib import Path
import multiprocessing  
import pandas as pd
import sys
import os

## Set up variables
CERT_FILENAME = f"{Path.home()}/.ssl/cadcproxy.pem"
TIME_STAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
OUTPUT_DIRECTORY = f"siCopies-{TIME_STAMP}"
OUTPUT_FILENAME_ROOT = f"siCopies-{TIME_STAMP}"

def query_site(namespace, namespace_filename_root, namespace_datestamp, site):

    site_name = site['site_name']
    site_url = site['url']

    ## Create a Cadc service object for the given site.
    try:
        site_service = Cadc(url=site_url)
        site_service.login(certificate_file=CERT_FILENAME)
    except Exception as e:
        print(f"Error creating service for site {site_name} at {site_url}: {e}")
        exit(1)

    site_query = f"select '{namespace_datestamp}' as datestamp, '{namespace}' as namespace, '{namespace_datestamp}' as datestamp, count(*) as {site_name}_count, '' as {site_name}_duration from inventory.Artifact where uri like '{namespace}/%'"
    site_filename = f"{namespace_filename_root}_{site_name}.csv"
    print(f"Querying site {site_name} for namespace {namespace}")

    try:
        start = datetime.now(timezone.utc)
        job = site_service.create_async(site_query)
        print(f"Job ID link: {site_url}/async/{job.job_id}")
        job.run().wait()
        job.raise_if_error()
        query_results = job.fetch_result().to_table().to_pandas()
        end = datetime.now(timezone.utc)
        duration = (end - start).total_seconds()
        query_results[f'{site_name}_duration'] = duration
        print(f"Query completed for {site_name} in {duration:.2f} seconds.")        
    except Exception as e:
        print(f"Error querying {site_name}: {e}")

    try:
        query_results.to_csv(site_filename, index=False)
        print(f"Results written to {site_filename}")
    except Exception as e:
        print(f"Error writing to {site_filename}: {e}")


## For a given namespace, query all sites in parallel and wait for them to complete.
def query_namespace(namespace):
    namespace_filename_root = f"{OUTPUT_FILENAME_ROOT}_{namespace}"
    namespace_datestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    processes = []

    ## Loop though the sites dataframe, format the query for the given namespce.
    for index, site in sites.iterrows():       
        try:
            p = multiprocessing.Process(target=query_site, args=(namespace, namespace_filename_root, namespace_datestamp, site))
            p.start()
            processes.append(p)
        except Exception as e:
            print(f"Error starting process for site {site['site_name']} and namespace {namespace}: {e}")
            continue
    
    if len(processes) == 0:
        print(f"No processes to join for namespace {namespace}.")
        return

    ## Join all processes to ensure they complete before moving on.
    for p in processes:
        try:
            p.join()
        except Exception as e:
            print(f"Error joining process {p} for namespace {namespace}: {e}")
            continue

    print(f"All queries for namespace {namespace} completed.")


## Main function to execute the script.
## It initializes the data structures by reading from pre-generated namespaces and sites files
## It then loops through the list of namespaces and processes them.
## If the script is give one or more arguments, these are the namespaces to be queried.
## The script will exit with a status code of 0 if successful, or 255 if an error occurs.

if __name__ == "__main__":

    ## Check if the certificate file exists.
    if not os.path.exists(CERT_FILENAME):
        print(f"Certificate file {CERT_FILENAME} does not exist. Please check the path.")
        exit(1)
    
    ## Determine where the siMonitoring directory is located and change to that directory.
    if os.path.isdir("/Users/gaudet_1/work/siMonitoring"):
        os.chdir("/Users/gaudet_1/work/siMonitoring")
    elif os.path.isdir("/arc/projects/CADC/siMonitoring"):
        os.chdir("/arc/projects/CADC/siMonitoring")
    else:
        print("Unable to determine the location of the siMonitoring directory.")
        exit(1)

    ## Check the first argument to determine if help is requested.
    if len(sys.argv) == 1 and sys.argv[0] in ['--help', '-h']:
        print(f"Usage: {sys.argv[0]} [namespace1 namespace2 ...]")
        print(f"       {sys.argv[0]} <-h || --help>")
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


    if len(sys.argv) == 1:
        ## If the script is given no arguments, it will query for all namespaces in the configuration file.
        namespaces_to_query = namespaces['namespace'].tolist()
        print(f"Querying all namespaces: {namespaces_to_query}")
    else:   
        ## If the script is given one or more arguments, these are the namespaces to be queried.
        namespaces_to_query = sys.argv[2:]
        error = False
        for namespace in namespaces_to_query:
            row_data = namespaces[namespaces['namespace'] == namespace]
            if row_data.empty:
                print(f"Namespace {namespace} not found in configuration file.")
                error = True
        if error == True:
            print("One or more namespaces not found in configuration file.")
            exit(255)   
        print(f"Querying specified site(s): {namespaces_to_query}")

    ## Creat a subdirectory for the output files if it does not exist.
    try:
        if not os.path.exists(OUTPUT_DIRECTORY):
            os.makedirs(OUTPUT_DIRECTORY)
            os.chdir(OUTPUT_DIRECTORY)
    except Exception as e:
        print(f"Error creating output directory {OUTPUT_DIRECTORY}: {e}")
        exit(1)
    
    ## Now query the namespaces.
    for namespace in namespaces_to_query:
        print(f"Querying namespace: {namespace}")
        query_namespace(namespace) 
    
    print("All namespaces have been queried.")
