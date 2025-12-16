from datetime import datetime, timezone
from pathlib import Path
import polars as pl
import requests
import os
import sys

## Set up variables
CERT_FILENAME = f"{Path.home()}/.ssl/cadcproxy.pem"
OUTPUT_DIRECTORY = f"artifactDup_reports"
OUTPUT_FILENAME_ROOT = "artifactDup"
SI_URL = "https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/luskan"
MAPPINGS_CONFIG = pl.DataFrame()
COLLECTIONS_CONFIG = pl.DataFrame()
SITES_CONFIG = pl.DataFrame()
CAOM_QUERY_DURATION = 0
MULTI_VALUED_SEPARATOR = '_'
PROCESSING_START_TIME = datetime.now(timezone.utc)

## Format a duration as HH:MM:SS
def format_duration(duration):
    total_seconds = int(duration.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"

## Execute the query as a sync call to the site URL, requesting CSV output as this is the most efficient
## way to get the query output which is then converted to a POLARS dataframe.

def execute_query(site_url, site_name, site_query):
    
    site_url_sync = site_url + "/sync"
    data_list = {"LANG": "ADQL", "RESPONSEFORMAT": "CSV", "QUERY": site_query}

    try:
        # Make the POST request with a streaming response and a 2 hour timeout
        with requests.post(site_url_sync, data=data_list, allow_redirects=True, cert=CERT_FILENAME, stream=True, timeout=7200) as response:
            response.raise_for_status()  # Raise an error for bad status codes
            # Read the raw CSV response into a Polars DataFrame
            query_result = pl.read_csv(response.raw)
        return query_result
    except requests.exceptions.HTTPError as e:
        print(f"{datetime.now(timezone.utc)} HTTP Error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"{datetime.now(timezone.utc)} Other Request Error: {e}")

    exit(1)

## Query the caom repository service for the specified collection in the specified si_namespace.
def query_caom_service(collection, si_namespace):
    global CAOM_QUERY_DURATION

    ## First determine which ams_site and ams_url to use for the given collection
    start_time = datetime.now(timezone.utc)
    row = COLLECTIONS_CONFIG.filter(pl.col('collection') == collection)
    ams_site = row['ams_site'][0]
    site_row = SITES_CONFIG.filter(pl.col('site_name') == ams_site)
    if site_row.is_empty():
        print(f"Site {ams_site} for collection {collection} not found in sites configuration file.")
        exit(1)
    ams_url = site_row['site_url'][0]

    ## Format the query to the caom2.Artifact table for uris in the given si_namespace and execute it.
    service_query = f"""SELECT A.uri, 
                case when A.productType = 'this' then 1 end as this,
                case when A.productType = 'science' then 1 end as science,
                case when A.productType = 'calibration' then 1 end as calibration,
                case when A.productType = 'preview' then 1 end as preview,
                case when A.productType = 'thumbnail' then 1 end as thumbnail,
                case when A.productType = 'auxiliary' then 1 end as auxiliary,
                case when A.productType = 'bias' then 1 end as bias,
                case when A.productType = 'coderived' then 1 end as coderived,
                case when A.productType = 'dark' then 1 end as dark,
                case when A.productType = 'documentation' then 1 end as documentation,
                case when A.productType = 'error' then 1 end as error,
                case when A.productType = 'flat' then 1 end as flat,
                case when A.productType = 'info' then 1 end as info,
                case when A.productType = 'noise' then 1 end as noise,
                case when A.productType = 'preview-image' then 1 end as preview_image,
                case when A.productType = 'preview-plot' then 1 end as preview_plot,
                case when A.productType = 'weight' then 1 end as weight
        FROM caom2.Observation AS O
        JOIN caom2.Plane AS P ON O.obsID = P.obsID
        JOIN caom2.Artifact AS A ON A.planeID = P.planeID
        WHERE O.collection = '{collection}'
        and A.uri LIKE '{si_namespace}/%'"""
    service_query_result = execute_query(ams_url, ams_site, service_query)

    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time
    CAOM_QUERY_DURATION += duration.total_seconds()

    return service_query_result

## Write inconsistent files to the output file. 

def write_files(f, filename, text, files_df):
    try:
        message = f"Number of {text} files\t{len(files_df)}"
        f.write(f"\n{message}\n")
        if len(files_df) > 0:
            files_df.write_csv(f, include_header=True, separator='\t')
    except Exception as e:
        print(f"Error writing comparison results to {filename}: {e}")
        exit(1)
        
    return

## Generate a dataframe of unique uri's with counts of number of instancs. Anything with a count > 1
## is a duplicate uri.

def process_query_results(query_result_df):

    process_start = datetime.now(timezone.utc)
    unique_uri_df = pl.DataFrame()

    ## Cast all product type columns to Int64 to ensure proper aggregation.
    query_result_df = query_result_df.with_columns(
        pl.col("this").cast( pl.Int64 ),
        pl.col("science").cast( pl.Int64 ),
        pl.col("calibration").cast( pl.Int64 ),
        pl.col("preview").cast( pl.Int64 ),
        pl.col("thumbnail").cast( pl.Int64 ),
        pl.col("auxiliary").cast( pl.Int64 ),
        pl.col("bias").cast( pl.Int64 ),
        pl.col("coderived").cast( pl.Int64 ),
        pl.col("dark").cast( pl.Int64 ),
        pl.col("documentation").cast( pl.Int64 ),
        pl.col("error").cast( pl.Int64 ),
        pl.col("flat").cast( pl.Int64 ),
        pl.col("info").cast( pl.Int64 ),
        pl.col("noise").cast( pl.Int64 ),
        pl.col("preview_image").cast( pl.Int64 ),
        pl.col("preview_plot").cast( pl.Int64 ),
        pl.col("weight").cast( pl.Int64 )
    )

    ## Merge rows by with the same uri and sum the productType columns.
    unique_uri_df = query_result_df.group_by( ["uri"] ).sum()
       
    ## Generate counts for each unique uri by horizontally summing the product type columns.
    unique_uri_df = unique_uri_df.with_columns(
        (pl.col("this") + pl.col("science") + pl.col("calibration") + pl.col("preview") + pl.col("thumbnail") +
         pl.col("auxiliary") + pl.col("bias") + pl.col("coderived") + pl.col("dark") + pl.col("documentation") +
         pl.col("error") + pl.col("flat") + pl.col("info") + pl.col("noise") + pl.col("preview_image") +
         pl.col("preview_plot") + pl.col("weight")).alias("count")
    )

    process_end = datetime.now(timezone.utc)
    process_duration = process_end - process_start
    return unique_uri_df, process_duration

def write_results(collection, si_namespaces, unique_uri_df, start_time, query_duration, processing_duration):
    write_start = datetime.now(timezone.utc)
    
        ## Count the number of unique uri's with count = 1 and count > 1
    num_uris = sum(unique_uri_df['count'])
    num_unique_uris = unique_uri_df.filter(pl.col('count') == 1).shape[0]
    num_duplicate_uris = unique_uri_df.filter(pl.col('count') > 1).shape[0]
    total_instances_duplicates = sum(unique_uri_df.filter(pl.col('count') > 1)['count'])
    
    ## Open output file and write the results.
    filename = f"{OUTPUT_FILENAME_ROOT}_{collection}.tsv"
    print(f"Writing processing results to {filename}.")
    try:
        f = open(filename, 'w')
        f.write(f"Query results for collection {collection}\n")
        f.write(f"\n")
        f.write(f"Start time\t{start_time.strftime('%Y-%m-%dT%H:%M:%S')} UTC\n")
        f.write(f"SI namespace(s)\t{si_namespaces}\n")
        f.write(f"AMS query duration\t{format_duration(query_duration)}\n")
        f.write(f"Processing duration\t{format_duration(processing_duration)}\n")
        f.write(f"\n")

        f.write(f"Total number of artifact URIs\t{num_uris}\n")
        f.write(f"Number of single instance artifact URIs\t{num_unique_uris}\n")
        f.write(f"Number of duplicate URIs\t{num_duplicate_uris}\n")
        f.write(f"Total instances of duplicate URIs\t{total_instances_duplicates}\n")   
        f.write(f"\n")

        ## Write the list of duplicate uri's if there are any.
        if num_duplicate_uris > 0:
            f.write(f"List of duplicate uri's:\n")
            unique_uri_df.filter(pl.col('count') > 1).write_csv(f, include_header=True, separator='\t')
        
        ## Write a summary of the processing.
        write_end = datetime.now(timezone.utc)
        write_duration = write_end - write_start
        end_time = datetime.now(timezone.utc)
        total_duration = end_time - start_time 
        f.write(f"\n")
        message = f"Category\tCollection\tStart time\tNum URIs\tNum unique URIs\tNum duplicate URIs\tNum instances of duplicate URIs\tQuery duration\tProcessing duration\tWrite duration\tDuration\tEnd time"
        f.write(f"\n{message}\n")
        message = f"SUMMARY\t{collection}\t{start_time.strftime('%Y-%m-%dT%H:%M:%S')}\t{num_uris}\t{num_unique_uris}\t{num_duplicate_uris}\t{total_instances_duplicates}\t{format_duration(query_duration)}\t{format_duration(processing_duration)}\t{format_duration(write_duration)}\t{format_duration(total_duration)}\t{end_time.strftime('%Y-%m-%dT%H:%M:%S')}\n"
        f.write(f"{message}\n")
        f.flush()
        print(message)
    except Exception as e:
        print(f"Error opening or writing intro to {filename}: {e}")
    
    return
        
## For each collection/namespace combination, compare the entire list of files in one go.

def query_collection(collection, si_namespaces):
    query_start = datetime.now(timezone.utc)
    query_results_df = pl.DataFrame()

    ## If there are underscores (separators), split namespaces into lists.
    if MULTI_VALUED_SEPARATOR in si_namespaces:
        si_namespace_list = si_namespaces.split(MULTI_VALUED_SEPARATOR)
    else:
        si_namespace_list = [si_namespaces]

    ## Loop through each collection and namespace combination, query CAOM and concatenate the results into a single dataframe.
    for si_namespace in si_namespace_list:
        print(f"Querying CAOM for collection {collection} with artifacts like {si_namespace}/%.")
        try:
            result_df = query_caom_service(collection, si_namespace)
            query_results_df = pl.concat([query_results_df, result_df])
        except Exception as e:
            print(f"Error querying CAOM for collection {collection} in namespace {si_namespace}: {e}")
            return   

    query_end = datetime.now(timezone.utc)
    query_duration = query_end - query_start
    return query_results_df, query_duration    

## Read the configuration files into global dataframes.
 
def read_configurations():
    global MAPPINGS_CONFIG, COLLECTIONS_CONFIG, SITES_CONFIG

    ## Read static configuration files for mapping collections to SI namespaces.
    ## This file must contain the columns collection, si_namespace.
    try:
        MAPPINGS_CONFIG = pl.read_csv("config/caomSiMappings.tsv", separator='\t')
    except FileNotFoundError as e:
        print(f"Error reading configuration file: {e}")
        exit(1)

    ## Read static configuration file for mapping collections to AMS sites.
    ## This file must contain the columns collection, in_si and ams_site.
    try:
        COLLECTIONS_CONFIG = pl.read_csv("config/caomCollections.tsv", separator='\t')
    except FileNotFoundError as e:
        print(f"Error reading collections file: {e}")
        exit(1)

    ## Read static configuration file for mapping AMS sites to URLs.
    ## This file must contain the columns site_name and site_url.
    try:
        SITES_CONFIG = pl.read_csv("config/caomSites.tsv", separator='\t')
    except FileNotFoundError as e:
        print(f"Error reading sites file: {e}")
        exit(1)
    
    return

## Prepare a list of collection/si_namespace mappings to be processed. Create a data frame with columns collection and si_namespaces.
## For a collection that used multiple si namespaces, concatenate the namespace names with a semi-colon.
def prepare_collection_si_mappings(collection_list):
    processing_df = pl.DataFrame()
    for collection in collection_list:
       
       ## Find all namespaces for the given collection.
        mapping_by_collection_rows = MAPPINGS_CONFIG.filter(pl.col('collection') == collection)
        si_namespace_list = []
        for mapping_by_collection_row in mapping_by_collection_rows.iter_rows(named=True):
            si_namespace = mapping_by_collection_row['si_namespace']
            if si_namespace not in si_namespace_list:
                si_namespace_list.append(si_namespace)
       
        ## Now create a new row in the processing dataframe for the collection and the list of namespaces.
        si_namespaces = "_".join(si_namespace_list)
        new_row = pl.DataFrame({"collection": collection, "si_namespaces": [si_namespaces]})
        processing_df = pl.concat([processing_df, new_row])
    
    return processing_df

## If the collection list is empty, read all collections from the collections configuration file that have in_si = "True".
## Otherwise, use the collection list provided as arguments to the script and check that they are valid collections.
def validate_collection_list(collection_list):
    if len(collection_list) == 0:
        collection_list = COLLECTIONS_CONFIG.filter(pl.col('in_si') == True)['collection'].to_list()
    else:
        ## Verify the collections provided as arguments to the script are valid.
        for collection in collection_list:
            row = COLLECTIONS_CONFIG.filter(pl.col('collection') == collection)
            if row.is_empty() or not row['in_si'][0]:
                print(f"Collection {collection} not found in collections configuration file.")
                exit(1)    
    
    print("Collections to be processed: ", end="")
    print(*collection_list)
    return collection_list

## Main function to execute the script.
## It initializes the data structures by reading from the pre-generated list of collections.
## It then loops through the list of collections and queries the list of uri's from CAOM and SI and
## compares them to identify missing and inconsistent files.
## If the script is give one or more arguments, these are the collections to be queried.
## The script will exit with a status code of 0 if successful, or 255 if an error occurs.

if __name__ == "__main__":

    ## Check if the certificate file exists.
    if not os.path.exists(CERT_FILENAME):
        print(f"Certificate file {CERT_FILENAME} does not exist. Please check the path.")
        exit(1)
    
    ## Determine where the siMonitoring directory is located and change to that directory.
    if os.path.isdir("/Users/gaudet_1/work/collectionAuditing"):
        os.chdir("/Users/gaudet_1/work/collectionAuditing")
    elif os.path.isdir("/arc/projects/CADC/collectionAuditing"):
        os.chdir("/arc/projects/CADC/collectionAuditing")
    else:
        print("Unable to determine the location of the collectionAuditing directory.")
        exit(1)

    ## Check the first argument to determine if help is requested.
    if len(sys.argv) == 2 and sys.argv[1] in ['--help', '-h']:
        print(f"Usage: {os.path.basename(sys.argv[0])} [collection1 collection2 ...]")
        print(f"       {os.path.basename(sys.argv[0])} <-h || --help>")
        exit(0)

    ## Reaed all configuration files into global dataframes.
    read_configurations()

    ## Create the list of collections to verify, either from the list provided on the command line or from the configuration file.
    collection_list = validate_collection_list(sys.argv[1:])

    ## Prepare the list of collection/si_namespace mappings to be processed.
    processing_df = prepare_collection_si_mappings(collection_list)

    ## Creat a subdirectory for the output files if it does not exist.
    try:
        if not os.path.exists(OUTPUT_DIRECTORY):
            os.makedirs(OUTPUT_DIRECTORY)
        os.chdir(OUTPUT_DIRECTORY)
    except Exception as e:
        print(f"Error creating output directory {OUTPUT_DIRECTORY}: {e}")
        exit(1)
    
    ## Now loop though the processing dataframe.
    for row in processing_df.iter_rows(named=True):
        collection = row['collection']
        si_namespaces = row['si_namespaces']
        print(f"Processing collection {collection} with SI namespace(s) {si_namespaces}.")

        start_time = datetime.now(timezone.utc)
        query_results_df, query_duration = query_collection(collection, si_namespaces)
        unique_uri_df, processing_duration = process_query_results(query_results_df)
        write_results(collection, si_namespaces, unique_uri_df, start_time, query_duration, processing_duration)

    print("All collections processed.")    
    exit(0)