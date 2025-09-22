from datetime import datetime, timezone
from pathlib import Path
import polars as pl
import requests
import os
import sys

## Set up variables
CERT_FILENAME = f"{Path.home()}/.ssl/cadcproxy.pem"
OUTPUT_DIRECTORY = f"caomArtifactDiff_reports"
OUTPUT_FILENAME_ROOT = "caomArtifactDiff"
SI_URL = "https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/luskan"
MAPPINGS_CONFIG = pl.DataFrame()
COLLECTIONS_CONFIG = pl.DataFrame()
SITES_CONFIG = pl.DataFrame()
TOTAL_CAOM_QUERY_TIME = 0
TOTAL_SI_QUERY_TIME = 0
COLLECTION_START_TIME = datetime.now(timezone.utc)

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
        print(f"HTTP Error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Other Request Error: {e}")

    exit(1)

## Query the Storage Inventory service for the specified collection.
def query_si_service(si_namespace):
    global TOTAL_SI_QUERY_TIME

    start_time = datetime.now()
    ## Format the query to the inventory.Artifact table and execute it.
    service_query = f"""SELECT uri as uri, contentChecksum as contentCheckSum, contentLength as contentLength, contentType as contentType, contentLastModified as lastModified
        FROM inventory.Artifact AS A
        WHERE uri LIKE '{si_namespace}/%'
        order by uri"""    
    service_query_result = execute_query(SI_URL, si_namespace, service_query)

    end_time = datetime.now()
    duration = end_time - start_time
    TOTAL_SI_QUERY_TIME += duration.total_seconds()

    return service_query_result

## Query the caom repository service for the specified collection in the specified si_namespace.
def query_caom_service(collection, si_namespace):
    global TOTAL_CAOM_QUERY_TIME

    start_time = datetime.now()
    ## First determine which ams_site and ams_url to use for the given collection
    row = COLLECTIONS_CONFIG.filter(pl.col('collection') == collection)
    ams_site = row['ams_site'][0]
    site_row = SITES_CONFIG.filter(pl.col('site_name') == ams_site)
    if site_row.is_empty():
        print(f"Site {ams_site} for collection {collection} not found in sites configuration file.")
        exit(1)
    ams_url = site_row['site_url'][0]

    ## Format the query to the caom2.Artifact table for uris in the given si_namespace and execute it.
    service_query = f"""SELECT A.uri as uri, A.contentChecksum as contentCheckSum, A.contentLength as contentLength, A.contentType as contentType, A.lastModified as lastModified
        FROM caom2.Observation AS O
        JOIN caom2.Plane AS P ON O.obsID = P.obsID
        JOIN caom2.Artifact AS A ON A.planeID = P.planeID
        WHERE O.collection = '{collection}'
        and A.uri LIKE '{si_namespace}/%'
        order by A.uri"""
    service_query_result = execute_query(ams_url, ams_site, service_query)
    end_time = datetime.now()
    duration = end_time - start_time
    TOTAL_CAOM_QUERY_TIME += duration.total_seconds()

    return service_query_result

## GIven the results from CAOM and SI, compare them and write the differences to a CSV file.

def compare_results(collection, caom_query_result, si_query_result, filename):

    cmp_start_time = datetime.now()

    ## Compare the two DataFrames and identify missing and inconsistent files.
    caom_uris = set(caom_query_result['uri'].to_list())
    si_uris = set(si_query_result['uri'].to_list())

    missing_in_si = caom_uris - si_uris
    if len(missing_in_si) > 0:
        missing_in_si = sorted(missing_in_si)
    missing_in_caom = si_uris - caom_uris
    if len(missing_in_caom) > 0:
        missing_in_caom = sorted(missing_in_caom)

    inconsistent_files = caom_query_result.join(
        si_query_result, on='uri', suffix='_si'
    ).filter(
        (pl.col('contentCheckSum') != pl.col('contentCheckSum_si')) |
        (pl.col('contentLength') != pl.col('contentLength_si')) |
        (pl.col('contentType') != pl.col('contentType_si'))
    )
    cmp_end_time = datetime.now()
    cmp_duration = cmp_end_time - cmp_start_time

    collection_end_time = datetime.now(timezone.utc)
    collection_duration = collection_end_time - COLLECTION_START_TIME

    ## print a summary of the comparison results.
    print(f"Files in CAOM: {len(caom_query_result)}; files in SI: {len(si_query_result)}; files in CAOM and not in SI: {len(missing_in_si)}; inconsistent files: {len(inconsistent_files)}; files in SI and not in CAOM: {len(missing_in_caom)}")
    
    ## Write the comparison results to a CSV file.
    write_start_time = datetime.now()
    try:
        with open(filename, 'w') as f:
            f.write(f"Comparison results for collection {collection}\n")
            f.write(f"\n")
            f.write(f"Began on {COLLECTION_START_TIME.strftime('%Y-%m-%dT%H-%M-%S')} UTC\n")
            f.write(f"Ended on {collection_end_time.strftime('%Y-%m-%dT%H-%M-%S')} UTC\n")
            f.write(f"Total collection processing time: {collection_duration.total_seconds():.2f} seconds\n")
            f.write(f"\n")
            f.write(f"Total CAOM query time: {TOTAL_CAOM_QUERY_TIME:.2f} seconds\n")
            f.write(f"Total SI query time: {TOTAL_SI_QUERY_TIME:.2f} seconds\n")
            f.write(f"Total comparison time: {cmp_duration.total_seconds():.2f} seconds\n")
            f.write(f"\n")
            f.write(f"Total files in CAOM: {caom_query_result.height}\n")
            f.write(f"Total files in SI: {si_query_result.height}\n")
            f.write(f"Number of files in CAOM and not in SI: {len(missing_in_si)}\n")
            f.write(f"Number of inconsistent files: {inconsistent_files.height}\n")
            f.write(f"Number of files in SI and not in CAOM: {len(missing_in_caom)}\n")
            f.flush()
    
            if len(missing_in_si) > 0:
                write_start_time = datetime.now()
                print(f"Writing list of {len(missing_in_si)} files in CAOM and not in SI.")
                f.write(f"\nList of files in CAOM and not in SI\n")
                f.write("category,uri,lastModified_caom\n")
                for uri in missing_in_si:
                    last_modified = caom_query_result.filter(pl.col('uri') == uri)['lastModified'][0]
                    f.write(f"MISSING_IN_SI,{uri},{last_modified}\n")
                write_end_time = datetime.now()
                write_duration = write_end_time - write_start_time
                print(f"Time to write list of {len(missing_in_si)} files in CAOM and not in SI {write_duration.total_seconds():.2f} seconds.")
                f.write(f"Time to write list of {len(missing_in_si)} files in CAOM and not in SI {write_duration.total_seconds():.2f} seconds.\n")
                f.flush()
                del missing_in_si

            if len(inconsistent_files) > 0:
                write_start_time = datetime.now()
                print(f"Writing list of {len(inconsistent_files)} inconsistent files.")
                f.write(f"\nList of inconsistent files\n")
                f.write("category,uri,contentCheckSum_caom,contentCheckSum_si,contentLength_caom,contentLength_si,contentType_caom,contentType_si,lastModified_caom,lastModified_si\n")
                for row in inconsistent_files.iter_rows(named=True):
                    f.write(f"INCONSISTENT,{row['uri']},{row['contentCheckSum']},{row['contentCheckSum_si']},{row['contentLength']},{row['contentLength_si']},{row['contentType']},{row['contentType_si']},{row['lastModified']},{row['lastModified_si']}\n")
                write_end_time = datetime.now()
                write_duration = write_end_time - write_start_time
                print(f"Time to write list of {len(inconsistent_files)} files {write_duration.total_seconds():.2f} seconds.")
                f.write(f"Time to write list of {len(inconsistent_files)} files {write_duration.total_seconds():.2f} seconds.\n")
                f.flush()
                del inconsistent_files

            if len(missing_in_caom) > 0:
                write_start_time = datetime.now()
                print(f"Writing list of {len(missing_in_caom)} files in SI and not in CAOM.")
                f.write(f"\nList of files in SI and not in CAOM\n")
                f.write("category,uri,lastModified_si\n")
                for uri in missing_in_caom:
                    last_modified = si_query_result.filter(pl.col('uri') == uri)['lastModified'][0]
                    f.write(f"MISSING_IN_CAOM,{uri},{last_modified}\n")
                write_end_time = datetime.now()
                write_duration = write_end_time - write_start_time
                print(f"Time to write list of {len(missing_in_caom)} files in SI and not in CAOM {write_duration.total_seconds():.2f} seconds.")
                f.write(f"Time to write list of {len(missing_in_caom)} files in SI and not in CAOM {write_duration.total_seconds():.2f} seconds.\n")
                f.flush()
                del missing_in_caom

    except Exception as e:
        print(f"Error writing comparison results to {filename}: {e}")
        exit(1)

    return
        
## For each collection/namespace combination, compare the entire list of files in one go.

def compare_collection(collection):
    global TOTAL_CAOM_QUERY_TIME, TOTAL_SI_QUERY_TIME, COLLECTION_START_TIME

    TOTAL_CAOM_QUERY_TIME = 0
    TOTAL_SI_QUERY_TIME = 0
    COLLECTION_START_TIME = datetime.now(timezone.utc)

    cmp_filename = f"{OUTPUT_FILENAME_ROOT}"
    caom_query_results = pl.DataFrame()
    si_query_results = pl.DataFrame()
    collection_list = []
    si_namespace_list = []
    
    ## Determine the multiplicity of collections and/or si_namespaces. Starting with the given collection, how many si_namespaces
    ## is it referencing and in those referenced si_namespaces, how many collections are there?
    ## A collection may use one or more si_namespaces, and a si_namespace may be used by one or more collections.

    mapping_by_collection_rows = MAPPINGS_CONFIG.filter(pl.col('collection') == collection)
    for mapping_by_collection_row in mapping_by_collection_rows.iter_rows(named=True):
        si_namespace = mapping_by_collection_row['si_namespace']
        si_namespace_list.append(si_namespace)
        si_namespace_name = si_namespace.replace(':', '-')

        mapping_by_namespace_rows = MAPPINGS_CONFIG.filter(pl.col('si_namespace') == si_namespace)
        for mapping_by_namespace_row in mapping_by_namespace_rows.iter_rows(named=True):
            collection_to_query = mapping_by_namespace_row['collection']
            cmp_filename = f"{cmp_filename}_{collection_to_query}"
            collection_list.append(collection_to_query)
            print(f"Querying CAOM with collection {collection_to_query} and SI with namespace {si_namespace}.")
            try:
                query_result = query_caom_service(collection, si_namespace)
                caom_query_results = pl.concat([caom_query_results, query_result])
            except Exception as e:
                print(f"Error querying CAOM for collection {collection} in site {mapping_by_collection_row['ams_site']}: {e}")
                return   
    
        cmp_filename = f"{cmp_filename}_{si_namespace_name}"
        print(f"Querying SI namespace {si_namespace} for collections {collection_list}.")  
        try:
            query_result = query_si_service(si_namespace)
            si_query_results = pl.concat([si_query_results, query_result])
        except Exception as e:
            print(f"Error querying SI for collection {collection} in namespace {mapping_by_collection_row['si_namespace']}: {e}")
            return

    cmp_filename = f"{cmp_filename}_diff.csv"
    print(f"Comparing SI namespace(s) {si_namespace_list} and collection(s) {collection_list}.")  
    compare_results(collection, caom_query_results, si_query_results, cmp_filename)
    
    return

## Read the configuration files into global dataframes.
 
def read_configurations():
    global MAPPINGS_CONFIG, COLLECTIONS_CONFIG, SITES_CONFIG

    ## Read static configuration files for mapping collections to SI namespaces.
    ## This file must contain the columns collection, si_namespace.
    try:
        MAPPINGS_CONFIG = pl.read_csv("config/caomSiMappings.csv")
    except FileNotFoundError as e:
        print(f"Error reading configuration file: {e}")
        exit(1)

    ## Read static configuration file for mapping collections to AMS sites.
    ## This file must contain the columns collection, in_si and ams_site.
    try:
        COLLECTIONS_CONFIG = pl.read_csv("config/caomCollections.csv")
    except FileNotFoundError as e:
        print(f"Error reading collections file: {e}")
        exit(1)

    ## Read static configuration file for mapping AMS sites to URLs.
    ## This file must contain the columns site_name and site_url.
    try:
        SITES_CONFIG = pl.read_csv("config/caomSites.csv")
    except FileNotFoundError as e:
        print(f"Error reading sites file: {e}")
        exit(1)
    
    return


## If the collection list is empty, read all collections from the collections configuration file that have in_si = "True".
## Otherwise, use the collection list provided as arguments to the script and check that they are valid collections.

def validate_collection_list(collection_list):
    if len(collection_list) == 0:
        collection_list = COLLECTIONS_CONFIG.filter(pl.col('in_si') == True)['collection'].to_list()
        print(f"Querying all collections: {collection_list}")
    else:
        ## Verify the collections provided as arguments to the script are valid.
        for collection in collection_list:
            row = COLLECTIONS_CONFIG.filter(pl.col('collection') == collection)
            if row.is_empty() or not row['in_si'][0]:
                print(f"Collection {collection} not found in collections configuration file.")
                exit(1)    
        print(f"Querying specified collection(s): {collection_list}")
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
    if os.path.isdir("/Users/gaudet_1/work/caomMonitoring"):
        os.chdir("/Users/gaudet_1/work/caomMonitoring")
    elif os.path.isdir("/arc/projects/CADC/caomMonitoring"):
        os.chdir("/arc/projects/CADC/caomMonitoring")
    else:
        print("Unable to determine the location of the caomMonitoring directory.")
        exit(1)

    ## Check the first argument to determine if help is requested.
    if len(sys.argv) == 1 and sys.argv[0] in ['--help', '-h']:
        print(f"Usage: {sys.argv[0]} [collection1 collection2 ...]")
        print(f"       {sys.argv[0]} <-h || --help>")
        exit(0)

    ## Reaed all configuration files into global dataframes.
    read_configurations()

    ## Create the list of collections to verify, either from the list provided on the command line
    ## create the list from the collections configuration.
    ## If no collections are specified, all collections in the configuration file will be queried.
    collection_list = validate_collection_list(sys.argv[1:])

    ## Creat a subdirectory for the output files if it does not exist.
    try:
        if not os.path.exists(OUTPUT_DIRECTORY):
            os.makedirs(OUTPUT_DIRECTORY)
        os.chdir(OUTPUT_DIRECTORY)
    except Exception as e:
        print(f"Error creating output directory {OUTPUT_DIRECTORY}: {e}")
        exit(1)
    
    ## Now loop though the collection list.
    for collection in collection_list:
        print(f"Processing collection {collection}.")
        compare_collection(collection)
    print("All collections processed.")    
    exit(0)