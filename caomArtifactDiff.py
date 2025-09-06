from astroquery.cadc import Cadc
from datetime import datetime, timezone, timedelta
from pathlib import Path
import multiprocessing  
import pandas as pd
import sys
import os

## Set up variables
CERT_FILENAME = f"{Path.home()}/.ssl/cadcproxy.pem"
OUTPUT_DIRECTORY = f"caomArtifactDiff_reports"
OUTPUT_FILENAME_ROOT = "caomArtifactDiff"
SI_URL = "https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/luskan"
MAPPINGS_CONFIG = pd.DataFrame()
COLLECTIONS_CONFIG = pd.DataFrame()
SITES_CONFIG = pd.DataFrame()
TOTAL_CAOM_QUERY_TIME = 0
TOTAL_SI_QUERY_TIME = 0
COLLECTION_START_TIME = datetime.now(timezone.utc)

## Create a TAP service object for the given site.
## This function will handle the login and return the service object.
## If an error occurs, it will print the error message and exit the script.
def get_tap_service(site_url):
    try:
        service = Cadc(url=site_url)
        service.login(certificate_file=CERT_FILENAME)
        return service
    except Exception as e:
        print(f"Error creating serice object for {site_url}: {e}")
        exit(1)

## Execute a TAP async query on the given site service.
## This function will run the query and return the results as a pandas DataFrame.
def execute_query(site_service, site_name, site_query):
    try:
        query_results = site_service.exec_sync(site_query).to_pandas()
        return query_results
        '''
        job = site_service.create_async(site_query)
        print(f"Job ID for {site_name}: {job.job_id}")
        job.run().wait()
        job.raise_if_error()
        query_results = job.fetch_result().to_table().to_pandas()
        return query_results
        '''
    except Exception as e:
        print(f"Error querying {site_name}: {e}")
        exit(1)
    
## Query the Storage Inventory service for the specified collection.
def query_si_service(si_namespace, bucket_size):
    global TOTAL_SI_QUERY_TIME

    start_time = datetime.now()
    ## Create a TAP service object for the given SI service,
    ## format the query to the inventory.Artifact table and execute it.
    service = get_tap_service(SI_URL)
    service_query = f"""
        SELECT uri as uri, contentChecksum as contentCheckSum, contentLength as contentLength, contentType as contentType, contentLastModified as lastModified
        FROM inventory.Artifact AS A
        WHERE uri LIKE '{si_namespace}/%'
        order by uri
        """    
    service_query_result = execute_query(service, si_namespace, service_query)

    end_time = datetime.now()
    duration = end_time - start_time
    TOTAL_SI_QUERY_TIME += duration.total_seconds()

    return service_query_result

## Query the caom repository service for the specified collection in the specified si_namespace.
def query_caom_service(collection, si_namespace, bucket_size):
    global TOTAL_CAOM_QUERY_TIME

    start_time = datetime.now()
    ## First determine which ams_site and ams_url to use for the given collection
    row = COLLECTIONS_CONFIG[COLLECTIONS_CONFIG['collection'] == collection]
    ams_site = row['ams_site'].values[0]
    site_row = SITES_CONFIG[SITES_CONFIG['site_name'] == ams_site]
    if site_row.empty:
        print(f"Site {ams_site} for collection {collection} not found in sites configuration file.")
        exit(1)
    ams_url = site_row['site_url'].values[0]

    ## Create a TAP service object for the given CAOM repository service,
    ## format the query to the caom2.Artifact table for uris in the given si_namespace and execute it.
    service = get_tap_service(ams_url)
    service_query = f"""
        SELECT A.uri as uri, A.contentChecksum as contentCheckSum, A.contentLength as contentLength, A.contentType as contentType, A.lastModified as lastModified
        FROM caom2.Observation AS O
        JOIN caom2.Plane AS P ON O.obsID = P.obsID
        JOIN caom2.Artifact AS A ON A.planeID = P.planeID
        WHERE O.collection = '{collection}'
        and A.uri LIKE '{si_namespace}/%'
        order by A.uri
        """
    service_query_result = execute_query(service, ams_site, service_query)
    end_time = datetime.now()
    duration = end_time - start_time
    TOTAL_CAOM_QUERY_TIME += duration.total_seconds()

    return service_query_result

## GIven the results from CAOM and SI, compare them and write the differences to a CSV file.

def compare_results(collection, caom_query_result, si_query_result, filename):

    cmp_start_time = datetime.now()

    ## Compare the two DataFrames and identify missing and inconsistent files.
    caom_uris = set(caom_query_result['uri'])
    si_uris = set(si_query_result['uri'])

    missing_in_si = caom_uris - si_uris
    missing_in_caom = si_uris - caom_uris

    inconsistent_files = pd.merge(caom_query_result, si_query_result, on='uri', suffixes=('_caom', '_si'))
    inconsistent_files = inconsistent_files[
        (inconsistent_files['contentCheckSum_caom'] != inconsistent_files['contentCheckSum_si']) |
        (inconsistent_files['contentLength_caom'] != inconsistent_files['contentLength_si']) |
        (inconsistent_files['contentType_caom'] != inconsistent_files['contentType_si'])
    ]
    cmp_end_time = datetime.now()
    cmp_duration = cmp_end_time - cmp_start_time

    collection_end_time = datetime.now(timezone.utc)
    collection_duration = collection_end_time - COLLECTION_START_TIME
    
    ## Write the comparison results to a CSV file.
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
            f.write(f"Total files in CAOM: {len(caom_query_result)}\n")
            f.write(f"Total files in SI: {len(si_query_result)}\n")
            f.write(f"Number of files missing in SI: {len(missing_in_si)}\n")
            f.write(f"Number of files missing in CAOM: {len(missing_in_caom)}\n")
            f.write(f"Number of inconsistent files: {len(inconsistent_files)}\n")
    
            if len(missing_in_si) > 0:
                f.write(f"\nList of files missing in SI\n")
                f.write("category,uri,lastModified_caom\n")
                for uri in sorted(missing_in_si):
                    last_modified = caom_query_result[caom_query_result['uri'] == uri]['lastModified'].values[0]
                    f.write(f"MISSING_IN_SI,{uri},{last_modified}\n")

            if len(missing_in_caom) > 0:
                f.write(f"\nList of files missing in CAOM\n")
                f.write("category,uri,lastModified_si\n")
                for uri in sorted(missing_in_caom):
                    last_modified = si_query_result[si_query_result['uri'] == uri]['lastModified'].values[0]
                    f.write(f"MISSING_IN_CAOM,{uri},{last_modified}\n")

            if len(inconsistent_files) > 0:
                f.write(f"\nList of inconsistent files\n")
                f.write("category,uri,contentCheckSum_caom,contentCheckSum_si,contentLength_caom,contentLength_si,contentType_caom,contentType_si,lastModified_caom,lastModified_si\n")
                for _, row in inconsistent_files.iterrows():
                    f.write(f"INCONSISTENT,{row['uri']},{row['contentCheckSum_caom']},{row['contentCheckSum_si']},{row['contentLength_caom']},{row['contentLength_si']},{row['contentType_caom']},{row['contentType_si']},{row['lastModified_caom']},{row['lastModified_si']}\n")
        print(f"Comparison results written to {filename}")
    except Exception as e:
        print(f"Error writing comparison results to {filename}: {e}")
        exit(1)

    return
        
def query_collectionspace_buckets(row):

    collection = row['collection']
    ams_site = row['ams_site']
    ams_url = row['ams_url']
    si_namespace = row['si_namespace']
    si_scheme = row['si_scheme']
    num_char = row['num_char']
    collection_filename_root = f"{OUTPUT_FILENAME_ROOT}_{collection}"
    print(f"Comparing collection {collection} in {ams_site} with namespace {si_namespace} with {num_char} bucket characters.")
    print(f"Querying collection {collection} in namespace {row['si_namespace']} by buckets of {num_char} characters.")
    
    ## First create the list of buckets based on the number of characters.
    num_buckets = 16 ** num_char
    bucket_list = []
    i = 0
    while i < num_buckets:
        bucket_string = hex(i)[2:]
        bucket_list.append(bucket_string)
        i += 1
    print(bucket_list)

    ## Now iterate through each bucket in the bucket list, querying both the CAOM and SI services and then comparing the results.
    for bucket in bucket_list:
        print(f"Querying bucket {bucket} for collection {collection} in namespace {si_namespace}.")
        try:
            ## Query the CAOM service for the collection and bucket.
            caom_filename = f"{collection_filename_root}_caom_{bucket}.csv"
            caom_query_result = query_caom_service('caom', row['caom_url'], collection, caom_filename)

            ## Query the SI service for the collection and bucket.
            si_filename = f"{collection_filename_root}_si_{bucket}.csv"
            si_query_result = query_si_service(row['si_url'], collection, si_filename)

            ## Now compare the results from CAOM and SI.
            cmp_filename = f"{collection_filename_root}_diff_{bucket}.csv"
            compare_results(caom_query_result, si_query_result, collection, bucket, collection_datestamp,)
        except Exception as e:
            print(f"Error querying bucket {bucket} for collection {collection}: {e}")
            continue    
    return

''' 
    ## The following is example code for multiprocessing, but it is not currently used.
    processes = []

    ## Launch all queries in parallel
    try:
        filename = f"{collection_filename_root}_argus_Observation.csv"
        query = f"select count(*) as obsCount, max(maxLastModified) as max(maxLastModified) from caom2.Observation where collection = '{collection}'"
        p = multiprocessing.Process(target=query_site, args=(collection, collection_filename_root, collection_datestamp, site))
        p.start()
        processes.append(p)
    except Exception as e:
        print(f"Error starting process for site {site['site_name']} and collection {collection}: {e}")
        return
    
    if len(processes) == 0:
        print(f"No processes to join for collection {collection}.")
        return

    ## Join all processes to ensure they complete before moving on.
    for p in processes:
        try:
            p.join()
        except Exception as e:
            print(f"Error joining process {p} for collection {collection}: {e}")
            continue

    print(f"All queries for collection {collection} completed.")
'''

## For each collection/namespace combination, compare the entire list of files in one go.

def compare_collection(collection):
    global TOTAL_CAOM_QUERY_TIME, TOTAL_SI_QUERY_TIME, COLLECTION_START_TIME

    TOTAL_CAOM_QUERY_TIME = 0
    TOTAL_SI_QUERY_TIME = 0
    COLLECTION_START_TIME = datetime.now(timezone.utc)

    cmp_filename = f"{OUTPUT_FILENAME_ROOT}"
    caom_query_results = pd.DataFrame()
    si_query_results = pd.DataFrame()
    collection_list = []
    
    ## Determine the multiplicity of collections and/or si_namespaces. Starting with the given collection, how many si_namespaces
    ## is it referencing and in those referenced si_namespaces, how many collections are there?
    ## A collection may use one or more si_namespaces, and a si_namespace may be used by one or more collections.
    mapping_by_collection_rows = MAPPINGS_CONFIG[MAPPINGS_CONFIG['collection'] == collection]
    for index, mapping_by_collection_row in mapping_by_collection_rows.iterrows():
        si_namespace = mapping_by_collection_row['si_namespace']
        si_namespace_name = si_namespace.replace(':', '-')
        bucket_size = mapping_by_collection_row['num_char']
        if bucket_size != 0:
            print(f"Functionality to support buckets is not yet implemented: collection {collection} in namespace {si_namespace} with bucket size {bucket_size}.")
            return

        mapping_by_namespace_rows = MAPPINGS_CONFIG[MAPPINGS_CONFIG['si_namespace'] == si_namespace]
        for index, mapping_by_namespace_row in mapping_by_namespace_rows.iterrows():
            collection_to_query = mapping_by_namespace_row['collection']
            cmp_filename = f"{cmp_filename}_{collection_to_query}"
            collection_list.append(collection_to_query)
            print(f"Querying CAOM with collection {collection_to_query} and SI with namespace {si_namespace} and bucket size {bucket_size}.")
            try:
                query_result = query_caom_service(collection, si_namespace, bucket_size)
                caom_query_results = pd.concat([caom_query_results, query_result], ignore_index=True)
            except Exception as e:
                print(f"Error querying CAOM for collection {collection} in site {mapping_by_collection_row['ams_site']}: {e}")
                return   
    
        cmp_filename = f"{cmp_filename}_{si_namespace_name}"
        print(f"Querying SI namespace {si_namespace} with bucket size {bucket_size} for collections {collection_list}.")  
        try:
            query_result = query_si_service(si_namespace, bucket_size)
            si_query_results = pd.concat([si_query_results, query_result], ignore_index=True)
        except Exception as e:
            print(f"Error querying SI for collection {collection} in namespace {mapping_by_collection_row['si_namespace']}: {e}")
            return

    cmp_filename = f"{cmp_filename}_diff.csv"
    compare_results(collection, caom_query_results, si_query_results, cmp_filename)
    
    return
 
 
def read_configurations():
    global MAPPINGS_CONFIG, COLLECTIONS_CONFIG, SITES_CONFIG

    ## Read static configuration files for mapping collections to SI namespaces.
    ## This file must contain the columns collection, si_namespace, num_char.
    try:
        MAPPINGS_CONFIG = pd.read_csv("config/caomSiMappings.csv")
    except FileNotFoundError as e:
        print(f"Error reading configuration file: {e}")
        exit(1)

    ## Read static configuration file for mapping collections to AMS sites.
    ## This file must contain the columns collection, in_si and ams_site.
    try:
        COLLECTIONS_CONFIG = pd.read_csv("config/caomCollections.csv")
    except FileNotFoundError as e:
        print(f"Error reading collections file: {e}")
        exit(1)

    ## Read static configuration file for mapping AMS sites to URLs.
    ## This file must contain the columns site_name and site_url.
    try:
        SITES_CONFIG = pd.read_csv("config/caomSites.csv")
    except FileNotFoundError as e:
        print(f"Error reading sites file: {e}")
        exit(1)
    
    return


## If the collection list is empty, read all collections from the collections configuration file that have in_si = "True".
## Otherwise, use the collection list provided as arguments to the script and check that they are valid collections.

def validate_collection_list(collection_list):
    if len(collection_list) == 0:
        collection_list = COLLECTIONS_CONFIG[COLLECTIONS_CONFIG['in_si'] == True]['collection'].tolist()
        print(f"Querying all collections: {collection_list}")
    else:
        ## Verify the collections provided as arguments to the script are valid.
        for collection in collection_list:
                row = COLLECTIONS_CONFIG[COLLECTIONS_CONFIG['collection'] == collection]
                if row.empty or row['in_si'].values[0] != True:
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