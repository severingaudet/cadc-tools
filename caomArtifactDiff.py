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
CAOM_QUERY_DURATION = 0
SI_QUERY_DURATION = 0
MULTI_VALUED_SEPARATOR = '_'
PROCESSING_START_TIME = datetime.now(timezone.utc)

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

## Query the Storage Inventory service for the specified collection.
def query_si_service(si_namespace):
    global SI_QUERY_DURATION

    ## Format the query to the inventory.Artifact table and execute it.
    start_time = datetime.now(timezone.utc)
    service_query = f"""SELECT uri as uri, contentChecksum as contentCheckSum, contentLength as contentLength, contentType as contentType, contentLastModified as lastModified
        FROM inventory.Artifact AS A
        WHERE uri LIKE '{si_namespace}/%'"""    
    service_query_result = execute_query(SI_URL, si_namespace, service_query)

    ## Now sort the result by uri and remove any duplicates by retaining the first instance. Although SI has a unique index on uri, this would protect against any change there.
    service_query_result = service_query_result.sort('uri').unique(subset=['uri'], keep='first')

    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time
    SI_QUERY_DURATION += duration.total_seconds()

    return service_query_result

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
    service_query = f"""SELECT A.uri as uri, A.contentChecksum as contentCheckSum, A.contentLength as contentLength, A.contentType as contentType, A.lastModified as lastModified
        FROM caom2.Observation AS O
        JOIN caom2.Plane AS P ON O.obsID = P.obsID
        JOIN caom2.Artifact AS A ON A.planeID = P.planeID
        WHERE O.collection = '{collection}'
        and A.uri LIKE '{si_namespace}/%'"""
    service_query_result = execute_query(ams_url, ams_site, service_query)

    ## Now sort the result by uri and remove any duplicates by retaining the first instance. Some collections such as JWST have multiple entries in CAOM for the same uri.
    service_query_result = service_query_result.sort('uri').unique(subset=['uri'], keep='first')

    ## Now cast the data type of the contentLength column to Int64 as this is how it is represented in SI. If a collection has only null values for contentLength,
    ## the data type will not be set to Int64 and this will cause problems when comparing the dataframes.
    service_query_result = service_query_result.with_columns(pl.col('contentLength').cast(pl.Int64))
        
    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time
    CAOM_QUERY_DURATION += duration.total_seconds()

    return service_query_result

## Write missing files to the output file. 

def write_missing_files(f, filename, text, missing_files):
    try:
        if len(missing_files) > 0:
            message = f"List of {len(missing_files)} {text} files"
            f.write(f"\n{message}\n")
            f.write("category,uri,lastModified\n")
            for row in missing_files.iter_rows(named=True):
                f.write(f"{text},{row['uri']},{row['lastModified']}\n")
            f.flush()
    except Exception as e:
        print(f"Error writing comparison results to {filename}: {e}")
        exit(1)
        
    return

## Write the inconsistent files to the output file. This is done in a separate function to avoid code duplication.

def write_inconsistent_files(f, filename, text, inconsistent_files):
    try:
        if len(inconsistent_files) > 0:
            message = f"List of {len(inconsistent_files)} {text} files"
            f.write(f"\n{message}\n")
            f.write("category,uri,contentCheckSum_caom,contentCheckSum_si,contentLength_caom,contentLength_si,contentType_caom,contentType_si,lastModified_caom,lastModified_si\n")
            for row in inconsistent_files.iter_rows(named=True):
                f.write(f"{text},{row['uri']},{row['contentCheckSum']},{row['contentCheckSum_si']},{row['contentLength']},{row['contentLength_si']},{row['contentType']},{row['contentType_si']},{row['lastModified']},{row['lastModified_si']}\n")
            f.flush()
    except Exception as e:
        print(f"Error writing comparison results to {filename}: {e}")
        exit(1)
        
    return


## Given the results from CAOM and SI, compare them and write the differences to a CSV file.

def compare_results(collections, caom_query_result, si_query_result, filename):

    cmp_start_time = datetime.now(timezone.utc)

    ## Count the number of uri's that are in both CAOM and SI and have the same contentCheckSum, contentLength and contentType values.
    consistent_files = pl.DataFrame()
    consistent_files = caom_query_result.join(
        si_query_result, on='uri', suffix='_si'
    ).filter(
        (pl.col('contentCheckSum') == pl.col('contentCheckSum_si')) &
        (pl.col('contentLength') == pl.col('contentLength_si')) &
        (pl.col('contentType') == pl.col('contentType_si'))
    ).select(pl.col('uri')).sort('uri')
    num_consistent_files = len(consistent_files)
    size_consistent_files = consistent_files.estimated_size()
    del consistent_files

   ## create a new dataframe that contains the uri and the lastModified value of rows of uri's that are in CAOM but not in SI.
    missing_in_si = pl.DataFrame()
    missing_in_si = caom_query_result.join(
        si_query_result, on='uri', how='anti'
    ).select(
        pl.col('uri'), pl.col('lastModified')
    ).sort('uri')

    ## create a new dataframe that contains the uri and lastModified value of rows of uri's that are in SI but not in CAOM.
    missing_in_caom = pl.DataFrame()
    missing_in_caom = si_query_result.join(
        caom_query_result, on='uri', how='anti'
    ).select(
        pl.col('uri'), pl.col('lastModified')
    ).sort('uri')

    ## create a new dataframe that contains the rows of uri's that are in both CAOM and SI, have different contentCheckSum values while other values match.
    diff_checksums = pl.DataFrame()
    diff_checksums = caom_query_result.join(
        si_query_result, on='uri', suffix='_si'
    ).filter(
        (pl.col('contentCheckSum') != pl.col('contentCheckSum_si')) &
        (pl.col('contentLength') == pl.col('contentLength_si')) &
        (pl.col('contentType') == pl.col('contentType_si'))
    ).sort('uri')

    ## create a new dataframe that contains the rows of uri's that are in both CAOM and SI but have different contentCheckSum values and contentLength but the same contentType values.
    diff_checksums_lengths = pl.DataFrame()
    diff_checksums_lengths = caom_query_result.join(
        si_query_result, on='uri', suffix='_si'
    ).filter(
        (pl.col('contentCheckSum') != pl.col('contentCheckSum_si')) &
        (pl.col('contentLength') != pl.col('contentLength_si')) &
        (pl.col('contentType') == pl.col('contentType_si'))
    ).sort('uri')

    ## create a new dataframe that contains the rows of uri's that are in both CAOM and SI but have different contentCheckSum values and contentType but the same contentLength values.
    diff_checksums_types = pl.DataFrame()
    diff_checksums_types = caom_query_result.join(
        si_query_result, on='uri', suffix='_si'
    ).filter(
        (pl.col('contentCheckSum') != pl.col('contentCheckSum_si')) &
        (pl.col('contentLength') == pl.col('contentLength_si')) &
        (pl.col('contentType') != pl.col('contentType_si'))
    ).sort('uri')

    ## create a new dataframe that contains the rows of uri's that are in both CAOM and SI but have different contentCheckSum values, contentLength and contentType values.
    diff_checksums_lengths_types = pl.DataFrame()
    diff_checksums_lengths_types = caom_query_result.join(
        si_query_result, on='uri', suffix='_si'
    ).filter(
        (pl.col('contentCheckSum') != pl.col('contentCheckSum_si')) &
        (pl.col('contentLength') != pl.col('contentLength_si')) &
        (pl.col('contentType') != pl.col('contentType_si'))
    ).sort('uri')

    ## Create a new dataframe that contains the rows of uri's that are in both CAOM and SI but have different contentLength values while other values match.
    diff_lengths = pl.DataFrame()
    diff_lengths = caom_query_result.join(
        si_query_result, on='uri', suffix='_si'
    ).filter(
        (pl.col('contentCheckSum') == pl.col('contentCheckSum_si')) &
        (pl.col('contentLength') != pl.col('contentLength_si')) &
        (pl.col('contentType') == pl.col('contentType_si'))
    ).sort('uri')

    ## Create a new dataframe that contains the rows of uri's that are in both CAOM and SI but have different contentLength and contentType values while other values match.
    diff_lengths_types = pl.DataFrame()
    diff_lengths_types = caom_query_result.join(
        si_query_result, on='uri', suffix='_si'
    ).filter(
        (pl.col('contentCheckSum') == pl.col('contentCheckSum_si')) &
        (pl.col('contentLength') != pl.col('contentLength_si')) &
        (pl.col('contentType') != pl.col('contentType_si'))
    ).sort('uri')

    ## Create a new dataframe that contains the rows of uri's that are in both CAOM and SI but have different contentType values while other values match.
    diff_types = pl.DataFrame()
    diff_types = caom_query_result.join(
        si_query_result, on='uri', suffix='_si'
    ).filter(
        (pl.col('contentCheckSum') == pl.col('contentCheckSum_si')) &
        (pl.col('contentLength') == pl.col('contentLength_si')) &
        (pl.col('contentType') != pl.col('contentType_si'))
    ).sort('uri')

    cmp_end_time = datetime.now(timezone.utc)
    cmp_duration = cmp_end_time - cmp_start_time

    ## print a summary of the comparison results.
    print(f"Files in CAOM: {len(caom_query_result)}; in SI: {len(si_query_result)}; in CAOM and not in SI: {len(missing_in_si)}; in SI and not in CAOM: {len(missing_in_caom)}; different checksums: {len(diff_checksums)}; different lengths: {len(diff_lengths)}; different types: {len(diff_types)}. Comparison took {cmp_duration.total_seconds():.2f} seconds.")
    
    ## Write the comparison results to a CSV file.
    print(f"Writing comparison results to {filename}.")
    try:
        with open(filename, 'w') as f:
            write_start_time = datetime.now(timezone.utc)
            f.write(f"Comparison results for collection(s) {collections}\n")
            f.write(f"\n")
            f.write(f"Processing began on {PROCESSING_START_TIME.strftime('%Y-%m-%dT%H-%M-%S')} UTC\n")
            f.write(f"\n")
            f.write(f"Total CAOM query time: {CAOM_QUERY_DURATION:.2f} seconds\n")
            f.write(f"Total SI query time: {SI_QUERY_DURATION:.2f} seconds\n")
            f.write(f"Total comparison time: {cmp_duration.total_seconds():.2f} seconds\n")
            f.write(f"\n")
            f.write(f"Files and dataframe size in CAOM: {len(caom_query_result)} rows, {caom_query_result.estimated_size()} bytes\n")
            f.write(f"Files and dataframe size in SI: {len(si_query_result)} rows, {si_query_result.estimated_size()} bytes\n")
            f.write(f"Files and dataframe size for consistent files: {num_consistent_files} rows, {size_consistent_files} bytes\n")
            f.write(f"Files and dataframe size for in CAOM and not in SI: {len(missing_in_si)} rows, {missing_in_si.estimated_size()} bytes\n")
            f.write(f"Files and dataframe size for in SI and not in CAOM: {len(missing_in_caom)} rows, {missing_in_caom.estimated_size()} bytes\n")
            f.write(f"Files and dataframe size with different checksums, lengths and types: {len(diff_checksums_lengths_types)} rows, {diff_checksums_lengths_types.estimated_size()} bytes\n")
            f.write(f"Files and dataframe size with different checksums and lengths: {len(diff_checksums_lengths)} rows, {diff_checksums_lengths.estimated_size()} bytes\n")
            f.write(f"Files and dataframe size with different checksums and types: {len(diff_checksums_types)} rows, {diff_checksums_types.estimated_size()} bytes\n")
            f.write(f"Files and dataframe size with different checksums: {len(diff_checksums)} rows, {diff_checksums.estimated_size()} bytes\n")
            f.write(f"Files and dataframe size with different lengths and types: {len(diff_lengths_types)} rows, {diff_lengths_types.estimated_size()} bytes\n")
            f.write(f"Files and dataframe size with different lengths: {len(diff_lengths)} rows, {diff_lengths.estimated_size()} bytes\n")
            f.write(f"Files and dataframe size with different types: {len(diff_types)} rows, {diff_types.estimated_size()} bytes\n")
            f.flush()
    
            ## Write the missing files to the output file.
            write_missing_files(f, filename, "MISSING_IN_SI", missing_in_si)
            write_missing_files(f, filename, "MISSING_IN_CAOM", missing_in_caom)

            ## Write the inconsistent files to the output file.
            write_inconsistent_files(f, filename, "DIFF_CHECKSUMS_LENGTHS_TYPES", diff_checksums_lengths_types)
            write_inconsistent_files(f, filename, "DIFF_CHECKSUMS_LENGTHS", diff_checksums_lengths)
            write_inconsistent_files(f, filename, "DIFF_CHECKSUMS_TYPES", diff_checksums_types)
            write_inconsistent_files(f, filename, "DIFF_CHECKSUMS", diff_checksums)
            write_inconsistent_files(f, filename, "DIFF_LENGTHS_TYPES", diff_lengths_types)
            write_inconsistent_files(f, filename, "DIFF_LENGTHS", diff_lengths)
            write_inconsistent_files(f, filename, "DIFF_TYPES", diff_types)
            
            ## Finally, write the summary message
            write_end_time = datetime.now(timezone.utc)
            write_duration = write_end_time - write_start_time
            processing_end_time = datetime.now(timezone.utc)
            processing_duration = processing_end_time - PROCESSING_START_TIME
            
            message = f"category,collections,processing_start_time,files_in_caom,files_in_si,consistent_files,files_in_caom_not_in_si,files_in_si_not_in_caom,diff_checksums_lengths_types,diff_checksums_lengths,diff_checksums_types,diff_checksums,diff_lengths_types,diff_lengths,diff_types,caom_query_duration_seconds,si_query_duration_seconds,comparison_duration_seconds,write_duration_seconds,processing_duration_seconds,processing_end_time"
            f.write(f"\n{message}\n")
            message = f"SUMMARY,{collections},{PROCESSING_START_TIME.strftime('%Y-%m-%dT%H-%M-%S')},{len(caom_query_result)},{len(si_query_result)},{num_consistent_files},{len(missing_in_si)},{len(missing_in_caom)},{len(diff_checksums_lengths_types)},{len(diff_checksums_lengths)},{len(diff_checksums_types)},{len(diff_checksums)},{len(diff_lengths_types)},{len(diff_lengths)},{len(diff_types)},{CAOM_QUERY_DURATION:.2f},{SI_QUERY_DURATION:.2f},{cmp_duration.total_seconds():.2f},{write_duration.total_seconds():.2f},{processing_duration.total_seconds():.2f},{processing_end_time.strftime('%Y-%m-%dT%H-%M-%S')}\n"
            f.write(f"{message}\n")
            f.flush()
            print(message)

            ## Explicitly delete the dataframes to free up memory.
            del missing_in_si
            del missing_in_caom
            del diff_checksums_lengths_types
            del diff_checksums_lengths
            del diff_checksums_types
            del diff_checksums
            del diff_lengths_types
            del diff_lengths
            del diff_types
            del caom_query_result
            del si_query_result

    except Exception as e:
        print(f"Error writing comparison results to {filename}: {e}")
        exit(1)

    return
        
## For each collection/namespace combination, compare the entire list of files in one go.

def process_collections_namespaces(collections, si_namespaces):
    global CAOM_QUERY_DURATION, SI_QUERY_DURATION, PROCESSING_START_TIME

    CAOM_QUERY_DURATION = 0
    SI_QUERY_DURATION = 0
    PROCESSING_START_TIME = datetime.now(timezone.utc)

    cmp_filename = f"{OUTPUT_FILENAME_ROOT}"
    caom_query_results = pl.DataFrame()
    si_query_results = pl.DataFrame()

    ## If there are underscores (separators), wplit the collections and namespaces into lists.
  
    if MULTI_VALUED_SEPARATOR in collections:
        collection_list = collections.split(MULTI_VALUED_SEPARATOR)
    else:
        collection_list = [collections]
    
    if MULTI_VALUED_SEPARATOR in si_namespaces:
        si_namespace_list = si_namespaces.split(MULTI_VALUED_SEPARATOR)
    else:
        si_namespace_list = [si_namespaces]

    ## Loop through each collection and namespace combination, query CAOM and concatenate the results into a single dataframe.
    for collection in collection_list:
        for si_namespace in si_namespace_list:
            print(f"Querying CAOM for collection {collection} with artifacts like {si_namespace}/%.")
            try:
                query_result = query_caom_service(collection, si_namespace)
                caom_query_results = pl.concat([caom_query_results, query_result])
            except Exception as e:
                print(f"Error querying CAOM for collection {collection} in namespace {si_namespace}: {e}")
                return   

    ## Loop through each namespace and query SI, concatenating the results into a single dataframe.
    for si_namespace in si_namespace_list:
        print(f"Querying SI namespace {si_namespace}.")  
        try:
            query_result = query_si_service(si_namespace)
            si_query_results = pl.concat([si_query_results, query_result])
        except Exception as e:
            print(f"Error querying SI for collection {collection} in namespace {si_namespace}: {e}")
            return

    ## Now compare the results and write the differences to a CSV file.
    si_namespace_names = si_namespaces.replace(':', '-')
    cmp_filename = f"{cmp_filename}_{collections}_{si_namespace_names}.csv"
    print(f"Comparing  collection(s) {collections} and SI namespace(s) {si_namespaces} and writing results to {cmp_filename}.")  
    compare_results(collections, caom_query_results, si_query_results, cmp_filename)
    
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

## Prepare a list of collection/si_namespace mappings to be processed. Create a data frame with columns collections and si_namespaces.
## For a collection that used multiple si namespaces, concatenate the namespace names with a semi-colon.
## For a si_namespace that is used by multiple collections, concatenate the collection names with a semi-colon.
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
        
            ## Create a list of all collections for the given namespace. Remove collections from the list as they are found to avoid duplicates.
            mapping_by_namespace_rows = MAPPINGS_CONFIG.filter(pl.col('si_namespace') == si_namespace)
            collections_to_query_list = []
            for mapping_by_namespace_row in mapping_by_namespace_rows.iter_rows(named=True):
                collection_to_query = mapping_by_namespace_row['collection']
                if collection_to_query not in collections_to_query_list:
                    collections_to_query_list.append(collection_to_query)
                if collection_to_query != collection and collection_to_query in collection_list:
                    collection_list.remove(collection_to_query)
        
        collections_to_query = "_".join(collections_to_query_list)
        si_namespaces = "_".join(si_namespace_list)
        new_row = pl.DataFrame({"collections": [collections_to_query], "si_namespaces": [si_namespaces]})
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
    
    print(f"Collections to be processed: {collection_list}.")
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
        collections = row['collections']
        si_namespaces = row['si_namespaces']
        print(f"Collection(s) {collections} uses SI namespace(s): {si_namespaces}.")    
        process_collections_namespaces(collections, si_namespaces)
    print("All collections processed.")    
    exit(0)