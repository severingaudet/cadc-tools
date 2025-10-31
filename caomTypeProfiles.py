from datetime import datetime, timezone
from pathlib import Path
import polars as pl
import requests
import os
import sys

CERT_FILENAME = f"{Path.home()}/.ssl/cadcproxy.pem"
OUTPUT_DIRECTORY = "typeProfiles_reports"
OUTPUT_FILENAME_ROOT = "typeProfiles"
AMS_QUERY_DURATION = 0
PROCESSING_START_TIME = datetime.now(timezone.utc)

COLLECTIONS_CONFIG = pl.DataFrame()
SITES_CONFIG = pl.DataFrame()

PLANE_ARTIFACT_TYPES_DF = pl.DataFrame()
DISTINCT_PLANE_ARTIFACT_TYPES_DF = pl.DataFrame()
ALL_TYPES_DF = pl.DataFrame()
NO_PLANES_DF = pl.DataFrame()
NO_ARTIFACTS_DF = pl.DataFrame()
JUNK_PLANES_DF = pl.DataFrame()

NO_PLANE_TEXT = "NO_PLANES"
NO_ARTIFACT_TEXT = "NO_ARTIFACTS"
TYPE_TEXT = "TYPE"

## Query the ams repository service for the specified collection.
def query_ams_service(ams_url, query):
    ams_url_sync = ams_url + "/sync"
    data_list = {"LANG": "ADQL", "RESPONSEFORMAT": "CSV", "QUERY": query}

    try:
        # Make the POST request with a streaming response and a 2 hour timeout
        with requests.post(ams_url_sync, data=data_list, allow_redirects=True, cert=CERT_FILENAME, stream=True, timeout=7200) as response:
            response.raise_for_status()  # Raise an error for bad status codes
            # Read the raw CSV response into a Polars DataFrame
            query_result = pl.read_csv(response.raw)
    except requests.exceptions.HTTPError as e:
        print(f"{datetime.now(timezone.utc)} HTTP Error: {e}")
        exit(1)
    except requests.exceptions.RequestException as e:
        print(f"{datetime.now(timezone.utc)} Other Request Error: {e}")
        exit(1)

    return query_result

def query_collection(collection):
    global AMS_QUERY_DURATION
    global PLANE_ARTIFACT_TYPES_DF
    global NO_ARTIFACTS_DF
    global NO_PLANES_DF
    global JUNK_PLANES_DF

    ## First determine which ams_site and ams_url to use for the given collection
    start_time = datetime.now(timezone.utc)
    row = COLLECTIONS_CONFIG.filter(pl.col('collection') == collection)
    ams_site = row['ams_site'][0]
    site_row = SITES_CONFIG.filter(pl.col('site_name') == ams_site)
    if site_row.is_empty():
        print(f"Site {ams_site} for collection {collection} not found in sites configuration file.")
        exit(1)
    ams_url = site_row['site_url'][0]

    query_no_planes = f"""
            select '{NO_PLANE_TEXT}' as category, collection, O.observationID, O.maxLastModified 
            from caom2.Observation as O left outer join caom2.Plane as P on O.obsID = P.obsID
            where O.collection = '{collection}' and P.planeID is null
        """.replace('\n', ' ')
    NO_PLANES_DF = query_ams_service(ams_url, query_no_planes)
    print( f"Observations with no planes: {len(NO_PLANES_DF)}" )

    query_no_artifacts = f"""
            select '{NO_ARTIFACT_TEXT}' as category, collection, O.observationID, P.planeID, P.dataProductType, P.maxLastModified
            from caom2.Observation as O join caom2.Plane as P on O.obsID = P.obsID left outer join caom2.Artifact as A on P.planeID = A.planeID
            where O.collection = '{collection}' and A.artifactID is null
        """.replace('\n', ' ')
    NO_ARTIFACTS_DF = query_ams_service(ams_url, query_no_artifacts)
    print( f"Planes with no artifacts: {len(NO_ARTIFACTS_DF)}" )

    query_junk_planes = f"""
            select 'JUNK_PLANE' as category, collection, O.observationID, P.planeID, P.dataProductType, P.maxLastModified
            from caom2.Observation as O join caom2.Plane as P on O.obsID = P.obsID
            where O.collection = '{collection}' and P.quality_flag = 'junk'
        """.replace('\n', ' ')
    JUNK_PLANES_DF = query_ams_service(ams_url, query_junk_planes)
    print( f"Junk planes: {len(JUNK_PLANES_DF)}" )

    query_plane_artifact_types = f"""
            select '{TYPE_TEXT}' as category, O.collection, P.planeID, P.dataProductType,
                case when A.productType = 'science' then 1 end as science,
                case when A.productType = 'calibration' then 1 end as calibration,
                case when A.productType = 'weight' then 1 end as weight,
                case when A.productType = 'noise' then 1 end as noise,
                case when A.productType = 'preview' then 1 end as preview,
                case when A.productType = 'thumbnail' then 1 end as thumbnail,
                case when A.productType = 'auxiliary' then 1 end as auxiliary,
                case when A.productType = 'info' then 1 end as info
            from caom2.Observation as O join caom2.Plane as P on O.obsID = P.obsID join caom2.Artifact as A on P.planeID = A.planeID
            where O.collection = '{collection}' and (P.quality_flag is null or P.quality_flag != 'junk')
        """.replace('\n', ' ')
    PLANE_ARTIFACT_TYPES_DF = query_ams_service(ams_url, query_plane_artifact_types)
    
    print( f"Number of artifacts: {len(PLANE_ARTIFACT_TYPES_DF)}" )

    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time
    AMS_QUERY_DURATION = duration.total_seconds()

    return

def write_no_planes(f, filename, collection, df):
    try:
        if len(df) > 0:
            message = f"List of {len(df)} NO_PLANES found"
            f.write(f"\n{message}\n")
            df.write_csv(f)
    except Exception as e:
        print(f"Error writing results to {filename}: {e}")
        exit(1)
        
    return

def write_no_artifacts(f, filename, collection, df):
    try:
        if len(df) > 0:
            message = f"List of {len(df)} NO_ARTIFACTS found"
            f.write(f"\n{message}\n")
            df.write_csv(f)
    except Exception as e:
        print(f"Error writing results to {filename}: {e}")
        exit(1)
        
    return

def write_profile(f, filename, collection, df):
    try:
        if len(df) > 0:
            message = f"List of {len(df)} TYPE combinations found"
            f.write(f"\n{message}\n")
            df.write_csv(f)
    except Exception as e:
        print(f"Error writing results to {filename}: {e}")
        exit(1)
        
    return

def write_csv(f, filename, text, df):
    try:
        if len(df) > 0:
            message = f"List of {len(df)} {text} found"
            f.write(f"\n{message}\n")
            df.write_csv(f)
    except Exception as e:
        print(f"Error writing results to {filename}: {e}")
        exit(1)
        
    return

## Write the query results to the output file.
def write_query_results(collection):
    global PLANE_ARTIFACT_TYPES_DF
    global DISTINCT_PLANE_ARTIFACT_TYPES_DF
    global NO_ARTIFACTS_DF
    global NO_PLANES_DF
    global JUNK_PLANES_DF
    global ALL_TYPES_DF

    filename = f"{OUTPUT_FILENAME_ROOT}_{collection}.csv"
    print(f"Writing query results to {filename}.")
    try:
        with open(filename, 'w') as f:
            write_start_time = datetime.now(timezone.utc)
            f.write(f"Query results for collection {collection}\n")
            f.write(f"\n")
            f.write(f"Processing began on {PROCESSING_START_TIME.strftime('%Y-%m-%dT%H:%M:%S')} UTC\n")
            f.write(f"Total AMS query time: {AMS_QUERY_DURATION:.2f} seconds\n")
            f.write(f"\n")

            f.write(f"Observations with no associated planes: {len(NO_PLANES_DF)}\n")
            f.write(f"Planes flagged as junk: {len(JUNK_PLANES_DF)}\n")
            f.write(f"Planes with no artifacts: {len(NO_ARTIFACTS_DF)}\n")
            f.write(f"Number of planes: {len(DISTINCT_PLANE_ARTIFACT_TYPES_DF)}\n")
            f.write(f"Number of artifacts: {len(PLANE_ARTIFACT_TYPES_DF)}\n")


            write_csv( f, filename, NO_PLANE_TEXT, NO_PLANES_DF )
            write_csv( f, filename, NO_ARTIFACT_TEXT, NO_ARTIFACTS_DF )
            write_csv( f, filename, TYPE_TEXT, ALL_TYPES_DF )

            ## Finally, write the summary message
            write_end_time = datetime.now(timezone.utc)
            write_duration = write_end_time - write_start_time
            processing_end_time = datetime.now(timezone.utc)
            processing_duration = processing_end_time - PROCESSING_START_TIME
            
            message = f"summary,collection,processing_start_time,observations_no_planes,planes_no_artifacts,junk_planes,num_planes,num_artifacts,type_combinations,ams_query_duration_seconds,write_duration_seconds,processing_duration_seconds,processing_end_time"
            f.write(f"\n{message}\n")
            message = f"SUMMARY,{collection},{PROCESSING_START_TIME.strftime('%Y-%m-%dT%H:%M:%S')},{len(NO_PLANES_DF)},{len(NO_ARTIFACTS_DF)},{len(JUNK_PLANES_DF)},{len(DISTINCT_PLANE_ARTIFACT_TYPES_DF)},{len(PLANE_ARTIFACT_TYPES_DF)},{len(ALL_TYPES_DF)},{AMS_QUERY_DURATION:.2f},{write_duration.total_seconds():.2f},{processing_duration.total_seconds():.2f},{processing_end_time.strftime('%Y-%m-%dT%H:%M:%S')}\n"
            f.write(f"{message}\n")
            f.flush()
            print(message)

            ## Explicitly delete the dataframes to free up memory.
            del PLANE_ARTIFACT_TYPES_DF
            del DISTINCT_PLANE_ARTIFACT_TYPES_DF
            del NO_ARTIFACTS_DF
            del NO_PLANES_DF
            del JUNK_PLANES_DF
            del ALL_TYPES_DF
    except Exception as e:
        print(f"Error writing query results to {filename}: {e}")
        exit(1)
    
    return

## Process the given collection by querying the ams service and writing the results to an output file.
def process_collection(collection):
    global PLANE_ARTIFACT_TYPES_DF, ALL_TYPES_DF, DISTINCT_PLANE_ARTIFACT_TYPES_DF

    ## Cast the auxiliary, calibration, info, noise, preview, science, thumbnail, weight columns to Int64 to allow aggregation
    PLANE_ARTIFACT_TYPES_DF = PLANE_ARTIFACT_TYPES_DF.with_columns(
        pl.col("science").cast( pl.Int64 ),
        pl.col("calibration").cast( pl.Int64 ),
        pl.col("weight").cast( pl.Int64 ),
        pl.col("noise").cast( pl.Int64 ),
        pl.col("preview").cast( pl.Int64 ),
        pl.col("thumbnail").cast( pl.Int64 ),
        pl.col("auxiliary").cast( pl.Int64 ),
        pl.col("info").cast( pl.Int64 )
)

    ## Merge rows by with the same collection, observationID, planeID, maxLastModified and set the auxiliary, calibration, info, noise, preview, science, thumbnail, weight columns to True if any one row in the plane is > 0.
    DISTINCT_PLANE_ARTIFACT_TYPES_DF = PLANE_ARTIFACT_TYPES_DF.group_by( ["category", "collection", "planeID", "dataProductType"] ).agg(
        [pl.col("science").min().alias("science"),
        pl.col("calibration").min().alias("calibration"),
        pl.col("weight").min().alias("weight"),
        pl.col("noise").min().alias("noise"),
        pl.col("preview").min().alias("preview"),
        pl.col("thumbnail").min().alias("thumbnail"),
        pl.col("auxiliary").min().alias("auxiliary"),
        pl.col("info").min().alias("info")]
    )
    print( f"All planes after merging artifacts: {len(DISTINCT_PLANE_ARTIFACT_TYPES_DF)}" )

    ## List distinct dataProductTypes, auxiliary, calibration, info, noise, science, weight, preview, thumbnail
    ALL_TYPES_DF = DISTINCT_PLANE_ARTIFACT_TYPES_DF.select( ["category", "collection", "dataProductType", "science", "calibration", "weight", "noise", "preview", "thumbnail", "auxiliary", "info"] ).unique().sort( ["category", "collection", "dataProductType", "science", "calibration", "weight", "noise", "preview", "thumbnail", "auxiliary", "info"] )
    print( f"Number of distinct combinations of plane and artifact types: {len(ALL_TYPES_DF)}" )

    return


## If the collection list is empty, read all collections from the collections configuration file.
## Otherwise, use the collection list provided as arguments to the script and check that they are valid collections.
def validate_collection_list(collection_list):
    if len(collection_list) == 0:
        collection_list = COLLECTIONS_CONFIG['collection'].to_list()
    else:
        ## Verify the collections provided as arguments to the script are valid.
        for collection in collection_list:
            row = COLLECTIONS_CONFIG.filter(pl.col('collection') == collection)
            if row.is_empty() or not row['in_si'][0]:
                print(f"Collection {collection} not found in collections configuration file.")
                exit(1)    
    
    print(f"Collections to be processed: {collection_list}.")
    return collection_list

## Read the configuration files into global dataframes.
def read_configurations():
    global COLLECTIONS_CONFIG, SITES_CONFIG

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

## Main function to execute the script.
## It initializes the data structures by reading from the pre-generated list of collections.
## It then loops through the list of collections and queries the ams service for each collection.
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
    if len(sys.argv) == 1 and sys.argv[0] in ['--help', '-h']:
        print(f"Usage: {sys.argv[0]} [collection1 collection2 ...]")
        print(f"       {sys.argv[0]} <-h || --help>")
        exit(0)

    ## Reaed all configuration files into global dataframes.
    read_configurations()

    ## Remove ams_maq collections from the collections configuration dataframe.
    COLLECTIONS_CONFIG = COLLECTIONS_CONFIG.filter(pl.col('ams_site') != 'ams_maq')
    print(f"Warning: MAQ collections are not supported and will be skipped.")

    ## Create the list of collections to verify, either from the list provided on the command line or from the configuration file.
    ## MAQ collections are not supported.
    collection_list = validate_collection_list(sys.argv[1:])

    ## Creat a subdirectory for the output files if it does not exist.
    try:
        if not os.path.exists(OUTPUT_DIRECTORY):
            os.makedirs(OUTPUT_DIRECTORY)
        os.chdir(OUTPUT_DIRECTORY)
    except Exception as e:
        print(f"Error creating output directory {OUTPUT_DIRECTORY}: {e}")
        exit(1)
    
    ## Now loop though the list of collections.
    for collection in collection_list:
        print(f"Processing collection {collection}.")
        PROCESSING_START_TIME = datetime.now(timezone.utc)
        query_collection(collection)
        process_collection(collection)
        write_query_results(collection)
    print("All collections processed.")    
    exit(0)