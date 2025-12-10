from datetime import datetime, timezone
from pathlib import Path
import polars as pl
import requests
import os
import sys

CERT_FILENAME = f"{Path.home()}/.ssl/cadcproxy.pem"
OUTPUT_DIRECTORY = "typeProfiles_reports"
OUTPUT_FILENAME_ROOT = "typeProfiles"
QUERY_DURATION = 0
PROCESS_RESULTS_DURATION = 0
START_TIME = datetime.now(timezone.utc)

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

## Format a duration as HH:MM:SS
def format_duration(duration):
    total_seconds = int(duration.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"

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
    global QUERY_DURATION
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
            select '{TYPE_TEXT}' as category, O.collection, O.instrument_name, O.intent, P.planeID, P.dataProductType,
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
            from caom2.Observation as O join caom2.Plane as P on O.obsID = P.obsID join caom2.Artifact as A on P.planeID = A.planeID
            where O.collection = '{collection}' and (P.quality_flag is null or P.quality_flag != 'junk')
        """.replace('\n', ' ')
    PLANE_ARTIFACT_TYPES_DF = query_ams_service(ams_url, query_plane_artifact_types)
    
    print( f"Number of artifacts: {len(PLANE_ARTIFACT_TYPES_DF)}" )

    end_time = datetime.now(timezone.utc)
    QUERY_DURATION = end_time - start_time

    return

def write_no_planes(f, filename, collection, df):
    try:
        if len(df) > 0:
            message = f"List of {len(df)} NO_PLANES found"
            f.write(f"\n{message}\n")
            df.write_csv(f, separator='\t')
    except Exception as e:
        print(f"Error writing results to {filename}: {e}")
        exit(1)
        
    return

def write_no_artifacts(f, filename, collection, df):
    try:
        if len(df) > 0:
            message = f"List of {len(df)} NO_ARTIFACTS found"
            f.write(f"\n{message}\n")
            df.write_csv(f, separator='\t')
    except Exception as e:
        print(f"Error writing results to {filename}: {e}")
        exit(1)
        
    return

def write_profile(f, filename, collection, df):
    try:
        if len(df) > 0:
            message = f"List of {len(df)} TYPE combinations found"
            f.write(f"\n{message}\n")
            df.write_csv(f, separator='\t')
    except Exception as e:
        print(f"Error writing results to {filename}: {e}")
        exit(1)
        
    return

def write_tsv(f, filename, text, df):
    try:
        if len(df) > 0:
            message = f"List of {len(df)} {text} found"
            f.write(f"\n{message}\n")
            df.write_csv(f, separator='\t')
    except Exception as e:
        print(f"Error writing results to {filename}: {e}")
        exit(1)
        
    return

## Write the query results to the output file.
def write_processing_results(collection):
    global PLANE_ARTIFACT_TYPES_DF
    global DISTINCT_PLANE_ARTIFACT_TYPES_DF
    global NO_ARTIFACTS_DF
    global NO_PLANES_DF
    global JUNK_PLANES_DF
    global ALL_TYPES_DF

    filename = f"{OUTPUT_FILENAME_ROOT}_{collection}.tsv"
    print(f"Writing query results to {filename}.")
    try:
        with open(filename, 'w') as f:
            write_start_time = datetime.now(timezone.utc)
            f.write(f"Query results for collection {collection}\n")
            f.write(f"\n")
            f.write(f"Start time\t{START_TIME.strftime('%Y-%m-%dT%H:%M:%S')} UTC\n")
            f.write(f"AMS query duration\t{format_duration(QUERY_DURATION)}\n")
            f.write(f"Process results duration\t{format_duration(PROCESS_RESULTS_DURATION)}\n")
            f.write(f"\n")

            f.write(f"Observations with no associated planes\t{len(NO_PLANES_DF)}\n")
            f.write(f"Planes flagged as junk\t{len(JUNK_PLANES_DF)}\n")
            f.write(f"Planes with no artifacts\t{len(NO_ARTIFACTS_DF)}\n")
            f.write(f"Number of planes\t{len(DISTINCT_PLANE_ARTIFACT_TYPES_DF)}\n")
            f.write(f"Number of artifacts\t{len(PLANE_ARTIFACT_TYPES_DF)}\n")


            write_tsv( f, filename, NO_PLANE_TEXT, NO_PLANES_DF )
            write_tsv( f, filename, NO_ARTIFACT_TEXT, NO_ARTIFACTS_DF )
            write_tsv( f, filename, TYPE_TEXT, ALL_TYPES_DF )

            ## Finally, write the summary message
            write_end_time = datetime.now(timezone.utc)
            write_duration = write_end_time - write_start_time
            end_time = datetime.now(timezone.utc)
            total_duration = end_time - START_TIME
            
            message = f"Category\tCollection\tStart time\tObservations with no planes\tPlanes with no artifacts\tJunk planes\tPlanes to by checked\tArtifacts to be checked\tNum profile combinations\tQuery duration\tProcessing results duration\tWrite duration\tTotal duration\tEnd time"
            f.write(f"\n{message}\n")
            message = f"SUMMARY\t{collection}\t{START_TIME.strftime('%Y-%m-%dT%H:%M:%S')}\t{len(NO_PLANES_DF)}\t{len(NO_ARTIFACTS_DF)}\t{len(JUNK_PLANES_DF)}\t{len(DISTINCT_PLANE_ARTIFACT_TYPES_DF)}\t{len(PLANE_ARTIFACT_TYPES_DF)}\t{len(ALL_TYPES_DF)}\t{format_duration(QUERY_DURATION)}\t{format_duration(PROCESS_RESULTS_DURATION)}\t{format_duration(write_duration)}\t{format_duration(total_duration)}\t{end_time.strftime('%Y-%m-%dT%H:%M:%S')}\n"
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

## Process the query results to create the type profile of the collection.
def process_query_results():
    global PLANE_ARTIFACT_TYPES_DF, ALL_TYPES_DF, DISTINCT_PLANE_ARTIFACT_TYPES_DF, PROCESS_RESULTS_DURATION

    start_time = datetime.now(timezone.utc)

    ## Cast the productType columns to Int64 to allow aggregation
    PLANE_ARTIFACT_TYPES_DF = PLANE_ARTIFACT_TYPES_DF.with_columns(
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

    ## Merge rows by with the same collection, observationID, planeID, maxLastModified and set the productType columns to 1 if any one row in the plane is > 0.
    DISTINCT_PLANE_ARTIFACT_TYPES_DF = PLANE_ARTIFACT_TYPES_DF.group_by( ["category", "collection", "instrument_name", "intent", "planeID", "dataProductType"] ).agg(
        [pl.col("this").min().alias("this"),
        pl.col("science").min().alias("science"),
        pl.col("calibration").min().alias("calibration"),
        pl.col("preview").min().alias("preview"),
        pl.col("thumbnail").min().alias("thumbnail"),
        pl.col("auxiliary").min().alias("auxiliary"),
        pl.col("bias").min().alias("bias"),
        pl.col("coderived").min().alias("coderived"),
        pl.col("dark").min().alias("dark"),
        pl.col("documentation").min().alias("documentation"),
        pl.col("error").min().alias("error"),
        pl.col("flat").min().alias("flat"),
        pl.col("info").min().alias("info"),
        pl.col("noise").min().alias("noise"),
        pl.col("preview_image").min().alias("preview_image"),
        pl.col("preview_plot").min().alias("preview_plot"),
        pl.col("weight").min().alias("weight")]
    )
    print( f"All planes after merging artifacts: {len(DISTINCT_PLANE_ARTIFACT_TYPES_DF)}" )

    ## List distinct instrument_name, intent, dataProductTypes and proeductType combinations. Insert the number of planes for each combination into the dataframe after the dataProductType column.
    ALL_TYPES_DF = DISTINCT_PLANE_ARTIFACT_TYPES_DF.group_by(
        ["category", "collection", "instrument_name", "intent", "dataProductType", "this", "science", "calibration", "preview", "thumbnail", "auxiliary", "bias", "coderived",
            "dark", "documentation", "error", "flat", "info", "noise", "preview_image", "preview_plot", "weight"]
        ).agg(pl.len().alias("num_planes")
    ).sort(
        by=["category", "collection", "instrument_name", "intent", "dataProductType", "this", "science", "calibration", "preview", "thumbnail", "auxiliary", "bias", "coderived",
            "dark", "documentation", "error", "flat", "info", "noise", "preview_image", "preview_plot", "weight"],
        descending=[False, False, False, True, False, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True, True]
    ).select([
        "category", "collection", "instrument_name", "intent", "dataProductType", "num_planes",
        "this", "science", "calibration", "preview", "thumbnail", "auxiliary", "bias", "coderived",
        "dark", "documentation", "error", "flat", "info", "noise", "preview_image", "preview_plot", "weight"
    ])
    print( f"Number of distinct combinations of plane and artifact types: {len(ALL_TYPES_DF)}" )

    end_time = datetime.now(timezone.utc)
    PROCESS_RESULTS_DURATION = end_time - start_time

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
            if row.is_empty():
                print(f"Collection {collection} not found in collections configuration file.")
                exit(1)    
    
    print("Collections to be processed: ", end="")
    print(*collection_list)

    return collection_list

## Read the configuration files into global dataframes.
def read_configurations():
    global COLLECTIONS_CONFIG, SITES_CONFIG

    ## Read static configuration file for mapping collections to AMS sites.
    ## This file must contain the columns collection and ams_site.
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
    
    ## Determine where the collection monitoring directory is located and change to that directory.
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
        START_TIME = datetime.now(timezone.utc)
        query_collection(collection)
        process_query_results()
        write_processing_results(collection)
    print("All collections processed.")    
    exit(0)