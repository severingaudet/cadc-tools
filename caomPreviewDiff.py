from datetime import datetime, timezone
from pathlib import Path
import polars as pl
import requests
import os
import sys

CERT_FILENAME = f"{Path.home()}/.ssl/cadcproxy.pem"
OUTPUT_DIRECTORY = "previewDiff_reports"
OUTPUT_FILENAME_ROOT = "previewDiff"

MAPPINGS_CONFIG = pl.DataFrame()
COLLECTIONS_CONFIG = pl.DataFrame()
SITES_CONFIG = pl.DataFrame()

PROFILE_TEXT = "PROFILE"

## Format a duration as HH:MM:SS
def format_duration(duration):
    total_seconds = int(duration.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02}:{minutes:02}:{seconds:02}"

## Query the ams repository service for the specified collection.
def query_ams_service(ams_url, query, schema):
    ams_url_sync = ams_url + "/sync"
    data_list = {"LANG": "ADQL", "RESPONSEFORMAT": "CSV", "QUERY": query}

    try:
        # Make the POST request with a streaming response and a 2 hour timeout
        with requests.post(ams_url_sync, data=data_list, allow_redirects=True, cert=CERT_FILENAME, stream=True, timeout=7200) as response:
            response.raise_for_status()  # Raise an error for bad status codes
            # Read the raw CSV response into a Polars DataFrame
            query_result = pl.read_csv(response.raw, schema_overrides=schema)
    except requests.exceptions.HTTPError as e:
        print(f"{datetime.now(timezone.utc)} HTTP Error: {e}")
        exit(1)
    except requests.exceptions.RequestException as e:
        print(f"{datetime.now(timezone.utc)} Other Request Error: {e}")
        exit(1)

    return query_result

def query_collection(collection):

    start_time = datetime.now(timezone.utc)

    ## First determine which ams_site and ams_url to use for the given collection
    row = COLLECTIONS_CONFIG.filter(pl.col('collection') == collection)
    ams_site = row['ams_site'][0]
    site_row = SITES_CONFIG.filter(pl.col('site_name') == ams_site)
    if site_row.is_empty():
        print(f"Site {ams_site} for collection {collection} not found in sites configuration file.")
        exit(1)
    ams_url = site_row['site_url'][0]

    ## Create query string
    plane_artifact_type_query = f"""
            select O.collection, O.observationID, O.instrument_name, P.planeID, P.dataProductType, P.maxLastModified,
                case when A.productType = 'preview' then 1 end as preview,
                case when A.productType = 'thumbnail' then 1 end as thumbnail,
                case when A.productType = 'science' then 1 end as science,
                case when A.productType = 'calibration' then 1 end as calibration
            from caom2.Observation as O join caom2.Plane as P on O.obsID = P.obsID join caom2.Artifact as A on P.planeID = A.planeID
            where O.collection = '{collection}' and (P.quality_flag is null or P.quality_flag != 'junk')
        """.replace('\n', ' ')
    
    ## Define the schema for the dataframe to be returned.
    plane_artifact_type_schema = {
        "collection": str,
        "observationID": str,
        "instrument_name": str,
        "planeID": str,
        "dataProductType": str,
        "maxLastModified": str,
        "preview": pl.Int64,
        "thumbnail": pl.Int64,
        "science": pl.Int64,
        "calibration": pl.Int64
    }

    ## Query the ams service for the collection.
    plane_artifact_type_df = query_ams_service(ams_url, plane_artifact_type_query, plane_artifact_type_schema) 

    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time

    return plane_artifact_type_df, duration

def write_intro(f, collection, collection_start_time, query_duration, instrument_dataProductType_df):
    try:
        f.write(f"Query results for collection {collection}\n")
        f.write(f"\n")
        f.write(f"Processing began on {collection_start_time.strftime('%Y-%m-%dT%H:%M:%S')} UTC\n")
        f.write(f"Total query time: {format_duration(query_duration)}\n")
        f.write(f"\n")

        if (len(instrument_dataProductType_df) == 0):
            f.write(f"No planes with preview or thumbnail artifacts found for collection {collection}.\n")
            f.flush()
        else:        
            f.write(f"Number of instrument_name, dataProductType, science, calibration combinations with at least a preview or a thumbnail: {len(instrument_dataProductType_df)}\n")
            instrument_dataProductType_df.write_csv(f, include_header=True)
            f.write(f"Number of planes with at least a preview or a thumbnail: {instrument_dataProductType_df['num_planes'].sum()}\n")
            f.flush()
    except Exception as e:
        print(f"Error writing intro to output file: {e}")

    return

def write_inconsistent_planes(f, consistent_instrument_dataProductType_df, inconsistent_planes_df, text):
    try:
        f.write(f"\n")
        f.write(f"Number of consistent planes matching instrument_name, dataProductType, science, calibration for {text}: {consistent_instrument_dataProductType_df['num_planes'].sum()}\n")
        if len(consistent_instrument_dataProductType_df) > 0:
            consistent_instrument_dataProductType_df.write_csv(f, include_header=True)
        
        f.write(f"\n")
        f.write(f"Number of inconsistent planes matching instrument_name, dataProductType, science, calibration for {text}: {len(inconsistent_planes_df)}\n")
        if len(inconsistent_planes_df) > 0:
            inconsistent_planes_df.write_csv(f, include_header=True)
        f.flush()
    except Exception as e:
        print(f"Error writing results to output file: {e}")
        exit(1)
        
    return

def write_summary(f, collection, collection_start_time, num_consistent_both, num_inconsistent_both,
                  num_consistent_preview_only, num_inconsistent_preview_only, num_consistent_thumbnail_only,
                  num_inconsistent_thumbnail_only, query_duration, processing_duration, processing_end_time):

    try:
        message = f"summary,collection,collection_start_time,num_planes_consistent_both,num_planes_inconsistent_both,num_planes_consistent_preview_only,num_planes_inconsistent_preview_only,num_planes_consistent_thumbnail_only,num_planes_inconsistent_thumbnail_only,query_duration,processing_duration,processing_end_time"
        f.write(f"\n{message}\n")
        message = f"SUMMARY,{collection},{collection_start_time.strftime('%Y-%m-%dT%H:%M:%S')},{num_consistent_both},{num_inconsistent_both},{num_consistent_preview_only},{num_inconsistent_preview_only},{num_consistent_thumbnail_only},{num_inconsistent_thumbnail_only},{format_duration(query_duration)},{format_duration(processing_duration)},{processing_end_time.strftime('%Y-%m-%dT%H:%M:%S')}\n"
        f.write(f"{message}\n")
        f.flush()
        print(message)
    except Exception as e:
        print(f"Error writing summary to output file: {e}")

    return

def find_inconsistent_planes(planes_df, instrument_dataProductType_df, constraint1, constraint2, category):

    consistent_planes_df = pl.DataFrame()
    inconsistent_planes_df = pl.DataFrame()

    ## Create a dataframe of planes matching constraint1
    consistent_planes_df = planes_df.filter(
        constraint1
        ).select(["collection", "observationID", "instrument_name", "planeID", "dataProductType", "science", "calibration"]
        ).sort(["collection", "observationID", "instrument_name", "planeID", "dataProductType", "science", "calibration"] )
    
    ## Create a dataframe of unique instrument_name, dataProductType combinations from the consistent planes.
    consistent_instrument_dataProductType_df = consistent_planes_df.group_by(["collection", "instrument_name", "dataProductType", "science", "calibration"]    
        ).agg( pl.len().alias("num_planes")
        ).sort( ["collection", "instrument_name", "dataProductType", "science", "calibration"] )
    ## Add a category column at as the first column of the dataframe and set the values of the category column to CONSISTENT
    category_column = pl.Series("category", ["CONSISTENT"] * len(consistent_instrument_dataProductType_df))
    consistent_instrument_dataProductType_df.insert_column(0, category_column)

#    print( f"Number of instrument_name, dataProductType, science, calibration combinations with {constraint1}: {len(consistent_instrument_dataProductType_df)}" )
#    print( f"consistent_instrument_dataProductType_df: {consistent_instrument_dataProductType_df}" )
#    print( f"Number of consistent planes: {consistent_instrument_dataProductType_df['num_planes'].sum()}" )
#    print( f"Number of consistent planes: {len(consistent_planes_df)}" )
    
    ## If length of constrainted_instrument_dataProductType_df is non-zero, add a category column and then find all planes with the same 
    ## combination of instrument_name, dataProductType from the instrument_dataProductType_df
    if len(consistent_instrument_dataProductType_df) > 0:
        ## Remove the consistent planes from the planes_df to find inconsistent planes.
        planes_df = planes_df.join(
            consistent_planes_df,
            on=["collection", "observationID", "instrument_name", "planeID", "dataProductType"],
            how="anti"
        )
        
        ## Now find inconsistent planes matching the instrument_name, dataProductType combinations
        inconsistent_planes_df = planes_df.join(
            consistent_instrument_dataProductType_df,
            on=["instrument_name", "dataProductType", "science", "calibration"],
            how="inner"
        ).filter(constraint2
        ).select(["collection", "observationID", "instrument_name", "planeID", "dataProductType", "maxLastModified", "science", "calibration"]
        ).sort( ["collection", "observationID", "instrument_name", "planeID", "dataProductType"] )
#        print( f"Number of inconsistent planes : {len(inconsistent_planes_df)}" )

        if ( len(inconsistent_planes_df) > 0 ):
            ## Add a category column at as th first column of the dataframe and set the values of the category column to category
            category_column = pl.Series("category", [category] * len(inconsistent_planes_df))
            inconsistent_planes_df.insert_column(0, category_column)

            ## Remove inconsistent planes from the planes_df
            planes_df = planes_df.join(
                inconsistent_planes_df,
                on=["collection", "observationID", "instrument_name", "planeID", "dataProductType"],
                how="anti"
            )

#        print( f"Initial instrument_name, dataProductType, science, calibration combinations to process: {len(instrument_dataProductType_df)}" )
#        print( f"instrument_dataProductType_df:\n{instrument_dataProductType_df}" )
#        print( f"Number of consistent instrument_name, dataProductType, science, calibration combinations: {len(consistent_instrument_dataProductType_df)}" )
#        print( f"consistent_instrument_dataProductType_df:\n{consistent_instrument_dataProductType_df}" )

        ## Remove the instrument_name, dataProductType combination from the instrument_dataProductType_df
        instrument_dataProductType_df = instrument_dataProductType_df.join(
            consistent_instrument_dataProductType_df,
            on=["instrument_name", "dataProductType", "science", "calibration"],
            how="anti"
        )
#    print( f"Remaining instrument_name, dataProductType, science, calibration combinations to process: {len(instrument_dataProductType_df)}" )
#    print( f"instrument_dataProductType_df:\n{instrument_dataProductType_df}" )
    return planes_df, consistent_instrument_dataProductType_df, inconsistent_planes_df, instrument_dataProductType_df
"""
Process a specific preview combination with the provided constraints in querying the planes dataframe.
"""
def process_preview_combination(f, planes_df, instrument_dataProductType_df, constraint1, constraint2, category):

    if (len(instrument_dataProductType_df ) > 0):
        planes_df, consistent_instrument_dataProductType_df, inconsistent_planes_df, instrument_dataProductType_df = \
                find_inconsistent_planes(planes_df, instrument_dataProductType_df, constraint1, constraint2, category)
        write_inconsistent_planes(f, consistent_instrument_dataProductType_df,inconsistent_planes_df, category)
        num_consistent_planes = consistent_instrument_dataProductType_df['num_planes'].sum()
        num_inconsistent_planes = len(inconsistent_planes_df)
    else:
        num_consistent_planes = 0
        num_inconsistent_planes = 0

    return planes_df, num_consistent_planes, num_inconsistent_planes, instrument_dataProductType_df


"""
Process the query results into lists of inconsistent files. The constraints are:
1. For an instrument, dataProductType combination that has both preview and thumbnail artifacts, any plane with a matching
   instrument, dataProductType that does not have both preview and thumbnail artifacts is inconsistent.
2. For an instrument, dataProductType combination that has only preview artifacts and never both, any plane with a matching
   instrument, dataProductType that does not have a preview artifact is inconsistent.
3. For an instrument, dataProductType combination that has only thumbnail artifacts and never both, any plane with a matching
   instrument, dataProductType that does not have a thumbnail artifact is inconsistent.
"""
def process_collection(collection, collection_start_time, query_duration, plane_artifact_type_df):

    num_consistent_planes_with_both = 0
    num_inconsistent_planes_with_both = 0
    num_consistent_planes_with_preview_only = 0
    num_inconsistent_planes_with_preview_only = 0
    num_consistent_planes_with_thumbnail_only = 0
    num_inconsistent_planes_with_thumbnail_only = 0
    
    processing_start_time = datetime.now(timezone.utc)
    
    ## Merge rows by with the same collection, observationID, planeID, maxLastModified and set the auxiliary, calibration, info, noise, preview, science, thumbnail, weight columns to 1 if any one row in the plane is > 0.
    planes_df = plane_artifact_type_df.group_by( ["collection", "observationID", "instrument_name", "planeID", "dataProductType", "maxLastModified"] ).agg(
        [pl.col("preview").min().alias("preview"),
        pl.col("thumbnail").min().alias("thumbnail"),
        pl.col("science").min().alias("science"),
        pl.col("calibration").min().alias("calibration")]
    ).sort( ["collection", "observationID", "instrument_name", "planeID", "dataProductType", "maxLastModified"] )
    
    ## Next, set nulls to "None" for instrument_name and dataProductType and science, calibration, preview and thumbnail to 0.
    #  This will allow joins and aggregations to work correctly.
    planes_df = planes_df.with_columns(
        pl.col("instrument_name").fill_null("None"),
        pl.col("dataProductType").fill_null("None"),
        pl.col("science").fill_null(0),
        pl.col("calibration").fill_null(0),
        pl.col("preview").fill_null(0),
        pl.col("thumbnail").fill_null(0)
    )
#    print( f"Distinct planes after artifact aggregation: {len(planes_df)}" )

    ## Create a dataframe of unique instrument_name, dataProductType combinations having either a preview or a thumbnail artifact.
    instrument_dataProductType_df = planes_df.filter(
        (pl.col("preview") == 1) | (pl.col("thumbnail") == 1)
        ).group_by( ["collection", "instrument_name", "dataProductType", "science", "calibration"]
        ).agg( pl.len().alias("num_planes")
        ).sort( ["collection", "instrument_name", "dataProductType", "science", "calibration"] )

    ## Add a category column at as the first column of the dataframe and set the values of the category column to PROFILE
    category_column = pl.Series("category", ["PROFILE"] * len(instrument_dataProductType_df))
    instrument_dataProductType_df.insert_column(0, category_column)
    
#    print( f"Number of instrument_name, dataProductType, science, calibration combinations with at least a preview or a thumbnail: {len(instrument_dataProductType_df)}" )
#    print( f"instrument_dataProductType_df:\n{instrument_dataProductType_df}" )
#    print( f"Number of planes with at least a preview or a thumbnail: {instrument_dataProductType_df['num_planes'].sum()}" )
    
     ## Open output file for writing and write the intro.
    filename = f"{OUTPUT_FILENAME_ROOT}_{collection}.csv"
    print(f"Writing processing results to {filename}.")
    try:
        f = open(filename, 'w')
        write_intro(f, collection, collection_start_time, query_duration, instrument_dataProductType_df)
    except Exception as e:
        print(f"Error opening or writing intro to {filename}: {e}")
        
    ## Process and output each of the three cases in turn, updating the instrument_dataProductType_df each time.
    planes_df, num_consistent_planes_with_both, num_inconsistent_planes_with_both, instrument_dataProductType_df = \
        process_preview_combination(f, planes_df, instrument_dataProductType_df,
                                    (pl.col("preview") == 1) & (pl.col("thumbnail") == 1),
                                    (pl.col("preview") == 0) | (pl.col("thumbnail") == 0),
                                    "INCONSISTENT_FOR_BOTH")
    planes_df, num_consistent_planes_with_preview_only, num_inconsistent_planes_with_preview_only, instrument_dataProductType_df = \
        process_preview_combination(f, planes_df, instrument_dataProductType_df,
                                    (pl.col("preview") == 1),
                                    (pl.col("preview") == 0),
                                    "INCONSISTENT_FOR_PREVIEW_ONLY")
    planes_df, num_consistent_planes_with_thumbnail_only, num_inconsistent_planes_with_thumbnail_only, instrument_dataProductType_df = \
        process_preview_combination(f, planes_df, instrument_dataProductType_df,
                                    (pl.col("thumbnail") == 1),
                                    (pl.col("thumbnail") == 0),
                                    "INCONSISTENT_FOR_THUMBNAIL_ONLY")
            
    if (len(instrument_dataProductType_df ) > 0):
        print( "Error - instrument_dataProductType should be empty" )
        print( instrument_dataProductType_df )
        exit(1)

    processing_end_time = datetime.now(timezone.utc)
    processing_duration = processing_end_time - processing_start_time 
    write_summary(f, collection, collection_start_time, num_consistent_planes_with_both, num_inconsistent_planes_with_both,
                  num_consistent_planes_with_preview_only, num_inconsistent_planes_with_preview_only, num_consistent_planes_with_thumbnail_only,
                  num_inconsistent_planes_with_thumbnail_only, query_duration, processing_duration, processing_end_time)
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
    
    print("Collections to be processed: ", end="")
    print(*collection_list)
    return collection_list

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
        collection_start_time = datetime.now(timezone.utc)
        plane_artifact_type_df, query_duration = query_collection(collection)
        process_collection(collection, collection_start_time, query_duration, plane_artifact_type_df)
        
        ## Explicitly delete the dataframe to free up memory if running through a list of collections.
        del plane_artifact_type_df

    print("All collections processed.")    
    exit(0)