from astroquery.cadc import Cadc
import argparse
import pandas as pd

def execute_query(query, field, filename):
    job = service.create_async(query)
    print(f"Job ID link for {field}: https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus/async/{job.job_id}")
    job.run().wait()
    job.raise_if_error()
    results = job.fetch_result().to_table().to_pandas()
    results.to_csv(filename, index=True)

def calculate_percentages(filename, array_collection):
    cp_results = pd.read_csv(filename)

    if len(cp_results) > 0:
        cp_results['num_instances'] = cp_results.apply(lambda row: find_num_instances(row['collection'], row['instrument_name'], array_collection), axis=1)
        cp_results['percentage_null'] = cp_results.apply(lambda row: row['num_null'] * 100 / row['num_instances'], axis=1)
        cp_results['num_instances'] = cp_results['num_instances'].round(2)
        cp_results['percentage_null'] = cp_results['percentage_null'].round(2)

        cp_results.to_csv(filename, index=False)

def find_num_instances(collection, array_collection):
    print(f"fni collection = {collection}")
    row = array_collection[(array_collection['collection'] == collection)]
    return row['num_instances'].values[0]

def calculate_summary(field, filename, num_collection, sum_instances_collection):
    results = pd.read_csv(filename)
    field_collection = len(results)
    print(f"Number of collection for {field}: {field_collection}")
    field_instances = results['num_null'].sum()
    print(f"Total instances for {field}: {field_instances}")

    percentage_null_collection = field_collection * 100 / num_collection
    percentage_null_instances = field_instances * 100 / sum_instances_collection
    print(f"Summary,{field},{num_collection},{field_collection},{percentage_null_collection:.2f},{sum_instances_collection},{field_instances},{percentage_null_instances:.2f}")
    with open(filename, 'a') as f:
        f.write(",Field name, number of collection, collection with null values, percentage of collection with null values, number of field instances, field instances with null values, percentage of field instances with null values\n")
        f.write(f"Summary,{field},{num_collection},{field_collection},{percentage_null_collection:.2f},{sum_instances_collection},{field_instances},{percentage_null_instances:.2f}\n")

def process_observation_field(field, array_collection_obs, num_collection_obs, sum_instances_collection_obs):
    filename = f"collectionByField/{field}.csv"
    query = f"""select collection, count(*) as num_null
        from caom2.Observation
        where instrument_name is not null and instrument_name != 'NULL' and {field} is null
        group by collection
        order by collection"""
    execute_query(query, field, filename)
    calculate_percentages(filename, array_collection_obs)
#    calculate_summary(field, filename, num_collection_obs, sum_instances_collection_obs)

def process_plane_field(field, array_collection_planes, num_collection_planes, sum_instances_collection_planes):
    filename = f"collectionByField/{field}.csv"
    query = f"""select collection, count(*) as num_null
        from caom2.Observation join caom2.Plane on caom2.Observation.obsID = caom2.Plane.obsID
        where instrument_name is not null and instrument_name != 'NULL' and {field} is null
        group by collection
        order by collection"""
    execute_query(query,field, filename)
    calculate_percentages(filename, array_collection_planes)
#    calculate_summary(field, filename, num_collection_planes, sum_instances_collection_planes)


if __name__ == "__main__":
    obs_filename = "collectionTotalObs.csv"
    planes_filename = "collectionTotalPlanes.csv"
    fields_filename = "fieldNames.csv"

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", default="caom2.Observation.accMetaChecksum", help="Start field")
    parser.add_argument("-e", "--end", default="caom2.Plane.time_sampleSize", help="End field")
    args = parser.parse_args()

    start_field = args.start
    end_field = args.end
    started = False

#
# Initialize the data structures for pre-generated collection counts for observations.
#
    array_collection_obs = pd.read_csv(obs_filename)
    num_collection_obs = len(array_collection_obs)
    print(f"Number of collection in {obs_filename}: {num_collection_obs}")
    sum_instances_collection_obs = array_collection_obs['num_instances'].sum()
    print(f"Sum of instances of collection in {obs_filename}: {sum_instances_collection_obs}")

#
# Initialize the data structures for pre-generated collection counts for planes.
#
    array_collection_planes = pd.read_csv(planes_filename)
    num_collection_planes = len(array_collection_planes)
    print(f"Number of collection in {planes_filename}: {num_collection_planes}")
    sum_instances_collection_planes = array_collection_planes['num_instances'].sum()
    print(f"Sum of instances of collection in {planes_filename}: {sum_instances_collection_planes}")

#
# Initialize lists of fields to be checked for null values.
#
    field_names = pd.read_csv(fields_filename)
    num_fields = len(field_names)
    print(f"Number of fields in {fields_filename}: {num_fields}")

#
# Now loop through the the list of field and process them.
#
    print(f"Execute from {start_field} to {end_field}")
    service = Cadc()

    for index, row in field_names.iterrows():
        field = row['field_name']

        if field == start_field:
            started = True

        if started:
            if field.startswith("caom2.Observation."):
                print(f"processObservationField {field}")
                process_observation_field(field, array_collection_obs, num_collection_obs, sum_instances_collection_obs)
            elif field.startswith("caom2.Plane."):
                print(f"processPlaneField {field}")
                process_plane_field(field, array_collection_planes, num_collection_planes, sum_instances_collection_planes)
            else:
                print(f"Unknown field {field}")
                exit(255)

        if field == end_field:
            exit(0)
