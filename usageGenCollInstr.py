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

def calculate_percentages(filename, array_coll_instr):
    cp_results = pd.read_csv(filename)

    if len(cp_results) > 0:
        cp_results['num_instances'] = cp_results.apply(lambda row: find_num_instances(row['collection'], row['instrument_name'], array_coll_instr), axis=1)
        cp_results['percentage_null'] = cp_results.apply(lambda row: row['num_null'] * 100 / row['num_instances'], axis=1)
        cp_results['num_instances'] = cp_results['num_instances'].round(2)
        cp_results['percentage_null'] = cp_results['percentage_null'].round(2)

        cp_results.to_csv(filename, index=False)

def find_num_instances(collection, instrument_name, array_coll_instr):
    print(f"fni collection = {collection}, instrument_name = {instrument_name}")
    row = array_coll_instr[(array_coll_instr['collection'] == collection) & (array_coll_instr['instrument_name'] == instrument_name)]
    return row['num_instances'].values[0]

def calculate_summary(field, filename, num_coll_instr, sum_instances_coll_instr):
    results = pd.read_csv(filename)
    field_coll_instr = len(results)
    print(f"Number of collection/instrument for {field}: {field_coll_instr}")
    field_instances = results['num_null'].sum()
    print(f"Total instances for {field}: {field_instances}")

    percentage_null_coll_instr = field_coll_instr * 100 / num_coll_instr
    percentage_null_instances = field_instances * 100 / sum_instances_coll_instr
    print(f"Summary,{field},{num_coll_instr},{field_coll_instr},{percentage_null_coll_instr:.2f},{sum_instances_coll_instr},{field_instances},{percentage_null_instances:.2f}")
    with open(filename, 'a') as f:
        f.write(",Field name, number of collection/instrument, collection/instrument with null values, percentage of collection/instrument with null values, number of field instances, field instances with null values, percentage of field instances with null values\n")
        f.write(f"Summary,{field},{num_coll_instr},{field_coll_instr},{percentage_null_coll_instr:.2f},{sum_instances_coll_instr},{field_instances},{percentage_null_instances:.2f}\n")

def process_observation_field(field, array_coll_instr_obs, num_coll_instr_obs, sum_instances_coll_instr_obs):
    filename = f"byField/{field}.csv"
    query = f"""select collection, instrument_name, count(*) as num_null
        from caom2.Observation
        where instrument_name is not null and instrument_name != 'NULL' and {field} is null
        group by collection, instrument_name
        order by collection, instrument_name"""
    execute_query(query, field, filename)
    calculate_percentages(filename, array_coll_instr_obs)
#    calculate_summary(field, filename, num_coll_instr_obs, sum_instances_coll_instr_obs)

def process_plane_field(field, array_coll_instr_planes, num_coll_instr_planes, sum_instances_coll_instr_planes):
    filename = f"byField/{field}.csv"
    query = f"""select collection, instrument_name, count(*) as num_null
        from caom2.Observation join caom2.Plane on caom2.Observation.obsID = caom2.Plane.obsID
        where instrument_name is not null and instrument_name != 'NULL' and {field} is null
        group by collection, instrument_name
        order by collection, instrument_name"""
    execute_query(query,field, filename)
    calculate_percentages(filename, array_coll_instr_planes)
#    calculate_summary(field, filename, num_coll_instr_planes, sum_instances_coll_instr_planes)


if __name__ == "__main__":
    obs_filename = "collInstrTotalObs.csv"
    planes_filename = "collInstrTotalPlanes.csv"
    fields_filename = "fieldNames.csv"

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", default="caom2.Observation.accMetaChecksum", help="Start field")
    parser.add_argument("-e", "--end", default="caom2.Plane.time_sampleSize", help="End field")
    args = parser.parse_args()

    start_field = args.start
    end_field = args.end
    started = False

#
# Initialize the data structures for pre-generated collection/instrument counts for observations.
#
    array_coll_instr_obs = pd.read_csv(obs_filename)
    num_coll_instr_obs = len(array_coll_instr_obs)
    print(f"Number of collection/instrument in {obs_filename}: {num_coll_instr_obs}")
    sum_instances_coll_instr_obs = array_coll_instr_obs['num_instances'].sum()
    print(f"Sum of instances of collection/instrument in {obs_filename}: {sum_instances_coll_instr_obs}")

#
# Initialize the data structures for pre-generated collection/instrument counts for planes.
#
    array_coll_instr_planes = pd.read_csv(planes_filename)
    num_coll_instr_planes = len(array_coll_instr_planes)
    print(f"Number of collection/instrument in {planes_filename}: {num_coll_instr_planes}")
    sum_instances_coll_instr_planes = array_coll_instr_planes['num_instances'].sum()
    print(f"Sum of instances of collection/instrument in {planes_filename}: {sum_instances_coll_instr_planes}")

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
                process_observation_field(field, array_coll_instr_obs, num_coll_instr_obs, sum_instances_coll_instr_obs)
            elif field.startswith("caom2.Plane."):
                print(f"processPlaneField {field}")
                process_plane_field(field, array_coll_instr_planes, num_coll_instr_planes, sum_instances_coll_instr_planes)
            else:
                print(f"Unknown field {field}")
                exit(255)

        if field == end_field:
            exit(0)
