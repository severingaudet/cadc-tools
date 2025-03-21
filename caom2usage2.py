from astroquery.cadc import Cadc
import argparse
import pandas as pd

def execute_query(query, field, filename, total_coll_instr, total_instances):
    job = service.create_async(query)
    job.run().wait()
    job.raise_if_error()
    results = job.fetch_result().to_table().to_pandas()
    field_coll_instr = len(results)
    print(f"Number of collection/instrument for {field}: {field_coll_instr}")
    field_instances = results['num_null'].sum()
    print(f"Total instances for {field}: {field_instances}")
    filename = f"{field}.csv"
    results.to_csv(filename, index=False)

    percentage_null_coll_instr = field_coll_instr * 100 / total_coll_instr
    percentage_null_instances = field_instances * 100 / total_instances
    print(f"Summary,{field},{total_coll_instr},{field_coll_instr},{percentage_null_coll_instr:.2f},{total_instances},{field_instances},{percentage_null_coll_instr:.6f}")
    with open(filename, 'a') as f:
        f.write(f"Summary,{field},{total_coll_instr},{field_coll_instr},{percentage_null_coll_instr:.2f},{total_instances},{field_instances},{percentage_null_coll_instr:.6f}\n")

def process_observation_field(field,total_coll_instr,total_instances):
    filename = f"{field}.csv"
    query = f"""select collection, instrument_name, count(*) as num_null
        from caom2.Observation
        where {field} is null
        group by collection, instrument_name
        order by collection, instrument_name"""
    print(query)

    execute_query(query, field, filename, total_coll_instr, total_instances)

def process_plane_field(field, total_coll_instr, total_instances):
    filename = f"{field}.csv"
    query = f"""select collection, instrument_name, count(*) as num_null
        from caom2.Observation join caom2.Plane on caom2.Observation.obsID = caom2.Plane.obsID
        where {field} is null
        group by collection, instrument_name
        order by collection, instrument_name"""
    print(query)
    execute_query(query,field, filename, total_coll_instr, total_instances)

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
    coll_instr_obs = pd.read_csv(obs_filename)
    num_coll_instr_obs = len(coll_instr_obs)
    print(f"Number of collection/instrument in {obs_filename}: {num_coll_instr_obs}")
    sum_instances_coll_instr_obs = coll_instr_obs['num_instances'].sum()
    print(f"Sum of instances of collection/instrument in {obs_filename}: {sum_instances_coll_instr_obs}")

#
# Initialize the data structures for pre-generated collection/instrument counts for planes.
#
    coll_instr_planes = pd.read_csv(planes_filename)
    num_coll_instr_planes = len(coll_instr_planes)
    print(f"Number of collection/instrument in {planes_filename}: {num_coll_instr_planes}")
    sum_instances_coll_instr_planes = coll_instr_planes['num_instances'].sum()
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

    field_names = field_names.reset_index()  # make sure indexes pair with number of rows
    for index, row in field_names.iterrows():
        field = row['field_name']

        if field == start_field:
            started = True

        if started:
            if field.startswith("caom2.Observation."):
                print(f"processObservationField {field}")
                process_observation_field(field,num_coll_instr_obs,sum_instances_coll_instr_obs)
            elif field.startswith("caom2.Plane."):
                print(f"processPlaneField {field}")
                process_plane_field(field,num_coll_instr_planes,sum_instances_coll_instr_planes)
            else:
                print(f"Unknown field {field}")
                exit(255)

        if field == end_field:
            exit(0)
