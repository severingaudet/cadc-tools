import pandas as pd
import os.path

# This script generates a summary of the number of collection-instrument and field instances with null values.
# It reads pre-generated CSV files containing collection-instrument counts for observations and planes,
# and a list of field names to be checked. It then processes each field, calculates the number of collection-instrument
# and field instances with null values, and writes the results to a summary CSV file.
#

if __name__ == "__main__":
    obs_filename = "config/collInstrTotalObs.csv"
    planes_filename = "config/collInstrTotalPlanes.csv"
    fields_filename = "config/fieldNames.csv"
    summary_filename = "collInstrByField/sumCollInstrByField.csv"

#
# Initialize the data structures for pre-generated collection-instrument counts for observations.
#
    array_coll_instr_obs = pd.read_csv(obs_filename)
    num_coll_instr_obs = len(array_coll_instr_obs)
    print(f"Number of collection-instrument in {obs_filename}: {num_coll_instr_obs}")
    sum_instances_coll_instr_obs = array_coll_instr_obs['num_instances'].sum()
    print(f"Sum of instances of collection-instrument in {obs_filename}: {sum_instances_coll_instr_obs}")

#
# Initialize the data structures for pre-generated collection-instrument counts for planes.
#
    array_coll_instr_planes = pd.read_csv(planes_filename)
    num_coll_instr_planes = len(array_coll_instr_planes)
    print(f"Number of collection-instrument in {planes_filename}: {num_coll_instr_planes}")
    sum_instances_coll_instr_planes = array_coll_instr_planes['num_instances'].sum()
    print(f"Sum of instances of collection-instrument in {planes_filename}: {sum_instances_coll_instr_planes}")
#
# Initialize the summary output file with the CSV header.
#
    with open(summary_filename, 'w') as f:
        f.write("Field name,number of collection-instrument,collection-instrument with null values,percentage of collection-instrument with null values,number of field instances,field instances with null values,percentage of field instances with null values\n")

#
# Initialize lists of fields to be checked for output.
#
    field_names = pd.read_csv(fields_filename)
    num_fields = len(field_names)
    print(f"Number of fields in {fields_filename}: {num_fields}")
#
# Now loop through the the list of field and process the corresponding output files.
#
    for index, row in field_names.iterrows():
        field = row['field_name']
        field_filename = f"collInstrByField/{field}.csv"

        if os.path.isfile(field_filename):
#            print(f"Processing field {field} from {field_filename}")
            results = pd.read_csv(field_filename)
            field_coll_instr = len(results)
#            print(f"Number of collection-instrument for {field}: {field_coll_instr}")
            field_instances = results['num_null'].sum()
#            print(f"Total instances for {field}: {field_instances}")

            if field.startswith("caom2.Observation."):
                num_coll_instr = num_coll_instr_obs
                sum_instances_coll_instr = sum_instances_coll_instr_obs
            elif field.startswith("caom2.Plane."):
                num_coll_instr = num_coll_instr_planes
                sum_instances_coll_instr = sum_instances_coll_instr_planes
            else:
                print(f"Unknown field {field}")
                exit(255)

            percentage_null_coll_instr = field_coll_instr * 100 / num_coll_instr
            percentage_null_instances = field_instances * 100 / sum_instances_coll_instr
#            print(f"{field},{num_coll_instr},{field_coll_instr},{percentage_null_coll_instr:.2f},{sum_instances_coll_instr},{field_instances},{percentage_null_instances:.2f}")
            with open(summary_filename, 'a') as f:
                f.write(f"{field},{num_coll_instr},{field_coll_instr},{percentage_null_coll_instr:.2f},{sum_instances_coll_instr},{field_instances},{percentage_null_instances:.2f}\n")    



