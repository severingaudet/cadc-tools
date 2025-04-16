import pandas as pd
import os.path

# This script generates a summary of the number of collection and field instances with null values.
# It reads pre-generated CSV files containing collection counts for observations and planes,
# and a list of field names to be checked. It then processes each field, calculates the number of collection
# and field instances with null values, and writes the results to a summary CSV file.
#

if __name__ == "__main__":

    ## Determine where the caom2usage directory is located and change to that directory.

    if os.path.isdir("/Users/gaudet_1/work/caom2usage"):
        os.chdir("/Users/gaudet_1/work/caom2usage")
    elif os.path.isdir("/arc/projects/CADC/caom2usage"):
        os.chdir("/arc/projects/CADC/caom2usage")
    else:
        print("Unable to determine the location of the caom2usage directory.")
        exit(1)

    obs_filename = "config/collectionTotalObs.csv"
    planes_filename = "config/collectionTotalPlanes.csv"
    fields_filename = "config/fieldNames.csv"
    summary_filename = "collectionByField/sumCollectionByField.csv"

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
# Initialize the summary output file with the CSV header.
#
    with open(summary_filename, 'w') as f:
        f.write("Field name,number of collections,collections with null values,percentage of collections with null values,number of field instances,field instances with null values,percentage of field instances with null values\n")

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
        field_filename = f"collectionByField/{field}.csv"

        if os.path.isfile(field_filename):
#            print(f"Processing field {field} from {field_filename}")
            results = pd.read_csv(field_filename)
            field_collection = len(results)
#            print(f"Number of collections for {field}: {field_collection}")
            field_instances = results['num_null'].sum()
#            print(f"Total instances for {field}: {field_instances}")

            if field.startswith("caom2.Observation."):
                num_collection = num_collection_obs
                sum_instances_collection = sum_instances_collection_obs
            elif field.startswith("caom2.Plane."):
                num_collection = num_collection_planes
                sum_instances_collection = sum_instances_collection_planes
            else:
                print(f"Unknown field {field}")
                exit(255)

            percentage_null_collection = field_collection * 100 / num_collection
            percentage_null_instances = field_instances * 100 / sum_instances_collection
#            print(f"{field},{num_collection},{field_collection},{percentage_null_collection:.2f},{sum_instances_collection},{field_instances},{percentage_null_instances:.2f}")
            with open(summary_filename, 'a') as f:
                f.write(f"{field},{num_collection},{field_collection},{percentage_null_collection:.2f},{sum_instances_collection},{field_instances},{percentage_null_instances:.2f}\n")    



