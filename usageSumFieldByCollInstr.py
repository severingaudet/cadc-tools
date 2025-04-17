import pandas as pd
import os.path

# This script generates a summary of the number of collection/instrument and field instances with null values.
# It reads pre-generated CSV files containing collection/instrument counts for observations and planes,
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

    obs_filename = "config/collInstrTotalObs.csv"
    planes_filename = "config/collInstrTotalPlanes.csv"
    fields_filename = "config/fieldNames.csv"
    summary_filename = "fieldByCollInstr/sumFieldByCollInstr.csv"

    #
    # Initialize the data structures for pre-generated collection/instrument counts for observations.
    #
    array_collInstr_obs = pd.read_csv(obs_filename)
    num_collInstr_obs = len(array_collInstr_obs)
    #print(f"Number of collection/instrument in {obs_filename}: {num_collInstr_obs}")
    sum_instances_collInstr_obs = array_collInstr_obs['num_instances'].sum()
    #print(f"Sum of instances of collection/instrument in {obs_filename}: {sum_instances_collInstr_obs}")

    #
    # Initialize the data structures for pre-generated collection/instrument counts for planes.
    #
    array_collInstr_planes = pd.read_csv(planes_filename)
    num_collInstr_planes = len(array_collInstr_planes)
    #print(f"Number of collection/instrument in {planes_filename}: {num_collInstr_planes}")
    sum_instances_collInstr_planes = array_collInstr_planes['num_instances'].sum()
    #print(f"Sum of instances of collection/instrument in {planes_filename}: {sum_instances_collInstr_planes}")

    #
    # Initialize the summary output filew with the CSV header.
    #
    with open(summary_filename, 'w') as f:
        f.write("Table,Collection,Instrument,number of fields,fields with null values,percentage of fields with null values,number of instances,instances with null values,percentage of instances with null values\n")

    #
    # Initialize lists of fields to be checked for output.
    #
    field_names = pd.read_csv(fields_filename)
    num_obs_fields = field_names['field_name'].str.startswith("caom2.Observation").sum()
    num_plane_fields = field_names['field_name'].str.startswith("caom2.Plane").sum()

    #
    # Now loop through the collection/instrument and summarize the corresponding output file.
    #
    for index, row_obs in array_collInstr_obs.iterrows():
        collection = row_obs['collection']
        instrument_name = row_obs['instrument_name']
        num_collInstr_observations = row_obs['num_instances']
        num_collInstr_obs_instances = num_collInstr_observations * num_obs_fields
    
        instrument_for_filename = instrument_name.replace(" ", "_")
        instrument_for_filename = instrument_for_filename.replace("/", "_")
        collInstr_filename = f"fieldByCollInstr/{collection}.{instrument_for_filename}.csv"

        if os.path.isfile(collInstr_filename):
            print(f"Processing collection/instrument {collection} {instrument_name} from {collInstr_filename}")
            results = pd.read_csv(collInstr_filename)

            ## Process Observation fields

            obs_results= results[results['Field'].str.contains("caom2.Observation")]
            num_collInstr_obs_fields_with_null = len(obs_results)
            percentage_obs_fields_with_null = num_collInstr_obs_fields_with_null * 100 / num_obs_fields
            #print(f"Number of obs fields with null for {collection}/{instrument_name}: {num_collInstr_obs_fields_with_null}")
            num_collInstr_obs_instances_with_null = obs_results['num_null'].sum()
            #print(f"Number of obs instances with null for {collection}/{instrument_name}: {num_collInstr_obs_instances_with_null}")
            percentage_obs_instances_with_null = num_collInstr_obs_instances_with_null * 100 / num_collInstr_obs_instances
            with open(summary_filename, 'a') as f:
                f.write(f"caom2.Observation,{collection},{instrument_name},{num_obs_fields},{num_collInstr_obs_fields_with_null},{percentage_obs_fields_with_null:.2f},{num_collInstr_obs_instances},{num_collInstr_obs_instances_with_null},{percentage_obs_instances_with_null:.2f}\n")    

            ## Process Plane fields

            row_planes = array_collInstr_planes[(array_collInstr_planes['collection'] == collection) & (array_collInstr_planes['instrument_name'] == instrument_name)]
            if len(row_planes) > 0:
                num_collInstr_planes = row_planes['num_instances'].values[0]
                num_collInstr_plane_instances = num_collInstr_planes * num_plane_fields

                plane_results = results[results['Field'].str.contains("caom2.Plane")]
                num_collInstr_plane_fields_with_null = len(plane_results)
                percentage_plane_fields_with_null = num_collInstr_plane_fields_with_null * 100 / num_plane_fields
                #print(f"Number of plane fields with null for {collection}/{instrument_name}: {num_collInstr_plane_fields_with_null}")
                num_collInstr_plane_instances_with_null = plane_results['num_null'].sum()
                #print(f"Number of plane instances with null for {collection}/{instrument_name}: {num_collInstr_plane_instances_with_null}")
                percentage_plane_instances_with_null = num_collInstr_plane_instances_with_null * 100 / num_collInstr_plane_instances
                with open(summary_filename, 'a') as f:
                    f.write(f"caom2.Plane,{collection},{instrument_name},{num_plane_fields},{num_collInstr_plane_fields_with_null},{percentage_plane_fields_with_null:.2f},{num_collInstr_plane_instances},{num_collInstr_plane_instances_with_null},{percentage_plane_instances_with_null:.2f}\n")    

