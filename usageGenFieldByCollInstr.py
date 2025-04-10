# This script is used to generate CSV files per collection containing the fields and counts of null values for all
# fields in caom2.Observation and caom2.Plane tables.

import pandas as pd
import os.path

if __name__ == "__main__":

## Initialize the data structure for pre-generated collection counts for observations.
    obs_filename = "config/collInstrTotalObs.csv"
    array_collection_obs = pd.read_csv(obs_filename)

## Initialize lists of fields to be checked for null values.

    fields_filename = "config/fieldNames.csv"
    field_names = pd.read_csv(fields_filename)

## Now loop through the the list of collections and process them

    for index, row in array_collection_obs.iterrows():
        collection = row['collection']
        instrument_name = row['instrument_name']
        print(f"Processing collection {collection}/{instrument_name}")
        instrument_for_filename = instrument_name.replace(" ", "_")
        instrument_for_filename = instrument_for_filename.replace("/", "_")
        collInstr_filename = f"fieldByCollInstr/{collection}.{instrument_for_filename}.csv"

        with open(collInstr_filename, 'w') as f:
           f.write("Field, num_null, num_instances,percentage_null\n")

        ## loop through all the fields

        for index, row in field_names.iterrows():
            field = row['field_name']
            field_filename = f"collInstrByField/{field}.csv"
            if os.path.isfile(field_filename):
                results = pd.read_csv(field_filename)
                row = results[(results['collection'] == collection) & (results['instrument_name'] == instrument_name)]
                if len(row) >  0:
                    num_null = row['num_null'].values[0]
                    num_instances = row['num_instances'].values[0]
                    percentage_null = row['percentage_null'].values[0]

                    with open(collInstr_filename, 'a') as f:
                        f.write(f"{field},{num_null},{num_instances},{percentage_null}\n")