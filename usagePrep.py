from astroquery.cadc import Cadc
import datetime
import pandas as pd

def execute_query(query, filename):
    job = service.create_async(query)
    print(f"Job ID: {job.job_id}")
    job.run().wait()
    job.raise_if_error()
    results = job.fetch_result().to_table().to_pandas()
    num_collection_instrument = len(results)
    print(f"Number of collection/instances: {num_collection_instrument}")
    total_instances = results['num_instances'].sum()
    print(f"Total instances: {total_instances}")

    results.to_csv(filename, index=False)


def count_collInstrObservations():
    filename = "collInstrTotalObs.csv"
    print( f"Querying caom2.Observation and writing results to {filename}")

    query = f"""select collection, instrument_name, count(*) as num_instances
        from caom2.Observation
        where instrument_name is not null and instrument_name != 'NULL'
        group by collection, instrument_name
        order by collection, instrument_name"""
    execute_query(query, filename)
    
def count_collInstrPlanes():
    filename = "collInstrTotalPlanes.csv"
    print( f"Querying caom2.Plane and writing results to {filename}")

    query = f"""select collection, instrument_name, count(*) as num_instances
        from caom2.Observation join caom2.Plane on caom2.Observation.obsID = caom2.Plane.obsID
        where instrument_name is not null and instrument_name != 'NULL'
        group by collection, instrument_name
        order by collection, instrument_name"""
    execute_query(query, filename)

def count_collectionObservations():
    filename = "collectionTotalObs.csv"
    print( f"Querying caom2.Observation and writing results to {filename}")

    query = f"""select collection, count(*) as num_instances
        from caom2.Observation
        where instrument_name is not null and instrument_name != 'NULL'
        group by collection
        order by collection"""
    execute_query(query, filename)
    
def count_collectionPlanes():
    filename = "collectionTotalPlanes.csv"
    print( f"Querying caom2.Plane and writing results to {filename}")

    query = f"""select collection, count(*) as num_instances
        from caom2.Observation join caom2.Plane on caom2.Observation.obsID = caom2.Plane.obsID
        where instrument_name is not null and instrument_name != 'NULL'
        group by collection
        order by collection"""
    execute_query(query, filename)
    
def list_fields():
    filename = "fieldNames.csv"
    print( f"Querying caom2 fields and writing results to {filename}")

    query = f"""select table_name, column_name
        from tap_schema.columns
        where table_name in ('caom2.Observation','caom2.Plane')
        order by table_name, column_name"""
    job = service.create_async(query)
    print(f"Job ID: {job.job_id}")
    job.run().wait()
    job.raise_if_error()
    results = job.fetch_result().to_table().to_pandas()
    num_fields = len(results)
    print(f"Number of fields: {num_fields}")
    results['field_name'] = results['table_name'] + "." + results['column_name']

    results.to_csv(filename, index=False, columns=['field_name'])
    

if __name__ == "__main__":
    service = Cadc()
    
    list_fields()
    count_collectionObservations()
    count_collInstrObservations()
    count_collectionPlanes()
    count_collInstrPlanes()
