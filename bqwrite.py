from google.cloud import bigquery
import datetime

def join_and_write_table():
    # Create BigQuery client
    client = bigquery.Client()

    # Define the source table names
    source_table1 = "project.dataset.source_table1"
    source_table2 = "project.dataset.source_table2"

    # Define the destination table name
    destination_table = "project.dataset.destination_table"

    # Get the last execution timestamp from the storage table
    last_execution_timestamp = get_last_execution_timestamp(client)

    # Define the join query with a timestamp filter
    query = f"""
        SELECT t1.timestamp, t1.devicename, t1.interface, t1.in_octets, t1.out_octets,
            t2.customername, t2.routername, t2.location, t2.interface, t2.speed
        FROM `{source_table1}` AS t1
        JOIN `{source_table2}` AS t2
        ON t1.devicename = t2.routername AND t1.interface = t2.interface
        WHERE t1.inserted_datetime > TIMESTAMP("{last_execution_timestamp}")
    """

    # Run the query and write the result to the destination table
    job_config = bigquery.QueryJobConfig(destination=destination_table)
    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Wait for the query to complete

    # Update the storage table with the current execution timestamp
    update_last_execution_timestamp(client)

def get_last_execution_timestamp(client):
    # Retrieve the last execution timestamp from the storage table
    # Implement your logic here to fetch the timestamp
    # Example: query the storage table and retrieve the latest timestamp

    query = """
        SELECT MAX(timestamp) AS last_execution_timestamp
        FROM `project.dataset.last_execution_table`
    """

    query_job = client.query(query)
    results = query_job.result()

    # Assuming the result contains a single row with a single column
    for row in results:
        if row.last_execution_timestamp is not None:
            return row.last_execution_timestamp.strftime("%Y-%m-%dT%H:%M:%S")

    # Return a default timestamp if no previous execution data is found
    return "1970-01-01T00:00:00"  # or any other appropriate default value

def update_last_execution_timestamp(client):
    # Update the storage table with the current execution timestamp
    # Implement your logic here to update the timestamp
    # Example: insert the current timestamp into the storage table

    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    query = f"""
        INSERT INTO `project.dataset.last_execution_table` (timestamp)
        VALUES (TIMESTAMP("{current_timestamp}"))
    """

    query_job = client.query(query)
    query_job.result()  # Wait for the query to complete

if __name__ == "__main__":
    join_and_write_table()
