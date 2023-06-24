from google.cloud import bigquery

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

    # Create a temporary table to hold the join results
    temp_table = create_temp_table(client)

    # Define the join query with a timestamp filter
    query = f"""
        CREATE TEMPORARY TABLE `{temp_table}`
        AS (
            SELECT t1.timestamp, t1.devicename, t1.interface, t1.in_octets, t1.out_octets,
                t2.customername, t2.routername, t2.location, t2.interface_name, t2.speed
            FROM `{source_table1}` AS t1
            JOIN `{source_table2}` AS t2
            ON t1.devicename = t2.routername AND t1.interface = t2.interface_name
            WHERE t1.inserted_datetime > TIMESTAMP("{last_execution_timestamp}")
        )
    """

    # Run the query to create the temporary table
    query_job = client.query(query)
    query_job.result()  # Wait for the query to complete

    # Copy the contents of the temporary table to the destination table
    copy_temp_table_to_destination(client, temp_table, destination_table)

    # Update the storage table with the current execution timestamp
    update_last_execution_timestamp(client)

def get_last_execution_timestamp(client):
    # Retrieve the last execution timestamp from the storage table
    # Implement your logic here to fetch the timestamp

def update_last_execution_timestamp(client):
    # Update the storage table with the current execution timestamp
    # Implement your logic here to update the timestamp

def create_temp_table(client):
    # Create a temporary table with a unique name
    temp_table = f"{client.project}.dataset.temp_table_{uuid.uuid4().hex}"
    return temp_table

def copy_temp_table_to_destination(client, temp_table, destination_table):
    # Copy the contents of the temporary table to the destination table
    job_config = bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.copy_table(temp_table, destination_table, job_config=job_config)
    job.result()  # Wait for the copy job to complete

    # Delete the temporary table
    client.delete_table(temp_table)

if __name__ == "__main__":
    join_and_write_table()
