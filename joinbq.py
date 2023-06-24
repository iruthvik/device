import uuid
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

    # Create a unique table name for the temporary results
    temp_table = f"temp_table_{uuid.uuid4().hex}"

    # Define the query to create the temporary results table
    create_table_query = f"""
        CREATE TABLE `{temp_table}`
        AS (
            SELECT t1.timestamp, t1.devicename, t1.interface, t1.in_octets, t1.out_octets,
                t2.customername, t2.routername, t2.location, t2.interface_name, t2.speed
            FROM `{source_table1}` AS t1
            JOIN `{source_table2}` AS t2
            ON t1.devicename = t2.routername AND t1.interface = t2.interface_name
            WHERE t1.inserted_datetime > TIMESTAMP("{last_execution_timestamp}")
        )
    """

    # Run the query to create the temporary results table
    create_table_job = client.query(create_table_query)
    create_table_job.result()  # Wait for the query to complete

    # Copy the contents of the temporary results table to the destination table
    copy_table_job = client.copy_table(temp_table, destination_table)
    copy_table_job.result()  # Wait for the copy operation to complete

    # Delete the temporary results table
    delete_table_query = f"DROP TABLE `{temp_table}`"
    delete_table_job = client.query(delete_table_query)
    delete_table_job.result()  # Wait for the query to complete

    # Update the storage table with the current execution timestamp
    update_last_execution_timestamp(client)

def get_last_execution_timestamp(client):
    # Query the storage table to get the last execution timestamp
    query = """
        SELECT last_execution_timestamp
        FROM `project.dataset.last_execution_table`
        ORDER BY last_execution_timestamp DESC
        LIMIT 1
    """

    query_job = client.query(query)
    results = query_job.result()

    if results.total_rows > 0:
        # Get the last execution timestamp from the query results
        last_execution_timestamp = results[0]["last_execution_timestamp"]
        return last_execution_timestamp
    else:
        # Handle the case when there are no previous execution timestamps
        # Return a default value or raise an exception
        return None

def update_last_execution_timestamp(client):
    # Get the current timestamp
    current_timestamp = datetime.datetime.now()

    # Insert the current timestamp into the storage table
    query = f"""
        INSERT INTO `project.dataset.last_execution_table` (last_execution_timestamp)
        VALUES ("{current_timestamp}")
    """

    query_job = client.query(query)
    query_job.result()  # Wait for the query to complete

if __name__ == "__main__":
    join_and_write_table()
