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

    # Define the destination table schema
    destination_table_schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("devicename", "STRING"),
        bigquery.SchemaField("interface", "STRING"),
        bigquery.SchemaField("in_octets", "INT64"),
        bigquery.SchemaField("out_octets", "INT64"),
        bigquery.SchemaField("customername", "STRING"),
        bigquery.SchemaField("routername", "STRING"),
        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("interface_name", "STRING"),
        bigquery.SchemaField("speed", "INT64"),
    ]

    # Create the destination table if it doesn't exist
    create_table_if_not_exists(client, destination_table, destination_table_schema)

    # Define the join query with a timestamp filter
    query = f"""
        INSERT INTO `{destination_table}`
        SELECT t1.timestamp, t1.devicename, t1.interface, t1.in_octets, t1.out_octets,
            t2.customername, t2.routername, t2.location, t2.interface_name, t2.speed
        FROM `{source_table1}` AS t1
        JOIN `{source_table2}` AS t2
        ON t1.devicename = t2.routername AND t1.interface = t2.interface_name
        WHERE t1.inserted_datetime > TIMESTAMP("{last_execution_timestamp}")
    """

    # Run the query to insert the joined data into the destination table
    query_job = client.query(query)
    query_job.result()  # Wait for the query to complete

    # Update the storage table with the current execution timestamp
    update_last_execution_timestamp(client)

def get_last_execution_timestamp(client):
    # Retrieve the last execution timestamp from the storage table
    # Implement your logic here to fetch the timestamp

def update_last_execution_timestamp(client):
    # Update the storage table with the current execution timestamp
    # Implement your logic here to update the timestamp

def create_table_if_not_exists(client, table_id, schema):
    # Create the table if it doesn't exist
    table = bigquery.Table(table_id, schema=schema)
    try:
        client.create_table(table)
        print(f"Table '{table_id}' created successfully.")
    except Exception as e:
        print(f"Error creating table '{table_id}': {e}")

if __name__ == "__main__":
    join_and_write_table()
