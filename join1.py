from google.cloud import bigquery
from pulsar import Client, Message

# Initialize BigQuery client
bq_client = bigquery.Client()

# Initialize Pulsar client
pulsar_client = Client('pulsar+ssl://your-pulsar-broker-url', authentication='tls',
                       tls_trust_certs_file_path='/path/to/trust.pem',
                       tls_cert_file_path='/path/to/cert.pem',
                       tls_key_file_path='/path/to/key.pem')

# Maintain a set to keep track of previously published messages
published_messages = set()

def process_event(event, context):
    # Retrieve data from counter_table and speed_table
    counter_data = get_latest_data_from_bigquery('counter_table')
    speed_data = get_latest_data_from_bigquery('speed_table')
    
    # Check if both tables have data
    if not counter_data or not speed_data:
        print("No data found in one or both tables.")
        return
    
    # Perform join operation
    joined_data = join(counter_data, speed_data)

    # Check if joined data is empty
    if not joined_data:
        print("No joined data available.")
        return

    # Filter out previously published messages from joined data
    new_messages = filter_published_messages(joined_data)

    # Publish only new messages to Pulsar topic
    publish_to_pulsar(new_messages)

    # Log the published messages
    log_published_messages(new_messages)

def get_latest_data_from_bigquery(table_name):
    query = f"""
        SELECT *
        FROM `{table_name}`
        ORDER BY timestamp DESC
        LIMIT 1
    """
    query_job = bq_client.query(query)
    rows = query_job.result()
    return rows

def join(counter_data, speed_data):
    # Implement join logic here
    # Assuming you have a common column 'devicename', 'interface', 'timestamp' for joining
    # Example:
    joined_data = []
    for counter_row in counter_data:
        for speed_row in speed_data:
            if counter_row.devicename == speed_row.devicename and \
               counter_row.interface == speed_row.interface and \
               counter_row.timestamp == speed_row.timestamp:
                joined_data.append({
                    'devicename': counter_row.devicename,
                    'interface': counter_row.interface,
                    'timestamp': counter_row.timestamp,
                    # Add other columns as needed
                })
                break
    return joined_data

def publish_to_pulsar(data):
    pulsar_topic = pulsar_client.create_producer('your-pulsar-topic')
    for row in data:
        pulsar_topic.send(Message(row))
        # Add published message to the set of published messages
        published_messages.add((row['devicename'], row['interface'], row['timestamp']))

def log_published_messages(data):
    for message in data:
        print("Published message:", message)

def filter_published_messages(data):
    new_messages = []
    for message in data:
        if (message['devicename'], message['interface'], message['timestamp']) not in published_messages




===============
def join(counter_data, speed_data):
    # Formulate the SQL query for joining counter_table and speed_table
    query = """
        SELECT c.*, s.*
        FROM counter_table c
        JOIN speed_table s
        ON c.devicename = s.devicename
        AND c.interface = s.interface
        AND c.timestamp = s.timestamp
    """

    # Execute the query
    query_job = bq_client.query(query)
    rows = query_job.result()

    # Convert the result to a list of dictionaries
    joined_data = []
    for row in rows:
        joined_data.append(dict(row.items()))

    return joined_data

