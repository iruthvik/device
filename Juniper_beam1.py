import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.gcp.pubsub import PubSubSink
from apache_beam.transforms.window import SlidingWindows
from apache_beam.transforms.util import FastUnnest
from apache_beam.transforms import Map, ParDo

# Define a class to parse the schema
class ParseSchema(beam.DoFn):
  def process(self, element):
    # Parse the JSON element based on the schema definition (replace with your schema logic)
    parsed_data = {
        "devicename": element["system"]["metaData"]["ucgDeviceName"],
        "timestamp": element["system"]["timestamp"],
        # ... other fields based on schema
    }
    for interface in element["system"]["perf"]["interfaces"]["interface"]:
      interface_data = {
          "interface": interface["name"],
          # ... other interface specific fields based on schema
      }
      interface_data.update(parsed_data)
      yield interface_data

# Define a function to calculate deltas
def calculate_delta(data, columns):
  window = beam.window.SlidingWindows(size=301, period=600)
  with beam.window.WindowedView(data, window=window) as windowed_data:
    max_values = windowed_data.aggregate(
        lambda elements: max(element[col] for element in elements) for col in columns
    )
    min_values = windowed_data.aggregate(
        lambda elements: min(element[col] for element in elements) for col in columns
    )
    delta_data = []
    for element in windowed_data:
      delta_row = {}
      for col in columns:
        delta_row[f"delta_{col}"] = max_values[col] - min_values[col]
      delta_row.update(element)
      delta_data.append(delta_row)
    return delta_data

# Beam Pipeline
with beam.Pipeline() as pipeline:
  # Read data from Pulsar topic
  data = pipeline | 'ReadFromKafka' >> ReadFromKafka(
      consumer_config={
          'pulsar.service.url': 'pulsar-service-url',
          'topic': 'pulsar-topic-name',
          # ... other Pulsar configuration options
      }
  )

  # Parse data using the schema
  parsed_data = data | 'ParseSchema' >> ParDo(ParseSchema())

  # Filter data with "in_octets" only (optional, modify as needed)
  filtered_data = parsed_data | 'FilterData' >> beam.Filter(lambda x: 'in_octets' in x)

  # Calculate deltas for specified columns
  delta_data = filtered_data | 'CalculateDelta' >> beam.FlatMapTuple(
      calculate_delta, columns=["in_octets", "out_octets", "..."]  # Update with your columns
  )

  # Enrich data with additional fields
  enriched_data = delta_data | 'EnrichData' >> beam.Map(
      lambda x: {
          "recordtype": "juniper_inf",
          # ... other fields based on your logic
          **x
      }
  )

  # Write data to BigQuery
  write_to_bq = enriched_data | 'WriteToBigQuery' >> WriteToBigQuery(
      table='project:dataset.table_name',
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
      temporary_gcs_bucket='temporary-gcs-bucket'
  )

  # Write data to Pub/Sub topic (optional)
  write_to_pubsub = enriched_data | 'WriteToPubSub' >> PubSubSink(
      topic='output-topic-name'
  )

  # Set pipeline options (replace with your GCP project and region)
  pipeline.options.view_as_dict()["project"] = "your-gcp-project"
  pipeline.options.view_as_dict()["region"] = "your-gcp-region"
  
