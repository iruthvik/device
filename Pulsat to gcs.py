import apache_beam as beam
import apache_beam.io.iobase as iobase
import pulsar

class PulsarSource(iobase.BoundedSource):
    def __init__(self, pulsar_service_url, topic, tls_cert_file, tls_key_file, tls_trust_cert_file, subscription_name):
        self.pulsar_service_url = pulsar_service_url
        self.topic = topic
        self.tls_cert_file = tls_cert_file
        self.tls_key_file = tls_key_file
        self.tls_trust_cert_file = tls_trust_cert_file
        self.subscription_name = subscription_name

    def estimate_size(self):
        return None

    def get_range_tracker(self, start_position, stop_position):
        return iobase.OffsetRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        client = pulsar.Client(
            service_url=self.pulsar_service_url,
            authentication=pulsar.AuthenticationTLS(self.tls_cert_file, self.tls_key_file),
            tls_trust_certs_file_path=self.tls_trust_cert_file,
            tls_allow_insecure_connection=False
        )
        consumer = client.subscribe(self.topic, self.subscription_name, consumer_type=pulsar.ConsumerType.Shared)
        
        while True:
            msg = consumer.receive()
            yield msg.data().decode('utf-8')
            consumer.acknowledge(msg)

        client.close()

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        yield iobase.SourceBundle(weight=1, source=self, start_position=0, stop_position=None)


class ReadFromPulsar(beam.PTransform):
    def __init__(self, pulsar_service_url, topic, tls_cert_file, tls_key_file, tls_trust_cert_file, subscription_name):
        self.pulsar_service_url = pulsar_service_url
        self.topic = topic
        self.tls_cert_file = tls_cert_file
        self.tls_key_file = tls_key_file
        self.tls_trust_cert_file = tls_trust_cert_file
        self.subscription_name = subscription_name

    def expand(self, pcoll):
        return pcoll | iobase.Read(
            PulsarSource(
                pulsar_service_url=self.pulsar_service_url,
                topic=self.topic,
                tls_cert_file=self.tls_cert_file,
                tls_key_file=self.tls_key_file,
                tls_trust_cert_file=self.tls_trust_cert_file,
                subscription_name=self.subscription_name
            )
        )


#####
pulsar-to-gcs.py

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pulsar_source import ReadFromPulsar

class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--pulsar_service_url', type=str)
        parser.add_value_provider_argument('--pulsar_topic', type=str)
        parser.add_value_provider_argument('--tls_cert_file', type=str)
        parser.add_value_provider_argument('--tls_key_file', type=str)
        parser.add_value_provider_argument('--tls_trust_cert_file', type=str)
        parser.add_value_provider_argument('--output_location', type=str)
        parser.add_value_provider_argument('--subscription_name', type=str)

def run(argv=None):
    pipeline_options = PipelineOptions()
    custom_options = pipeline_options.view_as(CustomPipelineOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        records = (
            p
            | 'Read from Pulsar' >> ReadFromPulsar(
                pulsar_service_url=custom_options.pulsar_service_url,
                topic=custom_options.pulsar_topic,
                tls_cert_file=custom_options.tls_cert_file,
                tls_key_file=custom_options.tls_key_file,
                tls_trust_cert_file=custom_options.tls_trust_cert_file,
                subscription_name=custom_options.subscription_name
            )
        )

        records | 'Write to GCS' >> beam.io.WriteToText(custom_options.output_location)

if __name__ == '__main__':
    run()
