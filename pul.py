from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.pulsar import SparkPulsarReceiver
from pulsar import AuthenticationTls, ClientBuilder

APP_NAME = "IEN-RE-ONDEMAND-REQUESTS-VMB"
MASTER = "yarn"
serviceUrl = "pulsar+ssl://vmb-aws-us-west-2-nonprod.verizon.com:6651/"
tlsCert = bytes(GCPSecretVersion.accessSecretVersion(config.gcpProjectId, config.gcpTlsCert, config.gcpVersion), 'utf-8')
tlsKey = bytes(GCPSecretVersion.accessSecretVersion(config.gcpProjectId, config.gcpTlsKey, config.gcpVersion), 'utf-8')
trustCert = bytes(GCPSecretVersion.accessSecretVersion(config.gcpProjectId, config.gcpTrustCert, config.gcpVersion), 'utf-8')
authentication = AuthenticationTls(tls_cert=tlscert, tls_key=tlskey, trust_certs=trustcert)

spark = SparkSession.builder().appName(APP_NAME).master(MASTER).getOrCreate()
thresholds = getDeviationProperties(spark, config)

try:
    pulsar_conf = {'pulsar.service.url': serviceUrl,
                   'pulsar.subscription.name': 'VMB',
                   'pulsar.topic.name': config.pulsarUWARequestTopic,
                   'pulsar.consumer.authentication': authentication}

    pulsar_receiver = SparkPulsarReceiver(**pulsar_conf)

    client = ClientBuilder().service_url(serviceUrl)\
                            .enable_tls(True)\
                            .allow_tls_insecure_connection(False)\
                            .authentication(authentication)\
                            .build()
    producer = client.new_producer(topic=config.pulsarUWAResponseTopic)

    pulsar_receiver.stream_id
    print("printing pulsar stream id " + str(pulsar_receiver.stream_id))

    sc = StreamingContext(spark.sparkContext, batchDuration=5)

    print("printing spark context " + str(sc))
    receiver = sc.receiverStream(pulsar_receiver)
    print("printing receiver " + str(receiver))
    print("printing receiver count " + str(receiver.count()))

    def process_request(d):
        try:
            print("inside map " + str(d))
            data = d.decode('utf-8')
            print("printing receiver stream data " + data)
            req = json.loads(data)
            print("Request Message  - " + data + " output :: " + str(req))
            return req
        except Exception as e:
            print("Error processing pod avro request {d} : {e}")
            return None

    json_requests = receiver.map(process_request)

except Exception as e:
    print("Exception caught: {e}")
