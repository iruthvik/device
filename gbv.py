from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import *
from pyspark.sql import *
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType, StructField

import os, sys, uuid, time
from secrets_utils import SecretsUtils

bootStrap = sys.argv[1]
subScribe = sys.argv[2]
Offset = sys.argv[3]
protocol = sys.argv[4].upper()
# truststoreName = sys.argv[5]
# truststorePWD = sys.argv[6]
# keystoreName = sys.argv[7]
kafka_sasl_mechanism=sys.argv[5]
kafka_ssl_endpoint_identification_algorithm=sys.argv[6]
kafka_jaas_conf=sys.argv[7]
sourceDataset = str(sys.argv[8])
avsc = sys.argv[9]
ckptLoc = sys.argv[10]
kafkaOutputTopic = sys.argv[11]
project_id = sys.argv[12]
secet_id = sys.argv[13]
version_id = sys.argv[14]

kafka_jaas_conf=SecretsUtils.getJaasWithCredentials(kafka_jaas_conf, project_id, secet_id, version_id)

spark = SparkSession.builder.appName("DNMSourceStreaming").enableHiveSupport().getOrCreate()
# we are getting error here, so commenting for testing
#spark = SparkSession.builder.appName("DNMSourceStreaming").getOrCreate()

spark.sparkContext.setLogLevel('WARN')

log4jLogger = spark._jvm.org.apache.log4j
logging = log4jLogger.LogManager.getLogger("DNM Main Streaming")

logging.info("--- DNM Streaming starts from here ---")

jsonSchema = StructType()\
    .add("finishTime", StringType(), True)\
    .add("vendor", StringType(), True)\
    .add("elements", ArrayType(StructType()\
    .add("oidTag", StringType(), True)\
    .add("ifIndex", StringType(), True)\
    .add("sysUpTime", StringType(), True)\
    .add("orient", StringType(), True)\
    .add("oid", StringType(), True)\
    .add("previousSysUpTime", StringType(), True)\
    .add("type", StringType(), True)\
    .add("value", StringType(), True)\
    .add("qosQueue", StringType(), True)\
    .add("delta_time_ms", StringType(), True)\
    .add("delta_counter", StringType(), True)\
    .add("delta_rate", StringType(), True)\
    .add("epoch_collected", StringType(), True)\
    .add("previousValue", StringType(), True), True))\
    .add("dataType", StringType(), True)\
    .add("startTime", StringType(), True)\
    .add("deviceName", StringType(), True)\
    .add("max_pdu_per_sec", StringType(), True)\
    .add("ifDescr", StringType(), True)\
    .add("loopback", StringType(), True)\
    .add("intervalTime", StringType(), True)\
    .add("ifAlias", StringType(), True)\
    .add("internalTopic", StringType(), True)\
    .add("pollerCID", StringType(), True)\
    .add("pollingInterval", StringType(), True)

sc = spark.sparkContext

keyStoreSecret = uuid.uuid4().hex

#spark._jvm.com.vz.vznet.lib.KeyUtils().createAthensKeyStoreFromOozieCred(oozieCred, athensDomain, athensService, athenskeyId, keyStoreSecret, "./" + keystoreName)

#sc.addFile("./" + keystoreName)
#
# logging.info('truststoreName:%s'%truststoreName)
# logging.info('truststore Password:%s'%truststorePWD)
# logging.info('keystoreName:%s'%keystoreName)
# logging.info('keyStoreSecret:%s'%keyStoreSecret)
# logging.info('avsc:%s'%avsc)
# logging.info('oozieCred:%s'%oozieCred)
# logging.info('athensDomain:%s'%athensDomain)
# logging.info('athensService:%s'%athensService)
# logging.info('athenskeyId:%s'%athenskeyId)
logging.info('Dataset name:%s'%sourceDataset)
logging.info('Kafka offset:%s'%Offset)
logging.info('Kafka input topic:%s'%subScribe)
logging.info('Kafka output topic:%s'%kafkaOutputTopic)
logging.info('Kafka checkpoint location:%s'%ckptLoc)

rawTopicMessage = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootStrap)\
        .option("subscribe", subScribe)\
        .option("startingOffsets", Offset)\
        .option("kafka.security.protocol", protocol)\
        .option("key.serializer","org.apache.kafka.common.serialization.StringSerializer")\
        .option("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")\
        .option("kafka.ssl.endpoint.identification.algorithm", kafka_ssl_endpoint_identification_algorithm)\
        .option("kafka.sasl.mechanism", kafka_sasl_mechanism)\
        .option("kafka.sasl.jaas.config",kafka_jaas_conf)\
        .option("failOnDataLoss", "false")\
        .option("mode","PERMISSIVE")\
        .load()

schema_tls = open(avsc,"r").read()

df = rawTopicMessage.select(from_avro(col("value"),schema_tls).alias("values"))


data = df.select("values.*")\
    .withColumn("json",col("rawdata").cast("String"))\
    .withColumn("json",from_json("json",jsonSchema))\
    .select("timestamp","host","src","_event_ingress_ts","_event_origin","_event_tags","_event_route","_event_route","json.*")\
    .withColumn("elements",explode("elements"))
    
filterdata = data.select("timestamp","host","src","_event_ingress_ts","_event_origin",col("_event_tags").cast("String"),"_event_route","finishTime","vendor","dataType","startTime","deviceName","max_pdu_per_sec","ifDescr","loopback","intervalTime","ifAlias","internalTopic","pollerCID","pollingInterval","elements.*").filter(upper(col("src")) == sourceDataset.upper())

ipfilter = filterdata.filter(col("dataType") == 'IP')

intervalData = ipfilter.withColumn("measurement_time_15min",to_timestamp(from_unixtime(floor((col("finishTime"))/(1000 * 900)) * 900, "yyyyMMdd HH:mm"),"yyyyMMdd HH:mm"))

bytesData = intervalData.withColumn("deltatime",(col("sysUpTime") - col("previousSysUpTime"))/100)\
        .withColumn("bytesInValue",when(col("orient") == '1',col("value") - col("previousValue")))\
        .withColumn("bytesOutValue",when(col("orient") == '0',col("value") - col("previousValue")))\
        .withColumn("intervalTime",round((col("finishTime") - col("startTime"))/1000, 0))\
        .withColumn("date",from_unixtime(floor((col("finishTime"))/(1000 * 900)) * 900, "yyyyMMdd"))

#oidData = bytesData.withColumn("timestamp",to_timestamp(from_unixtime((col("timestamp"))/1000, "yyyyMMdd HH:mm"),"yyyyMMdd HH:mm")).withColumn("oidTag", col("oidTag").cast("float")).filter(col("deltaTime").isNotNull()).filter(col("oidTag") == 103.0)
#change to add additional filter 100
oidData = bytesData.withColumn("timestamp",to_timestamp(from_unixtime((col("timestamp"))/1000, "yyyyMMdd HH:mm"),"yyyyMMdd HH:mm")).withColumn("oidTag", col("oidTag").cast("float")).filter(col("deltaTime").isNotNull()).filter(col("oidTag").isin('100.0','103.0'))

#oidGroupData = oidData.withWatermark("kafkaTimestamp", "1 minutes").groupBy(window("kafkaTimestamp","5 minutes"),"dataType","deviceName","ifDescr","oidTag","measurement_time_15min","src","date").agg(sum("bytesInValue").alias("bytesIn"),sum("bytesOutValue").alias("bytesOut"),sum("deltatime").alias("deltaTime")).filter(col("deltaTime").isNotNull())


flat = oidData.select("deviceName","ifDescr","measurement_time_15min","bytesInValue","bytesOutValue","deltaTime","src","date")

nonulls = flat.withColumn("bytesInValue",when(col("bytesInValue").isNull(),lit("0.0")).otherwise(col("bytesInValue")))\
            .withColumn("bytesOutValue",when(col("bytesOutValue").isNull(),lit("0.0")).otherwise(col("bytesOutValue")))\
            .withColumn("src",when(col("src").isNull(),lit("UNKNOWN")).otherwise(col("src")))

#to handle negative values
nonnegative = nonulls.withColumn("bytesInValue", when(col("bytesInValue") < 0,lit("0.0")).otherwise(col("bytesInValue")))\
                    .withColumn("bytesOutValue", when(col("bytesOutValue") < 0,lit("0.0")).otherwise(col("bytesOutValue")))

#noDups = nonulls.withWatermark("measurement_time_15min","5 minutes").dropDuplicates()

psvdata = nonnegative.withColumn("value",concat(col("deviceName"),lit("|"),col("ifDescr"),lit("|"),col("measurement_time_15min").cast("String"),lit("|"),col("bytesInValue").cast("String"),lit("|"),col("bytesOutValue").cast("String"),lit("|"),col("deltaTime").cast("String"),lit("|"),col("src"),lit("|"),col("date").cast("String")))


avrodata = psvdata.select(to_avro(col("value")).alias("value"))

final = avrodata.writeStream\
      .format("kafka")\
      .option("kafka.bootstrap.servers", bootStrap)\
      .option("topic", kafkaOutputTopic)\
      .option("kafka.security.protocol", protocol)\
      .option("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")\
      .option("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")\
      .option("kafka.ssl.endpoint.identification.algorithm", kafka_ssl_endpoint_identification_algorithm)\
      .option("kafka.sasl.mechanism", kafka_sasl_mechanism)\
      .option("kafka.sasl.jaas.config",kafka_jaas_conf)\
      .option("kafka.serializer.class", "kafka.serializer.DefaultEncoder")\
      .option("checkpointLocation", ckptLoc)\
      .start()

start = time.time()

while(final.isActive):
    logging.info("Current time : %s" % time.ctime())
    logging.info(final.lastProgress)
    logging.info(final.status)
    timeDiff = time.time() - start
    runningTime = timeDiff/60
    logging.info("Job running time: %s mins" %runningTime)
    time.sleep(300)
    if timeDiff >= 86000:
        final.stop()
        
final.stop()
