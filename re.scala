import com.google.gson.Gson
import com.verizon.npi.util.{AvroSerializer, Constants, SharedVariable, SparkAuthenticationTls}
import com.verizon.npi.config.StreamingConfig
import org.apache.log4j.Logger
import org.apache.hadoop.hbase.client.Table
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import com.verizon.npi.data.CaseManagementResponse
import com.verizon.npi.data.CaseManagementRequest
import com.verizon.npi.hbase.{HbaseConnection, HbaseUtil}
import com.vz.secretmanager.GCPSecretVersion

import java.util.concurrent.TimeUnit
import org.apache.pulsar.client.api.{Authentication, CompressionType, Producer, PulsarClient, PulsarClientException, SubscriptionInitialPosition}
import org.apache.pulsar.spark.SparkStreamingPulsarReceiver
import org.apache.spark.streaming.api.java.{JavaDStream, JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.{HashSet, Properties, Set}
import scala.util.Try


class PulsarSink(createProducer: () => Producer[Array[Byte]]) extends Serializable {

  lazy val producer = createProducer()

  def send(value: Array[Byte]): Unit = producer.send(value)
}

object PulsarSink {
  def apply(brokers: String, topic: String, authentication: Authentication): PulsarSink = {
    val f = () => {
      println("[service-verification] Attempting to creating Producer for topic : " + topic)
      val client = PulsarClient.builder().serviceUrl(brokers).enableTls(true).allowTlsInsecureConnection(false)
        .authentication(authentication).build();
      val producer: Producer[Array[Byte]] = client.newProducer().topic(topic).enableBatching(true)
        .blockIfQueueFull(true).batchingMaxMessages(10000)
        .maxPendingMessages(10000).batchingMaxPublishDelay(1300, TimeUnit.MILLISECONDS)
        .compressionType(CompressionType.LZ4).create();

      println("[service-verification] Producer connection established for topic : " + topic)

      sys.addShutdownHook {
        Try(producer.close())
        Try(client.close())
        println("[service-verification] Closed producer and client connection ")
      }

      producer
    }
    new PulsarSink(f)
  }
}

/***
  * This class is the streaming class to get the CM UWA Request on demand to process it and respond back
  *
  */
object REOnDemandStream extends AvroSerializer with HbaseUtil {

  val APP_NAME = "IEN-RE-ONDEMAND-REQUESTS"
  val MASTER = "yarn"
  val gsonShared: SharedVariable[Gson] = SharedVariable {
    import com.google.gson.GsonBuilder
    new GsonBuilder().create
  }
  
  val jaasConfig:String ="org.apache.kafka.common.security.plain.PlainLoginModule required serviceName=\"kafka\" username=\"<username>\" password=\"<password>\";"

  @transient override lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    logger.info("[service-verification] REOnDemandStream about to start : Args Received : "+ args.mkString("|"))
    //check for number of args and accordingly proceed
    if (args.length != 18) {
      logger.error("[service-verification] INVALID SPARK SUBMIT COMMAND: Invalid number of args " + args.length)
      System.exit(1)
    }

    // Get Configuration properties from app.conf
    val config:StreamingConfig = StreamingConfig(args)

    val spark = SparkSession.builder()
      .appName(APP_NAME)
      .master(MASTER)
      .getOrCreate()

    try {

      val thresholds = getDeviationProperties(spark, config)
      // Construct a streaming DataFrame that reads data from topic
      val tlsCert: Array[Byte] = GCPSecretVersion.accessSecretVersion(config.gcpProjectId,
        config.gcpTlsCert, "1").getBytes()
      val tlsKey: Array[Byte] = GCPSecretVersion.accessSecretVersion(config.gcpProjectId,
        config.gcpTlsKey, "1").getBytes()
      val trustCert: Array[Byte] = GCPSecretVersion.accessSecretVersion(config.gcpProjectId,
        config.gcpTrustCert, "1").getBytes()
      val authentication: Authentication = new SparkAuthenticationTls(tlsCert, tlsKey, trustCert)

      import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData
      val pulsarConf: ConsumerConfigurationData[Array[Byte]] = new ConsumerConfigurationData[Array[Byte]]
      val set: Set[String] = new HashSet[String]()
      set.add(config.requestTopic)
      pulsarConf.setTopicNames(set)
      pulsarConf.setSubscriptionName("VMB_IEN_TRANSPORT")
      pulsarConf.setSubscriptionInitialPosition(SubscriptionInitialPosition.Latest)
      val pulsarReceiver = new SparkStreamingPulsarReceiver(config.pulsarServiceUrl, pulsarConf, authentication)
      val jsc = new JavaStreamingContext(spark.sparkContext, batchDuration = Seconds(10))

      val pulsarSink = spark.sparkContext.broadcast(PulsarSink(config.pulsarServiceUrl, config.responseTopic,
        authentication))

      val lineDStream: JavaReceiverInputDStream[Array[Byte]] = jsc.receiverStream(pulsarReceiver)

      import spark.implicits._
      val requestStream: JavaDStream[CaseManagementRequest] = lineDStream.map(requestJson => {
        try {
          val jsonRequest = new String(requestJson)
          val gson = gsonShared.get
          logger.info("[service-verification] Request Message  - " + new String(requestJson))
          val request = Try(gson.fromJson(jsonRequest, classOf[CaseManagementRequest])).getOrElse(null)
          request
        } catch {
          case e:Exception => {
            logger.error(s"[service-verification] Error processing json request $requestJson : ", e)
            null
          }
        }
      }).filter(rec => rec != null)

      val responseStream : JavaDStream[String] = requestStream.map(request => {
        var caseManagementResponse: CaseManagementResponse = null
        if(Constants.HEALTHCHECK.equalsIgnoreCase(request.aid) || Constants.HEALTHCHECK.equalsIgnoreCase(request.alarmPoint)){
          //healthcheck request should not do a hbase scan
          caseManagementResponse = CaseManagementResponse(request, defaultHealthCheckResult(request))
        } else {

          var table: Table = null
          try {
            table = HbaseConnection.getTable(config)
            caseManagementResponse = processOnDemandRequest(table, thresholds, request)
          } catch {
            case e: Exception => {
              logger.error(s"[service-verification] Error processing request $request : ", e)
              caseManagementResponse = CaseManagementResponse(e)
            }
          } finally {
            if (null != table) table.close()
          }
        }

        logger.info("[service-verification] Response Message- " + caseManagementResponse.toString)
        val gson = gsonShared.get
        val responseJson = gson.toJson(caseManagementResponse)
        responseJson
      })

      //result.print
      responseStream.foreachRDD( rdd => {
        rdd.foreach { message =>
          pulsarSink.value.send(message.getBytes())
        }
      })

      jsc.checkpoint(config.checkPoint)
      jsc.start()
      jsc.awaitTermination()
    } catch {
      case e: NoClassDefFoundError => {
        logger.error(
          "[service-verification] EXCEPTION CAUGHT: ", e)
        System.exit(1)
      }
    }
  }

  def getDeviationProperties(spark: SparkSession, config: StreamingConfig): Map[String, String] = {
    // upload this file under files
    val thresholdPropertiesFile = spark.sparkContext.textFile(config.deviationThreshholdPropertiesFilePath)
    //splitting the rdd content from file into key value pairs and storing it
    thresholdPropertiesFile.map(m2 => (m2.split("=")(0), m2.split("=")(1))).collect().toMap
  }

}
