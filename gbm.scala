import com.google.gson.Gson
import com.verizon.npi.config.StreamingConfigVMB
import com.verizon.npi.data.CaseManagementRequest
import com.verizon.npi.hbase.{HbaseConnectionVMB, HbaseUtil}
import com.verizon.npi.util.StormAuthenticationTls
import com.verizon.npi.util.{AvroSerializer, SharedVariable}
import com.vz.secretmanager.GCPSecretVersion
import org.apache.log4j.Logger
import org.apache.pulsar.client.api.{Authentication, Producer, PulsarClient,Schema}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client.Table
import com.verizon.npi.data.CaseManagementResponse
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData
import org.apache.pulsar.spark.SparkStreamingPulsarReceiver
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._

import java.util
import java.util.{HashSet, Set}
import scala.util.Try

/***
 * This class is the streaming class to get the CM UWA Request on demand to process it and respond back
 *
 */
object REOnDemandStreamVMB extends AvroSerializer with HbaseUtil {

  val APP_NAME = "IEN-RE-ONDEMAND-REQUESTS-VMB"
  val MASTER = "yarn"
  val serviceUrl = "pulsar+ssl://vmb-aws-us-west-2-nonprod.verizon.com:6651/"
  val gsonSharedVariable: SharedVariable[Gson] = SharedVariable {
    import com.google.gson.GsonBuilder
    new GsonBuilder().create
  }


  @transient override lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("REOnDemandStreamVMB about to start : Args Received : " + args.mkString("|"))
    //check for number of args and accordingly proceed
    if (args.length != 15) {
      logger.error("INVALID SPARK SUBMIT COMMAND: Invalid number of args " + args.length)
      System.exit(1)
    }

    // Get Configuration properties from app.conf
    val config: StreamingConfigVMB = StreamingConfigVMB(args)


    val spark = SparkSession.builder()
      .appName(APP_NAME)
      .master(MASTER)
      .getOrCreate()

    val thresholds = getDeviationProperties(spark, config)

    val tlsCert: Array[Byte] = GCPSecretVersion.accessSecretVersion(config.gcpProjectId, config.gcpTlsCert, config.gcpVersion).getBytes()
    val tlsKey: Array[Byte] = GCPSecretVersion.accessSecretVersion(config.gcpProjectId, config.gcpTlsKey, config.gcpVersion).getBytes()
    val trustCert: Array[Byte] = GCPSecretVersion.accessSecretVersion(config.gcpProjectId, config.gcpTrustCert, config.gcpVersion).getBytes()


    logger.info("[tlsCert] :" + tlsCert)
    logger.info("[tlsKey] :" + tlsKey)
    logger.info("[trustCert] :" + trustCert)

    val authentication: Authentication = new StormAuthenticationTls(tlsCert, tlsKey, trustCert)


    try {


      import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData
      val pulsarConf: ConsumerConfigurationData[Array[Byte]] = new ConsumerConfigurationData[Array[Byte]]
      val set: Set[String] = new HashSet[String]()
      set.add(config.pulsarUWARequestTopic)
      pulsarConf.setTopicNames(set)
      pulsarConf.setSubscriptionName("VMB")
      val pulsarReceiver = new SparkStreamingPulsarReceiver(config.pulsarServiceUrl, pulsarConf, authentication)

      val client:PulsarClient= PulsarClient.builder()
        .serviceUrl(config.pulsarServiceUrl)
        .enableTls(true)
        .allowTlsInsecureConnection(false)
        .authentication(authentication)
        .build();
      val producer=client.newProducer().topic(config.pulsarUWAResponseTopic).create()



      pulsarReceiver.streamId
      logger.info("printing pulsar stream id " + pulsarReceiver.streamId)
      //val conf = spark.conf
      val sc=new StreamingContext(spark.sparkContext, batchDuration = Seconds(5))

      logger.info("printing spark context " + sc)
      val receiver=sc.receiverStream(pulsarReceiver)

      logger.info("printing receiver " + receiver)
      logger.info("printing receiver " + receiver.count())
      val jsonRequests=receiver.map(d=>{
        try {
          logger.info("inside map " + d)
          val data = new String(d)
          logger.info("printing receiver stream data " + data)
          val gson = gsonSharedVariable.get
          logger.info("Request Message  - " + data + " output :: " + Try(gson.fromJson(data, classOf[CaseManagementRequest])).getOrElse(null).toString())
          Try(gson.fromJson(data, classOf[CaseManagementRequest])).getOrElse(null)
        }catch {
          case e:Exception => {
            logger.error(s"Error processing pod avro request $d : ", e)
            null
          }
        }
      })

      jsonRequests.print()

       val jsonResult=jsonRequests.mapPartitions(requests => {
        val newPartition = requests.map(request => {
          var table: Table = null
          var caseManagementResponse: CaseManagementResponse = null
          try {
            table = HbaseConnectionVMB.getTable(config)
            caseManagementResponse = processOnDemandRequest(table, thresholds, request)
          } catch {
            case e:Exception => {
              logger.error(s"Error processing request $request : ", e)
              caseManagementResponse = CaseManagementResponse(e)
              logger.error(e.getStackTrace.mkString("\n"))
            }
          } finally {
            if(null != table) table.close()
          }

          val gson = gsonSharedVariable.get
          val responseJson = gson.toJson(caseManagementResponse)
          logger.info("responseJson - formatted Response Message  - " + new String(responseJson))
          responseJson
          

        }).toList

        

        
        newPartition.iterator


      })

      jsonResult.print()
      logger.info("checking producer connection " +producer.isConnected)

        producer.sendAsync(jsonResult.toString.getBytes)

        logger.info("sent response to pulsar topic  - " + jsonResult)
      
      sc.start()
     

     sc.awaitTerminationOrTimeout(600000)
     producer.wait()
    }
  }
  def getDeviationProperties(spark: SparkSession, config: StreamingConfigVMB): Map[String, String] = {
    // upload this file under files
    val thresholdPropertiesFile = spark.sparkContext.textFile(config.deviationThreshholdPropertiesFilePath)
    //splitting the rdd content from file into key value pairs and storing it
    thresholdPropertiesFile.map(m2 => (m2.split("=")(0), m2.split("=")(1))).collect().toMap
  }

}
