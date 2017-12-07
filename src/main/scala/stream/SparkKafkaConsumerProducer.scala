package stream

// http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams.html

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka09.{ ConsumerStrategies, KafkaUtils, LocationStrategies }
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.producer._

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Consumes messages from a topic in MapR Streams using the Kafka interface,
 * enriches the message with  the k-means model cluster id and publishs the result in json format
 * to another topic
 * Usage: SparkKafkaConsumerProducer  <model> <topicssubscribe> <topicspublish>
 *
 *   <model>  is the path to the saved model
 *   <topics> is a  topic to consume from
 *   <topicp> is a  topic to publish to
 * Example:
 *    $  spark-submit --class com.sparkkafka.flight.SparkKafkaConsumerProducer --master local[2] \
 * mapr-sparkml-streaming-flight-1.0.jar /user/user01/data/savemodel  /user/user01/stream:flights /user/user01/stream:flightp
 *
 *    for more information
 *    http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams_Consume.html
 */

object SparkKafkaConsumerProducer extends Serializable {

  import org.apache.spark.streaming.kafka.producer._

  // schema for flight data   
  case class Flight(_id: String, dofW: Integer, carrier: String, origin: String,
    dest: String, crsdephour: Integer, crsdeptime: Double, depdelay: Double,
    crsarrtime: Double, arrdelay: Double, crselapsedtime: Double, dist: Double)
    extends Serializable

  val schema = StructType(Array(
    StructField("_id", StringType, true),
    StructField("dofW", IntegerType, true),
    StructField("carrier", StringType, true),
    StructField("origin", StringType, true),
    StructField("dest", StringType, true),
    StructField("crsdephour", IntegerType, true),
    StructField("crsdeptime", DoubleType, true),
    StructField("depdelay", DoubleType, true),
    StructField("crsarrtime", DoubleType, true),
    StructField("arrdelay", DoubleType, true),
    StructField("crselapsedtime", DoubleType, true),
    StructField("dist", DoubleType, true)
  ))
  def main(args: Array[String]): Unit = {
    var modelpath = "/user/user01/data/cfModel"
    var topics = "/user/user01/stream:flights"
    var topicp = "/user/user01/stream:flightp"

    if (args.length == 3) {
      modelpath = args(0)
      topics = args(1)
      topicp = args(2)
    } else {
      System.out.println("Using hard coded parameters unless you specify the model path, subscribe topic and publish topic. For example /user/user01/data/cfModel /user/user01/stream:flights /user/user01/stream:flightp ")
    }

    System.out.println("Use model " + modelpath + " Subscribe to : " + topics + " Publish to: " + topicp)

    val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka
    val groupId = "sparkApplication"
    val batchInterval = "2"
    val pollTimeout = "10000"

    val sparkConf = new SparkConf().setAppName("UberStream")
    val spark = SparkSession.builder().appName("ClusterUber").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchInterval.toInt))

    import spark.implicits._

    val producerConf = new ProducerConf(
      bootstrapServers = brokers.split(",").toList
    )
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      "spark.kafka.poll.time" -> pollTimeout,
      "spark.streaming.kafka.consumer.poll.ms" -> "8192"
    )

    val model = CrossValidatorModel.load(modelpath)

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    val messagesDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy
    )
    // get message values from key,value
    val valuesDStream: DStream[String] = messagesDStream.map(_.value())

    valuesDStream.foreachRDD { rdd =>

      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        // Get the singleton instance of SparkSession
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        import org.apache.spark.sql.functions._
        val df: Dataset[Flight] = spark.read.schema(schema).json(rdd).as[Flight]
        println("show dstream dataset received ")
        df.show

        // get cluster categories from  model
        val predictions = model.transform(df)
        println("show dstream dataset received with predictions ")
        predictions.show()
        //  categories.show
        predictions.createOrReplaceTempView("flights")

        // convert results to JSON string to send to topic 

        val lp = predictions.select($"_id", $"dofW", $"carrier", $"origin", $"dest",
          $"crsdephour", $"crsdeptime", $"crsarrtime", $"crselapsedtime", $"label",
          $"prediction".alias("pred_dtree"))
        println("show predictions fordataframe received ")

        lp.show
        lp.createOrReplaceTempView("flight")

        println("what is the count of predicted  by origin")

        spark.sql("select origin, pred_dtree, count(pred_dtree) from flight group by origin, pred_dtree order by origin").show

        val tRDD: org.apache.spark.sql.Dataset[String] = lp.toJSON

        val temp: RDD[String] = tRDD.rdd
        temp.sendToKafka[StringSerializer](topicp, producerConf)

        println("show 2 messages sent")
        temp.take(2).foreach(println)
      }
    }

    // Start the computation
    println("start streaming")
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}