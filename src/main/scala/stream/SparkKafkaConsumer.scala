package stream

import org.apache.spark._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka09.{ ConsumerStrategies, KafkaUtils, LocationStrategies }
import org.apache.spark.streaming.kafka.producer._

import org.apache.kafka.clients.consumer.ConsumerConfig

/*

*/
object SparkKafkaConsumer {

  // schema for flight data   
  case class FlightwPred(_id: String, dofW: Integer, carrier: String, origin: String,
    dest: String, crsdephour: Integer, crsdeptime: Double, crsarrtime: Double, crselapsedtime: Double,
    label: Double, pred_dtree: Double) extends Serializable

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
    StructField("label", DoubleType, true),
    StructField("pred_dtree", DoubleType, true)
  ))

  def main(args: Array[String]) = {

    var topicp = "/user/user01/stream:flightp"
    if (args.length == 1) {
      topicp = args(0)
    } else {
      System.out.println("Using hard coded parameters unless you specify the publish topic. For example  /user/user01/stream:flightp ")
    }

    val groupId = "testgroup"
    val offsetReset = "earliest"
    val pollTimeout = "5000"
    val Array(topicc) = args
    val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka

    val sparkConf = new SparkConf()
      .setAppName(SparkKafkaConsumer.getClass.getName)

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicsSet = topicc.split(",").toSet

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    val messagesDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy
    )

    val valuesDStream = messagesDStream.map(_.value())

    valuesDStream.foreachRDD { (rdd: RDD[String], time: Time) =>
      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._

        import org.apache.spark.sql.functions._
        val df: Dataset[FlightwPred] = spark.read.schema(schema).json(rdd).as[FlightwPred]
        println("dstream dataset")
        df.show
        df.createOrReplaceTempView("flight")
        println("what is the count of predicted delay/notdelay for this dstream dataset")
        df.groupBy("pred_dtree").count().show()

        println("what is the count of predicted delay/notdelay by scheduled departure hour")
        spark.sql("select crsdephour, pred_dtree, count(pred_dtree) from flight group by crsdephour, pred_dtree order by crsdephour").show
        println("what is the count of predicted delay/notdelay by origin")
        spark.sql("select origin, pred_dtree, count(pred_dtree) from flight group by origin, pred_dtree order by origin").show
        println("what is the count of predicted and actual  delay/notdelay by origin")
        spark.sql("select origin, pred_dtree, count(pred_dtree),label, count(label) from flight group by origin, pred_dtree, label order by origin, label, pred_dtree").show
        println("what is the count of predicted delay/notdelay by dest")
        spark.sql("select dest, pred_dtree, count(pred_dtree) from flight group by dest, pred_dtree order by dest").show
        println("what is the count of predicted delay/notdelay by origin,dest")
        spark.sql("select origin,dest, pred_dtree, count(pred_dtree) from flight group by origin,dest, pred_dtree order by origin,dest").show
        println("what is the count of predicted delay/notdelay by day of the week")
        spark.sql("select dofW, pred_dtree, count(pred_dtree) from flight group by dofW, pred_dtree order by dofW").show
        println("what is the count of predicted delay/notdelay by carrier")
        spark.sql("select carrier, pred_dtree, count(pred_dtree) from flight group by carrier, pred_dtree order by carrier").show

      }
    }

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
