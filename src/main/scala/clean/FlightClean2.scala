package clean

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset


object FlightClean2{


  case class Flight(dofM: Integer, dofW: Integer, fldate: String, carrier: String, flnum: String, origin: String, dest: String, crsdephour: Integer, crsdeptime: Integer, depdelay: Double, crsarrtime: Integer, arrdelay: Double, crselapsedtime: Double, dist: Double)

  val schema = StructType(Array(
    StructField("dofM", IntegerType, true),
    StructField("dofW", IntegerType, true),
    StructField("fldate", StringType, true),
    StructField("carrier", StringType, true),
    StructField("flnum", StringType, true),
    StructField("origin", StringType, true),
    StructField("dest", StringType, true),
    StructField("crsdephour", IntegerType, true),
    StructField("crsdeptime", IntegerType, true),
    StructField("depdelay", DoubleType, true),
    StructField("crsarrtime", IntegerType, true),
    StructField("arrdelay", DoubleType, true),
    StructField("crselapsedtime", DoubleType, true),
    StructField("dist", DoubleType, true)
  ))

 case class FlightwId(_id: String, dofW: Integer, carrier: String, origin: String, dest: String, crsdephour: Integer, crsdeptime: Integer, depdelay: Double, crsarrtime: Integer, arrdelay: Double, crselapsedtime: Double, dist: Double) extends Serializable
  
  def createFlightwId(f: Flight): FlightwId = {
    val id = f.carrier + '_' + f.fldate + '_' + f.origin + '_' + f.dest + '_' + f.flnum
    FlightwId(id, f.dofW, f.carrier, f.origin, f.dest, f.crsdephour, f.crsdeptime,f.depdelay, f.crsarrtime, f.arrdelay, f.crselapsedtime, f.dist)
  }

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("churn").getOrCreate()


    import spark.implicits._
   
    val df: Dataset[Flight] = spark.read.option("inferSchema", "false").schema(schema).json("/user/user01/data/flightsjan2017.json").as[Flight]

    df.first()
    df.count()
    df.cache()

    df.createOrReplaceTempView("flights")

    val df2: Dataset[FlightwId] = df.map(createFlightwId)
    df2.show
  
    
     df2.write.format("json").save("/user/user01/flightid.json")

  }
}

