package clean

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset

object FlightClean {

  case class Flight(dofM: Integer, dofW: Integer, fldate: String, carrier: String, flnum: String, origin: String, dest: String, crsdephour: Long, crsdeptime: Integer, depdelay: Double, crsarrtime: Integer, arrdelay: Double, crselapsedtime: Double, dist: Double)

  val schema = StructType(Array(
    StructField("DAY_OF_MONTH", IntegerType, true),
    StructField("DAY_OF_WEEK", IntegerType, true),
    StructField("FL_DATE", DateType, true),
    StructField("CARRIER", StringType, true),
    StructField("FL_NUM", StringType, true),
    StructField("ORIGIN", StringType, true),
    StructField("DEST", StringType, true),
    StructField("CRS_DEP_TIME", IntegerType, true),
    StructField("DEP_DELAY_NEW", DoubleType, true),
    StructField("CRS_ARR_TIME", IntegerType, true),
    StructField("ARR_DELAY_NEW", DoubleType, true),
    StructField("CRS_ELAPSED_TIME", DoubleType, true),
    StructField("DISTANCE", DoubleType, true)
  ))

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("churn").getOrCreate()

    import spark.implicits._
    val df1 = spark.read.format("com.databricks.spark.csv").schema(schema)
      .option("header", "true").option("treatEmptyValuesAsNulls", "true").option("dateFormat", "yyyy-MM-dd").load("/user/user01/apr.csv")
    df1.take(1)
    df1.count()
    val df2 = df1.na.drop
    df2.count()
    val toDouble = udf { s: Integer => (s / 100.0).round }
    val df = df2.withColumn("crsdephour", toDouble(df2("CRS_DEP_TIME")))
    df.count

    df.createOrReplaceTempView("flights")


    val ds: Dataset[Flight] = spark.sql("select DAY_OF_MONTH as dofM, DAY_OF_WEEK as dofW, CARRIER as carrier,FL_DATE as fldate , FL_NUM as flnum, ORIGIN as origin, DEST as dest,crsdephour as crsdephour, CRS_DEP_TIME  as crsdeptime, DEP_DELAY_NEW as depdelay, CRS_ARR_TIME as crsarrtime ,ARR_DELAY_NEW as arrdelay, CRS_ELAPSED_TIME as crselapsedtime , DISTANCE as dist   from flights").as[Flight]
    println("ds :" + ds.count() + "ds :" + df1.count())

    ds.show
    ds.createOrReplaceTempView("flights")

    val df5 = ds.filter("origin in ('ORD', 'JKF', 'LGA', 'EWR','BOS', 'IAH', 'ATL', 'DEN', 'SFO', 'MIA')")

    val df6 = df5.filter("dest in ('ORD', 'JKF', 'LGA', 'EWR','BOS', 'IAH', 'ATL', 'DEN', 'SFO', 'MIA')")

    val df7 = df6.filter("carrier in ('AA', 'UA', 'DL', 'WN')")
    df7.show

    println("ds :" + ds.count() + "ds :" + df7.count())

    df7.createOrReplaceTempView("flights")

    df7.write.format("json").save("/user/user01/apr2017")

  }
}

