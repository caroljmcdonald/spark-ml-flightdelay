package ml

import org.apache.spark._
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Flight {

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

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("churn").getOrCreate()

    import spark.implicits._
    val df: Dataset[Flight] = spark.read.option("inferSchema", "false").schema(schema).json("/user/user01/data/flights20170102.json").as[Flight]
    println("training dataset")

    df.first()
    df.count()
    df.createOrReplaceTempView("flights")
    spark.catalog.cacheTable("flights")
    df.show

    val test = spark.read.option("inferSchema", "false").schema(schema).json("/user/user01/data/flights20170304.json").as[Flight]
    println("test dataset")
    test.count()
    test.first()
    test.show

    println("what is the average departure delay by day of the week")
    spark.sql("SELECT dofW, avg(depdelay) as avgdelay FROM flights GROUP BY dofW ORDER BY avgdelay desc").show

    println("what is the count of departure delay by origin airport where delay minutes >40")
    spark.sql("select origin, count(depdelay) from flights where depdelay > 40 group by origin ORDER BY count(depdelay) desc").show

    val delaybucketizer = new Bucketizer().setInputCol("depdelay")
      .setOutputCol("delayed").setSplits(Array(0.0, 40.0, Double.PositiveInfinity))
    val df4 = delaybucketizer.transform(df)
    df4.groupBy("delayed").count.show
    df4.createOrReplaceTempView("flight")

    println("what is the count of departure delay and not delayed by origin")
    spark.sql("select origin, delayed, count(delayed) from flight group by origin, delayed order by origin").show

    println("what is the count of departure delay by dest")
    spark.sql("select dest, delayed, count(delayed) from flight where delayed=1 group by dest, delayed order by dest").show
    println("what is the count of departure delay by origin, dest")
    spark.sql("select origin,dest, delayed, count(delayed) from flight where delayed=1 group by origin,dest, delayed order by origin,dest").show
    println("what is the count of departure delay by dofW")
    spark.sql("select dofW, delayed, count(delayed) from flight where delayed=1 group by dofW, delayed order by dofW").show

    println("what is the count of departure delay by hour where delay minutes >40")
    spark.sql("select crsdephour, delayed, count(delayed) from flight where delayed=1 group by crsdephour, delayed order by crsdephour").show

    println("what is the count of departure delay carrier where delay minutes >40")
    spark.sql("select carrier, delayed, count(delayed) from flight where delayed=1 group by carrier, delayed order by carrier").show

    val fractions = Map(0.0 -> .29, 1.0 -> 1.0)
    val strain = df4.stat.sampleBy("delayed", fractions, 36L)
    strain.groupBy("delayed").count.show

    val categoricalColumns = Array("carrier", "origin", "dest", "dofW")

    val stringIndexers = categoricalColumns.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(df)
    }
    val encoders = categoricalColumns.map { colName =>
      new OneHotEncoder()
        .setInputCol(colName + "Indexed")
        .setOutputCol(colName + "Enc")
    }

    val labeler = new Bucketizer().setInputCol("depdelay")
      .setOutputCol("label")
      .setSplits(Array(0.0, 40.0, Double.PositiveInfinity))
    val featureCols = Array("carrierEnc", "destEnc", "originEnc",
      "dofWEnc", "crsdephour", "crselapsedtime", "crsarrtime", "crsdeptime", "dist")
    //put features into a feature vector column   
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val dTree = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features").setMaxBins(7000)
    val steps = stringIndexers ++ encoders ++ Array(labeler, assembler, dTree)

    val pipeline = new Pipeline().setStages(steps)

    val paramGrid = new ParamGridBuilder().addGrid(dTree.maxDepth, Array(4, 5, 6)).build()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label").setPredictionCol("prediction")
      .setMetricName("accuracy")

    // Set up 3-fold cross validation with paramGrid
    val crossval = new CrossValidator().setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid).setNumFolds(3)

    val ntrain = strain.drop("delayed").drop("arrdelay")
    println(ntrain.count)
    ntrain.show
    val cvModel = crossval.fit(ntrain)

    // note if you want to do analyze the decision tree it is easier not to use OneHotEncoding

    //  val featureImportances = bestModel.stages.last.asInstanceOf[RandomForestRegressionModel].featureImportances.toArray

    //  val treeModel = cvModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages.last.asInstanceOf[DecisionTreeClassificationModel]

    // val featureImportances = treeModel.featureImportances.toArray

    // val fis = s"features importances:\n ${featureCols.zip(featureImportances).map(t => s"\t${t._1} = ${t._2}").mkString("\n")}\n"
    // println(fis)

    val predictions = cvModel.transform(test)

    val accuracy = evaluator.evaluate(predictions)

    val lp = predictions.select("label", "prediction")
    val counttotal = predictions.count()
    val label0count = lp.filter($"label" === 0.0).count()
    val pred0count = lp.filter($"prediction" === 0.0).count()
    val label1count = lp.filter($"label" === 1.0).count()
    val pred1count = lp.filter($"prediction" === 1.0).count()

    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    val truep = lp.filter($"prediction" === 0.0)
      .filter($"label" === $"prediction").count() / counttotal.toDouble
    val truen = lp.filter($"prediction" === 1.0)
      .filter($"label" === $"prediction").count() / counttotal.toDouble
    val falsep = lp.filter($"prediction" === 0.0)
      .filter(not($"label" === $"prediction")).count() / counttotal.toDouble
    val falsen = lp.filter($"prediction" === 1.0)
      .filter(not($"label" === $"prediction")).count() / counttotal.toDouble

    println("ratio correct", ratioCorrect)

    predictions.createOrReplaceTempView("flight")

    println("what is the count of predicted delay/notdelay by scheduled departure hour")
    spark.sql("select crsdephour, prediction, count(prediction) from flight group by crsdephour, prediction order by crsdephour").show
    println("what is the count of predicted delay/notdelay by origin")
    spark.sql("select origin, prediction, count(prediction) from flight group by origin, prediction order by origin").show
    println("what is the count of predicted delay/notdelay by destination")
    spark.sql("select dest, prediction, count(prediction) from flight group by dest, prediction order by dest").show
    println("what is the count of predicted delay/notdelay by origin,dest")
    spark.sql("select origin,dest, prediction, count(prediction) from flight group by origin,dest, prediction order by origin,dest").show
    println("what is the count of predicted delay/notdelay by day of the week")
    spark.sql("select dofW, prediction, count(prediction) from flight group by dofW, prediction order by dofW").show
    println("what is the count of predicted delay/notdelay by carrier")
    spark.sql("select carrier, prediction, count(prediction) from flight group by carrier, prediction order by carrier").show

    // cvModel.write.overwrite().save("/user/user01/data/cfModel")

  }
}

