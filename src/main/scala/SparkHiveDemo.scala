import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkHiveDemo {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir","thrift://quickstart.cloudera:9083")
      .enableHiveSupport()
      .getOrCreate()

    //val Table = spark.sql("SELECT * FROM bdp.hv_parq").toDF()

    //val result = Table.groupBy("merchantName").count()
    //  .orderBy(("count")).limit(5).show()

    val transDF = spark.read.format("csv").option("header", "true").load("/home/cloudera/Desktop/transactionsTable.csv")

    transDF.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("bdp.hv_pa rq")

    val df = spark.sql("SELECT * FROM bdp.hv_parq").toDF()

    val results = snippet.collect()

    results.foreach(println)

    //transDF.write.parquet("/home/cloudera/Desktop/zipcodes.parquet")

    //val parqDF = spark.read.parquet("/home/cloudera/Desktop/zipcodes.parquet")

    println("End")
  }
}