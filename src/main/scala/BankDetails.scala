package BankDetails

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}

object BankDetails {

  case class Transaction(Account_Number:Int,
                         Customer_ID:Int,
                         Credit_Limit:Int,
                         Available_Balance:Float,
                         Trans_Date:String,
                         Trans_Time:String,
                         Trans_Amount:Float,
                         Merchant_Name:String,
                         Merchant_Country:String,
                         Merchant_Category:String,
                         Expiration_Date:String,
                         Trans_Type:String,
                         IsFraud:Boolean)

  def mapper(line:String): Transaction = {

    val fields = line.split(',')

    val transaction:Transaction = {
      Transaction(
        fields(0).toInt,
        fields(1).toInt,
        fields(2).toInt,
        fields(3).toFloat,
        fields(4).substring(0, 10),
        fields(4).substring(11),
        fields(5).toFloat,
        fields(6),
        fields(8),
        fields(11),
        fields(12).replace('/', '-'),
        fields(18),
        fields(19).toBoolean
      )
    }

    transaction
  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
/*
    // Convert our csv file to a DataSet
    import spark.implicits._
    val lines = spark.sparkContext.textFile("/home/cloudera/Desktop/transactions.csv")
    val headingRow = lines.first()
    val dataRows = lines.filter(row => row != headingRow)
    val transactions = dataRows.map(mapper).toDS().cache()

    //println("Filter out anyone over 21:")
    //transactions.filter(transactions("age") < 21).show()

    println("Group by age:")
    transactions.select("Customer_ID", "Merchant_Name", "Trans_Date")
      .filter(transactions("Merchant_Country") === "CAN" &&
        transactions("IsFraud") === true  &&
        transactions("Merchant_Category") === "fastfood").show()
*/
    val transactionsDataFrame = spark.read.format("csv").option("header", "true").load("/home/cloudera/Desktop/transactions.csv")
        .withColumn("accountNumber",col("accountNumber").cast("Integer"))
        .withColumn("customerId",col("customerId").cast("Integer"))
        .withColumn("creditLimit",col("creditLimit").cast("Integer"))
        .withColumn("availableMoney",col("availableMoney").cast("Float"))
        .withColumn("transactionAmount",col("transactionAmount").cast("Float"))
        .withColumn("isFraud",col("isFraud").cast("Boolean"))
        .withColumn("transactionDate",split(col("transactionDateTime"),"T").getItem(0))
        .withColumn("transactionTime",split(col("transactionDateTime"),"T").getItem(1))
        .drop("transactionDateTime")

    //transactionsDataFrame.printSchema()

    transactionsDataFrame.createOrReplaceTempView("TransactionsTable")

    //val snippet = spark.sql("SELECT customerId, merchantName, transactionDate FROM TransactionsTable WHERE isFraud=1 AND merchantCountryCode='CAN' AND merchantCategoryCode='fastfood'")

    //val snippet1 = spark.sql(
    //  "SELECT COUNT(*) FROM ((SELECT customerId FROM TransactionsTable WHERE merchantCategoryCode='gym' GROUP BY customerId) INTERSECT (SELECT customerId FROM TransactionsTable WHERE merchantCategoryCode='fastfood' GROUP BY customerId))")

    val mcdonalds = spark.sql(
      """
      SELECT customerId, MIN(transactionDate) AS startDate
      FROM TransactionsTable
      WHERE merchantName LIKE 'McDonalds%'
      GROUP BY customerId
      ORDER BY MIN(transactionDate)
      """)

    val hardees = spark.sql(
      """
      SELECT customerId, MIN(transactionDate) AS startDate
      FROM TransactionsTable
      WHERE merchantName LIKE "Hardee's%"
      GROUP BY customerId
      ORDER BY MIN(transactionDate)
      """)

    mcdonalds.createOrReplaceTempView("McDonalds_Table")
    hardees.createOrReplaceTempView("Hardees_Table")

    val Mc_Hardees = spark.sql(
      """
        SELECT McDonalds_Table.customerId
          FROM McDonalds_Table INNER JOIN Hardees_Table
            ON McDonalds_Table.customerId = Hardees_Table.customerId
         WHERE McDonalds_Table.startDate < Hardees_Table.startDate
      """)

    Mc_Hardees.createOrReplaceTempView("McHardees")

    val snippet5 = spark.sql(
      """
        (SELECT customerId
        FROM McHardees)
        EXCEPT
        (SELECT customerId
        FROM TransactionsTable
        WHERE merchantName LIKE 'Five Guys%'
        GROUP BY customerId)
      """)

    val distinctMonths = spark.sql("SELECT DISTINCT MONTH(transactionDate) AS Month FROM TransactionsTable ORDER BY Month")
    distinctMonths.createOrReplaceTempView("MonthsTable")

    val snippet9 = spark.sql(
      """
         (SELECT customerId, ROUND(SUM(transactionAmount), 2) as totalAmount
         FROM TransactionsTable
         WHERE MONTH(transactionDate)=1
             AND
         (merchantCategoryCode='fastfood' OR merchantCategoryCode='food' OR merchantCategoryCode='food_delivery')
         GROUP BY customerId
         ORDER BY totalAmount DESC
         LIMIT 3)
      """)

    val foodCustomers = spark.sql(
      """
         SELECT customerId, transactionAmount, MONTH(transactionDate) AS Month
         FROM TransactionsTable
         WHERE
            merchantCategoryCode='fastfood' OR
            merchantCategoryCode='food' OR
            merchantCategoryCode='food_delivery'
         ORDER BY Month
      """)
    foodCustomers.createOrReplaceTempView("FoodCustomers")

    val result = spark.sql(
      """
        Select Month, customerId, TotalAmount FROM (
        Select Month, customerId, TotalAmount, row_number() over(partition by Month ORDER BY Month, TotalAmount Desc) as rn
        FROM(
        SELECT  Month, customerId, Round(Sum(transactionAmount), 2) as TotalAmount
        FROM    FoodCustomers
        GROUP BY Month, customerId) as Rank) as Result
        WHERE rn <= 3
      """)

    //       AVG (price) OVER (
    // 	      PARTITION BY group_name
    //val snippet3 = spark.sql(
    //  "SELECT DISTINCT COUNT(*) FROM (SELECT customerId FROM TransactionsTable WHERE merchantCategoryCode='fastfood' GROUP BY customerId)")

    //WHERE merchantCategoryCode='gym' OR merchantCategoryCode='fastfood'
    result.show()

    //results.foreach(println)

    spark.stop()
  }
}
