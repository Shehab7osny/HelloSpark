# Loading and Processing Data using Spark
![README Image 1 Edited 1](https://user-images.githubusercontent.com/36346614/99642971-cecb9500-2a54-11eb-9e57-380a16947336.png)

## Contents of the file
* [Project Aim](#Project-Aim)
* [Project Environment](#Project-Environment)
* [Dataset Used](#Dataset-Used)
* [Sample Operations](#Sample-Operations)

## Project Aim
This project mainly aims to provide some samples on how to load data from either a **CSV** or a **Parquet Table** as well as some code snippets for processing this loaded data. The code provided in this project is written in Scala. Apache Spark also provides the ability to write the code in either Python or Java. However, this project was meant to target efficiency, that’s why Scala was used as it provides more efficiency compared to other programming languages mentioned previously especially for spark because spark itself is implemented using Scala.

## Project Environment
In order to set up this project successfully, some prerequisites were required to provide the perfect environment for this project. Here are some of essential tools for our environment:
* ### Cloudera Quickstart VM
  ![README Image 2](https://user-images.githubusercontent.com/36346614/99648499-c7f45080-2a5b-11eb-993d-eb249876194f.png)
  
  This is the virtual machine where most of our work will be done. Cloudera QuickStart virtual machines (VMs) include everything you need to try CDH, Cloudera Manager, Impala, and Cloudera Search. Hence, we will use it to connect with Hive database later on.
  <br/>You can download Cloudera from this [link](https://docs.cloudera.com/documentation/enterprise/5-14-x/topics/cloudera_quickstart_vm.html).

* ### Java Development Kit (JDK) 8
  ![README Image 3](https://user-images.githubusercontent.com/36346614/99648570-d9d5f380-2a5b-11eb-8fff-3c1c3f08207f.png)
  
  This is the software development environment that offers a collection of tools and libraries necessary for developing Java applications. This version is specifically required for writing code in Scala.
  <br/>You can download Cloudera from this [link](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html).
* ### Apache Spark
  ![README Image 4](https://user-images.githubusercontent.com/36346614/99649047-6680b180-2a5c-11eb-85f7-10f7d9b70d38.png)

  This is the main component of our project which will enable us to apply some processing operations on our data.
  <br/>You can download Cloudera from this [link](https://spark.apache.org/downloads.html).

* ### Intellij
  ![README Image 5](https://user-images.githubusercontent.com/36346614/99649706-3554b100-2a5d-11eb-8003-b06806570069.png)
  
  This is the main cross-platform IDE that provides us to write a Scala code to apply some Spark operations.
  <br/>You can download Cloudera from this [link](https://www.jetbrains.com/idea/download/).
  
## Dataset Used

In this project I used a randomly selected dataset from [Kaggle](https://www.kaggle.com/). This dataset is a simple collection of Credit Card Transactions. You can access the dataset from this [link](https://www.kaggle.com/derykurniawan/credit-card-transaction). This dataset consists of the following fields:
| Field Name              | Field Description                                       |
|-------------------------|---------------------------------------------------------|
| Account Number	        |	Represents the customer’s bank account number           |
| Customer ID			        |Represents a unique number for each bank customer        |
| Credit Limit			      |Represents the maximum amount to be withdrawal           |
| Available Money		      |Represents the amount of Debit Balance available         |
| Transaction Date & Time	|Represents the transaction date and time                 |
| Transaction Amount	    |Represents the total amount of the transaction           |
| Merchant Name		        |Represents the name of account accepting payments        |
| Merchant Country		    |Represents the country of account accepting payments     |
| Merchant Category		    |Represents the category of account accepting payments    |
| Expiry Date			        |Represents the expiry date of the Credit Card            |
| Transaction Type		    |Represents the type of the credit card transaction       |
| Is Fraud?			          |Represents whether the transaction is Fraud or not       |

## Sample Operations

In this section, I will provide a sort of visualization some sample operations used in this project. For furthermore operations along with their output results, please check the following [Technical Report](https://docs.google.com/document/d/11fQVFLdn-PzKAS4yYhUaY8kSfog2MFIYcz6i1hpm3vQ/edit?usp=sharing).

* ### Operation #1: Load data from CSV into a Spark Data Frame:

  Code Snippet:
  ```Scala
  val transactions = spark.read.format("csv").option("header", "true").load("/home/transactions.csv")
  ```
  
* ### Operation #2: Load data from Parquet Table into a Spark Data Frame:

  Code Snippet:
  ```Scala
  transactions.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("bdp.hv_parq")
  val df = spark.sql("SELECT * FROM bdp.hv_parq")
  ```

* ### Operation #3: Find out how many Fraud Transactions are there:

  Code Snippet:
  ```Scala
  transactions.groupBy("IsFraud").count().show()
  ```
  Output:
  | IsFraud       | Count       |
  |---------------|-------------|
  | true	        |	11302       |
  | false   			| 630612      |

  Output Chart:
  
  ![README Image 6](https://user-images.githubusercontent.com/36346614/99653159-5cad7d00-2a61-11eb-98b7-eb3da7547dc4.png)
  
* ### Operation #4: Find out the top 5 most merchants (According to number of transactions):

  Code Snippet:
  ```Scala
  transactions.groupBy("Merchant_Name").count().orderBy(desc("count")).limit(5).show()
  ```
  Output:
  | Merchant_Name | Count       |
  |---------------|-------------|
  | Lyft	        |	25311       |
  | Uber			    | 25263       |
  | gap.com       | 13824       |
  | apple.com     | 13607       |
  | target.com    | 13601       |
  

  Output Chart:
  
  ![README Image 7](https://user-images.githubusercontent.com/36346614/99653815-30dec700-2a62-11eb-855c-455692a1ca75.png)

* ### Operation #5: Find out how many types of transactions are available arranged according to popularity:

  Code Snippet:
  ```Scala
  transactions.groupBy("Trans_Type").count().orderBy(desc("count")).show()
  ```
  Output:
  | Trans_Type          | Count  |
  |---------------------|--------|
  | PURCHASE            | 608685 |
  | ADDRESS_VERIFICATION| 16478  |
  | REVERSAL            | 16162  |
  | Others              |   589  |
  

  Output Chart:
  
  ![README Image 8](https://user-images.githubusercontent.com/36346614/99654456-05101100-2a63-11eb-9b38-d5bb6810081e.png)
  
* ### Operation #6: Find out the number of customers that go to the gym and eat fast food:

  Code Snippet:
  ```Scala
  transactions.createOrReplaceTempView("TransactionsTable")
  val snippet = spark.sql(
  """SELECT COUNT(*) FROM (
  (SELECT customerId FROM TransactionsTable WHERE merchantCategoryCode='gym' 
  GROUP BY customerId) 
  INTERSECT 
  (SELECT customerId FROM TransactionsTable WHERE merchantCategoryCode='fastfood' GROUP BY customerId))""")
  val results = snippet.collect()
  results.foreach(println)
  ```
  Output:
  ```Scala
  90
  ```
  
* ### Operation #7: Find out the customers who tried McDonalds before Hardee's and never tried Five Guys:

  Code Snippet:
  ```Scala
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
      FROM McDonalds_Table JOIN Hardees_Table
          ON McDonalds_Table.customerId = Hardees_Table.customerId
      WHERE McDonalds_Table.startDate < Hardees_Table.startDate
      """)
 
  Mc_Hardees.createOrReplaceTempView("McHardees")
 
  val result = spark.sql(
  """
  (SELECT customerId
        FROM McHardees)
        EXCEPT
        (SELECT customerId
        FROM TransactionsTable
        WHERE merchantName LIKE 'Five Guys%'
        GROUP BY customerId)
    """)
 
  result.show()
  ```
  Output:
  |customerId|
  |----------|
  | 847174168|
  | 667315366|
  | 477081008|

* ### Operation #8: Find out top 3 customers total spending on food/fast food/food delivery on each month:

  Code Snippet:
  ```Scala
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
        Select Month, customerId, TotalAmount, row_number() over(partition by Month
		ORDER BY Month, TotalAmount Desc) as rn
        FROM(
        SELECT  Month, customerId, Round(Sum(transactionAmount), 2) as TotalAmount
        FROM    FoodCustomers
        GROUP BY Month, customerId) as Rank) as Result
        WHERE rn <= 3
      """)
      
  result.show()
  ```
  Output:
  |Month|customerId|TotalAmount|
  |-----|----------|-----------|
  |    1| 314506271|   43343.01|
  |    1| 456044564|   41831.58|
  |    1| 772212779|   38482.51|
  |    2| 314506271|   36458.69|
  |    2| 772212779|   31741.49|

