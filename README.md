# HelloSpark
### Loading and Processing Data using Spark (Scala)

## Table of contents
* [Introduction](#Introduction)
* [Project Aim](#Aim)
* [Project Technical Details](#Details)

## Introduction
Apache Spark is a lightning-fast cluster computing technology, designed for fast computation. It is based on Hadoop MapReduce and it extends the MapReduce model to efficiently use it for more types of computations, which includes interactive queries and stream processing. The main feature of Spark is its in-memory cluster computing that increases the processing speed of an application.

Spark is designed to cover a wide range of workloads such as batch applications, iterative algorithms, interactive queries and streaming. Apart from supporting all these workload in a respective system, it reduces the management burden of maintaining separate tools.

## Aim
This project aims to provide some examples on how to load data from either a csv or a parquet table. It provides different approaches for each category as well as different queries applied to facilitate the uasage of spark to access and process data efficiently. This is the [dataset](https://www.kaggle.com/derykurniawan/credit-card-transaction) used for the examples mentioned previously and [here](https://docs.google.com/document/d/11fQVFLdn-PzKAS4yYhUaY8kSfog2MFIYcz6i1hpm3vQ/edit?usp=sharing) is the project documentation.

## Details
This project is implemented using scala. Scala provides more efficiency compared to python code especially for spark because spark itself is implemented using Scala.

Some of the main spark features used are:
* SparkSession()
* saveAsTable()
* insertInto()
