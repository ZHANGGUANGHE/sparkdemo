package com.example.demofirst


import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._


class SparkDemoScalaTest {
  @Test
  def getDataDF() {
  }

  @Test
  def getDataDF2() {
  }

  @Test
  def getSparkSession(): Unit = {
    val spark = SparkDemo.getSparkSession()
    spark.stop()
  }

  @Test
  def countDate(): Unit = {
    val spark = SparkDemo.getSparkSession()
    val dataDF = SparkDemo.getDataDF2(spark)
    SparkDemo.countDate(spark,5,dataDF)
    println("==============")
    SparkDemo.countDate(spark,7,dataDF)
    spark.stop()
  }
}
