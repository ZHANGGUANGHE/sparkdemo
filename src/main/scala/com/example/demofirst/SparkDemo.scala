package com.example.demofirst



import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object SparkDemo {


  def getSparkSession(): SparkSession ={
    val spark = SparkSession
      .builder()
//      设置是否本地运行
      .master("local[*]")
      .appName("Read data From CSV")
      .getOrCreate()
    return spark
  }
  def countDate(sparkSession: SparkSession,paramNum: Int,dataFrame: DataFrame): Unit = {
    val spark = sparkSession
    val dataDF = dataFrame
    //    1.	For each peer_id, get the year when peer_id contains id_2, for example for ‘ABC17969(AB)’ year is 2022.
    import spark.implicits._
    val peeryearResults = dataDF.where($"peer_id".contains($"id_2")).select($"peer_id",$"year")

    //    2.	Given a size number, for example 3. For each peer_id count the number of each year
    //        (which is smaller or equal than the year in step1).
    //    2-1 提取第一步结果得到每个peer_id的最大年份，重命名容易混淆的列名
    val maxyear = peeryearResults.groupBy($"peer_id").max("year").withColumnRenamed("peer_id","peer_id2")

    //    2-2 dataDF原数据跟maxyear表连接，并根据peer_id,year列聚合，根据peer_id统计每个年份出现的次数
    val dataPeerIdYearCount = dataDF.join(maxyear,
      dataDF("peer_id")===maxyear("peer_id2")
        and
        dataDF("year")<=maxyear("max(year)")
    ).groupBy($"peer_id",$"year").count()

    //    3.	Order the value in step 2 by year and check if the count number of the first year is bigger or equal than the given size number.
    //        If yes, just return the year.
    //    If not, plus the count number from the biggest year to next year until the count number is bigger or equal than the given number.

    //    3-1、以 peerId 为组的 list 排序，以年份降序排序，并使用移动窗口逐行累加count值
    val orderByYearSumCount = dataPeerIdYearCount.select(expr("peer_id"),expr("year"),expr("count"),
      expr("sum(count) over (partition by peer_id order by year desc) as sumCount"))

    //    3-2、对排序后的 list 进行过滤，找出需要的年份记录，先设计参数
    //    广播变量paramNumber：传入的参数
    val paramNumber = spark.sparkContext.broadcast(paramNum);
    //    3-3、提取orderByYearSumCount中sumCount大于等于给定值的最小值
    val peerMinsumcount = orderByYearSumCount.where($"sumCount" >= paramNumber.value).groupBy("peer_id").min("sumCount")
      .withColumnRenamed("peer_id","peer_id2").withColumnRenamed("min(sumCount)","minsumcount")
    //    3-4、提取数据中累积到给目标值的数据
    val resultDF = orderByYearSumCount.join(peerMinsumcount,orderByYearSumCount("peer_id")===peerMinsumcount("peer_id2")
      and orderByYearSumCount("sumCount") <=peerMinsumcount("minsumcount")).select("peer_id","year")
//    println("=====question1:peeryearResults=====")
//    peeryearResults.collect().foreach(println)
//    println("=====question2:dataPeerIdYearCount=====")
//    dataPeerIdYearCount.collect().foreach(println)
    println("=====question3:resultDF=====")
    resultDF.collect().foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession()
    val dataDF = getDataDF(spark)
    countDate(spark,3,dataDF)

    spark.stop()
  }
  def getDataDF(sparkSession: SparkSession): DataFrame = {

    val fields = Array(StructField("peer_id", StringType, nullable = true),
      StructField("id_1", StringType, nullable = true),
      StructField("id_2", StringType, nullable = true),
      StructField("year", IntegerType, nullable = true))
    val dataList = List(
      ("ABC17969(AB)", "1", "ABC17969", 2022),
      ("ABC17969(AB)", "2", "CDC52533", 2022),
      ("ABC17969(AB)", "3", "DEC59161", 2023),
      ("ABC17969(AB)", "4", "F43874", 2022),
      ("ABC17969(AB)", "5", "MY06154", 2021),
      ("ABC17969(AB)", "6", "MY4387", 2022),
      ("AE686(AE)", "7", "AE686", 2023),
      ("AE686(AE)", "8", "BH2740", 2021),
      ("AE686(AE)", "9", "EG999", 2021),
      ("AE686(AE)", "10", "AE0908", 2021),
      ("AE686(AE)", "11", "QA402", 2022),
      ("AE686(AE)", "12", "OM691", 2022)
    ).map(f => Row(f._1,f._2,f._3,f._4))

    val dataRDD = sparkSession.sparkContext.parallelize(dataList)
    val schema = StructType(fields)
    val df = sparkSession.createDataFrame(dataRDD,schema)
    return df
  }
  def getDataDF2(sparkSession: SparkSession): DataFrame = {

    val fields = Array(StructField("peer_id", StringType, nullable = true),
      StructField("id_1", StringType, nullable = true),
      StructField("id_2", StringType, nullable = true),
      StructField("year", IntegerType, nullable = true))
    val dataList = List(
      ("AE686(AE)", "7", "AE686", 2022),
      ("AE686(AE)", "8", "BH2740", 2021),
      ("AE686(AE)", "9", "EG999", 2021),
      ("AE686(AE)", "10", "AE0908", 2023),
      ("AE686(AE)", "11", "QA402", 2022),
      ("AE686(AE)", "12", "OA691", 2022),
      ("AE686(AE)", "12", "OB691", 2022),
      ("AE686(AE)", "12", "OC691", 2019),
      ("AE686(AE)", "12", "OD691", 2017)
    ).map(f => Row(f._1,f._2,f._3,f._4))

    val dataRDD = sparkSession.sparkContext.parallelize(dataList)
    val schema = StructType(fields)
    val df = sparkSession.createDataFrame(dataRDD,schema)
    return df
  }

}

