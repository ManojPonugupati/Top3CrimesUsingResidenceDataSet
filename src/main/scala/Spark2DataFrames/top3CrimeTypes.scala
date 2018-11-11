package Spark2DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object top3CrimeTypes {
  def main(args: Array[String]){
    val spark = SparkSession
      .builder()
      .appName("Get_Revenue_Per_OrderDF")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    val inputDf = spark.read.option("inferSchema","true").option("header","true").csv("C:\\data\\crime_data_test.csv")
    val ResdenceDF = inputDf.withColumnRenamed("Location Description","Location_Description")
      .where("Location_Description == 'RESIDENCE' ").withColumnRenamed("Primary Type","Primary_Type")
        .groupBy("Primary_Type").count().orderBy(desc("count")).limit(3)

    ResdenceDF.show()
  }

}
