package com.mxbi.adServer.jobs

import com.mxbi.adServer.services.SparkJob
import com.mxbi.adServer.utils.MxCohortUsersUpdateConfig.mxCohortUsersUpdateConfig
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

object MxCohortUsersUpdateJob {

  def main(args: Array[String]): Unit = {
    // TODO *** add logging

    println("                                         ")
    println("*** ! MxCohort Users Update application config properties !***")
    println("                                         ")


    println(
      "application name: " + mxCohortUsersUpdateConfig.appName + "\n\n" +
        "input bucket: " + mxCohortUsersUpdateConfig.s3Details.inputBucketName + "\n" +
        "input s3 path: " + mxCohortUsersUpdateConfig.s3Details.inputDirectory + "\n\n" +
        "output bucket: " + mxCohortUsersUpdateConfig.s3Details.processedBucketName + "\n" +
        "output s3 path: " + mxCohortUsersUpdateConfig.s3Details.processedDirectory + "\n\n" +
        "redshift url: " + mxCohortUsersUpdateConfig.redshift.jdbcUrl + "\n" +
        "redshift table: " + mxCohortUsersUpdateConfig.redshift.tableName + "\n\n"
    )

    val obj = new MxCohortUsersUpdateJob()
    val spark = obj.getSparkSession()
    obj.run(spark)
    spark.stop()
  }
}

class MxCohortUsersUpdateJob() extends SparkJob with Logging {

  override def getSparkConfig(): SparkConf = {
    new SparkConf()
      .setAppName(mxCohortUsersUpdateConfig.appName)
      .setIfMissing("spark.master", s"local[*]")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      //.set("spark.streaming.stopGracefullyOnShutdown", "true")
  }

  //val toInt    = udf[Int, String]( _.toInt)

  override def run(spark: SparkSession): Unit = {

    // config properties

    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "1500")

    val histIds = "select distinct advertiseid from mx_adserver_newusers_test"

    val deltaIds = """
     select distinct start_date,advertiseid,platform,networktype,packagename,versionname,osname,city,regionname,countryname,
     model,manufacturer,installmarket,internalnetworkstatus
     from engagement_table_v2
     where event='appOpened'
     and to_date(bucketpath, 'yyyy-MM-dd') = current_date-1
     """

    val histDf = spark.read
      .format("jdbc")
      .option("url", "jdbc:<URL>")
      .option("driver","com.amazon.redshift.jdbc42.Driver")
      .option("query", histIds)
      .option("user", "user")
      .option("password", "pwd")
      .load()

    val deltaDf = spark.read
      .format("jdbc")
      .option("url", "jdbc:<URL>")
      .option("driver","com.amazon.redshift.jdbc42.Driver")
      .option("query", deltaIds)
      .option("user", "user")
      .option("password", "pwd")
      .load()

    deltaDf.registerTempTable("delta_table")

    val updateDf = deltaDf.select(col("advertiseid")).distinct

    val newUsersDf = updateDf.join(histDf,updateDf("advertiseid") === histDf("advertiseid"),"leftanti")

    newUsersDf.registerTempTable("new_users")

    val insertStr = s"""
select distinct advertiseid,platform,networktype,packagename,versionname,osname,city,regionname,countryname,
model,manufacturer,installmarket,internalnetworkstatus,
min(start_date)over(partition by advertiseid) as app_open_date
from delta_table
where advertiseid in ( select distinct advertiseid from new_users )
"""

    val insertDf = spark.sql(insertStr)

    insertDf
      .write
      .format("jdbc")
      .option("url","jdbc:<URL>")
      .option("driver","com.amazon.redshift.jdbc42.Driver")
      .option("dbtable","mx_adserver_newusers_test")
      .option("user","users")
      .option("password","pwd")
      .mode("append")
      .save()
  }

}
