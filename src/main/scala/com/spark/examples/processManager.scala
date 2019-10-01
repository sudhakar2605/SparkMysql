package com.spark.examples

import java.util.Properties
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.spark.examples.common.dbUtils
import java.sql.{Connection,DriverManager,Statement}

object processManager {
  def createSparkSession: SparkSession = {
       val spark = SparkSession.builder().master("local[*]").appName("EmployeeApplication").getOrCreate()
         spark.sparkContext.setLogLevel("ERROR")
          spark
  }

  def sparkDataReader(spark:SparkSession,sourceTable:String,table:String,envConfig:Config) = {
    val properties = new Properties()
    val sourceJdbcUrl = envConfig.getString(s"employee.${table}.source.JdbcUrl")
    val sourceUsername = envConfig.getString(s"employee.${table}.source.Username")
    val sourcePassword = envConfig.getString(s"employee.${table}.source.Password")

    println("Source JDBC Url "+sourceJdbcUrl)
    println("Source Username "+sourceUsername)
    println("Source Password "+sourcePassword)

    properties.setProperty("user","root")
    properties.setProperty("password","admin")
    val df = spark.read.jdbc(sourceJdbcUrl,sourceTable,properties)
    println(sourceTable)
    df.createOrReplaceTempView(sourceTable.split("\\.")(1))
    df.printSchema()
    println(df.count())
  }

  def sparkStagingDataWriter(stagingDF:DataFrame, stagingTable:String, table:String, envConfig:Config)={

      val properties = new Properties()
      val targetJdbcUrl = envConfig.getString(s"employee.${table}.target.JdbcUrl")
      val targetUsername = envConfig.getString(s"employee.${table}.target.Username")
      val targetPassword = envConfig.getString(s"employee.${table}.target.Password")

      println("Target JDBC Url "+targetJdbcUrl)
      println("Target Username "+targetUsername)
      println("Target Password "+targetPassword)

      properties.setProperty("user","root")
      properties.setProperty("password","admin")
      //properties.setProperty("driver","com.mysql.jdbc.Driver")
    stagingDF.write.option("numPartitions",1).mode(SaveMode.Overwrite).jdbc(targetJdbcUrl,stagingTable,properties)

  }

  def processTable(BatchID:String,appConfig:Config,envConfig:Config,table:String) = {

    var spark:SparkSession= null
    var targetConnection:Connection = null
    try {
      val sourceTables = appConfig.getStringList(s"employee.${table}.sourceTable").toArray()
      val stagingTable = appConfig.getString(s"employee.${table}.stagingTable")
      val targetTable = appConfig.getString(s"employee.${table}.targetTable")

      println("Source Tables " + sourceTables.foreach(println))
      println("Staging Table " + stagingTable)
      println("Target Table " + targetTable)
      val targetJdbcUrl = envConfig.getString(s"employee.${table}.target.JdbcUrl")

      targetConnection = dbUtils.getConnection(targetJdbcUrl)

      println("Create Spark Session")
      spark = createSparkSession

      println("Reading Source Tables")
      sourceTables.foreach(sourceTable => sparkDataReader(spark, sourceTable.toString, table, envConfig))

      println("Truncating staging table "+ stagingTable)
      val truncateQuery = "TRUNCATE TABLE "+ stagingTable
      dbUtils.executeDDL(targetConnection,truncateQuery)

      println("Inserting into staging table "+ stagingTable)
      val stagingQuery = appConfig.getString(s"employee.queries.stagingQueries.${table}").replace("BATCHID", s"'${BatchID}'")
      println("Staging Query \n")
      println(stagingQuery)

      val stagingDF = spark.sql(stagingQuery)
      stagingDF.show()
      sparkStagingDataWriter(stagingDF, stagingTable.toString, table, envConfig)

      println("Merging staging data with target table "+ targetTable)

      val targetMergeQuery = appConfig.getString(s"employee.queries.targetQueries.${table}").replace("BATCHID", s"'${BatchID}'")
      dbUtils.executeUpdate(targetConnection, targetMergeQuery)
    }
    catch {
      case e:Exception => {
        println("Expection in ProcessManager")
        spark.close()
        if(targetConnection != null){
          targetConnection.close
          throw new RuntimeException(e)
        }
      }
    }

  }
}
