package com.spark.examples

import java.util.Properties

import com.spark.examples.common.argumentParser
import com.spark.examples.common.dbUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import java.util.UUID.randomUUID

import scala.util.{Failure, Success, Try}

object SparkEmployeeApplication {

  def main(args: Array[String]):Unit ={

    val argMap = argumentParser.getArgMap(args)

    val parsedArguments = Array(
      argMap.getOrElse("env",null),
      argMap.getOrElse("jobType",null),
      argMap.getOrElse("numPartitions",null)
    )

    if (argumentParser.isArgNull(argMap)){
      throw new RuntimeException(
        s"""
           |env = ${parsedArguments(0)},
           |jobType = ${parsedArguments(1)},
           |numPartitions = ${parsedArguments(2)}
           |""".stripMargin
      )
    }
    println(
      s"""
          |env = ${parsedArguments(0)},
          |jobType = ${parsedArguments(1)},
          |numPartitions = ${parsedArguments(2)}
         |""".stripMargin
    )

    val env = parsedArguments(0)
    val jobType = parsedArguments(1)
    val numPartitions = parsedArguments(2)

    val envConfigFile = env match {
      case "dev" => s"dev/env.conf"
      case "qa" => s"qa/env.conf"
      case "prod" => s"prod/env.conf"
    }

    println("Loading Config File ::"+ envConfigFile)
    val envConfig = ConfigFactory.load(envConfigFile)


    println("Reading application Config File")
    val appConfig = ConfigFactory.load("application/employee.conf")

    if (jobType == null){
      println("Missing jobType. Terminating process")
      throw new RuntimeException
    }
    else {
      val jobTypeList = appConfig.getStringList(s"employee.JobType").toArray()

      val BatchID = randomUUID.toString

      println(s"Validating Job Type ${jobType}")
      if (jobTypeList.contains(jobType)){
          val tableList = appConfig.getStringList(s"employee.${jobType}").toArray()
          tableList.foreach( table => {
            println("----------------------------------------------")
            println("Processing Table "+ table)
            println("----------------------------------------------")

            Try(processManager.processTable(BatchID,appConfig,envConfig,table.toString))
            match {
              case Success(s) => {
                println("----------------------------------------------")
                println("Table " + table + " processed successfully")
                println("----------------------------------------------")

              }
              case Failure(f) => {
                println("----------------------------------------------")
                println("Failed to process Table " + table + "")
                println("----------------------------------------------")
                f.printStackTrace()
              }
            }
          })
      }
      else{
        println("Invalid JobType "+ jobType)
        throw  new RuntimeException
      }
    }

  //  val sourceConnection = dbUtils.getConnection(sourceJdbcUrl)

//    val spark = SparkSession.builder().master("local[*]").appName("EmployeeApplication").getOrCreate()





  //  val empDF = spark.read.jdbc(sourceJdbcUrl,"employees.employees",properties)

   // empDF.show()
  }

}
