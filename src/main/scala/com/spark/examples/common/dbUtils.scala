package com.spark.examples.common

import java.sql.{Connection,DriverManager,Statement}
import java.util.Properties

object dbUtils {
def getConnection(jdbcUrl:String):Connection ={
  val properties = new Properties()
  properties.setProperty("user","root")
  properties.setProperty("password","admin")
  properties.setProperty("driver","com.mysql.jdbc.Driver")

  var connection: Connection = null
    try{
      Class.forName("com.mysql.jdbc.Driver")
      connection=DriverManager.getConnection(jdbcUrl,properties)
      connection
      }
      catch {
      case e: Exception => {
     println("Error establising connection to database")
      throw new RuntimeException
      }
    }
  }
  def executeDDL(connection: Connection,ddlQuery:String): Unit = {
    try{
      val stmt = connection.createStatement()
      val status = stmt.execute(ddlQuery)
    }
    catch {
      case e:Exception => {
        println("Error Truncating Table")
        e.printStackTrace()
        throw new RuntimeException(e)
      }
    }
  }

  def executeUpdate(connection:Connection,query:String): Int = {
    try{
      val pstmt = connection.prepareStatement(query)
      pstmt.executeUpdate(query)
    }
    catch {
      case e:Exception => {
        println("Error executing Update query")
        e.printStackTrace()
        throw new RuntimeException(e)
      }
    }
  }
}
