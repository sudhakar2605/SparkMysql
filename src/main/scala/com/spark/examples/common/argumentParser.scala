package com.spark.examples.common

object argumentParser {
  def getArgMap(args: Array[String]):Map[String,String] ={
    val argMap = scala.collection.mutable.Map[String,String]()

    args.foreach(arg => {
      val keyValue = arg.split("=").map(x=>x.trim)
      argMap += keyValue(0) -> keyValue(1)
    })

    argMap.toMap
  }

  def isArgNull(values: Any*):Boolean={
    values.filter(_==null).size>0
  }

}
