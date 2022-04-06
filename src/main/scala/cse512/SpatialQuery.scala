package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(pointString,queryRectangle))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(pointString,queryRectangle))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((stWithin(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((stWithin(pointString1, pointString2, distance)))) 
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def ST_Contains(pointString:String, queryRectangle:String) : Boolean = {
    val point = pointString.split(",")
    val pointX = point(0).toDouble
    val pointY = point(1).toDouble

    val area = queryRectangle.split(",")
    val areaX1 = area(0).toDouble
    val areaY1 = area(1).toDouble
    val areaX2 = area(2).toDouble
    val areaY2 = area(3).toDouble

    var x1: Double = 0
    var x2: Double = 0
    var y1: Double = 0
    var y2: Double = 0

    if (areaX1 < areaX2) {
      x1 = areaX1
      x2 = areaX2
    }
    else{
      x1 = areaX2
      x2 = areaX1
    }

    if (areaY1 < areaY2) {
      y1 = areaY1
      y2 = areaY2
    }
    else{
      y1 = areaY2
      y2 = areaY1
    }

    if(pointX >= x1 && pointX <= x2 && pointY >= y1 && pointY <= y2) {
      return true
    }
    else{
      return false
    }
  }

  def stWithin(pointString1:String, pointString2:String, distance:Double) : Boolean = {
    val p1 = pointString1.split(",")
    val p2 = pointString2.split(",")
    val x1 = p1(0).toDouble
    val y1 = p1(1).toDouble
    val x2 = p2(0).toDouble
    val y2 = p2(1).toDouble

    val l2_distance = scala.math.pow((scala.math.pow((x2 - x1), 2) + scala.math.pow((y2-y1), 2)),0.5)
    if (l2_distance < distance) {
      return true
    }
    else{
      return false
    }
  }

}
