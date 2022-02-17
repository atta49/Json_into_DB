

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ExplodeArrayAndMap{

  def main(args:Array[String]) : Unit = {

    val spark = SparkSession.builder().appName("SparkByExamples.com")
      .master("local[1]")
      .getOrCreate()

    // create DataFrame

    val arrayData = Seq(
      Row("James",List("Java","Scala","C++"),Map("hair"->"black","eye"->"brown")),
      Row("Michael",List("Spark","Java","C++",null),Map("hair"->"brown","eye"->null)),
      Row("Robert",List("CSharp","Python",""),Map("hair"->"red","eye"->"")),
      Row("Washington",null,null),
      Row("Jeferson",List(),Map())
    )

    val arraySchema = new StructType()
      .add("name",StringType)
      .add("knownLanguages", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
    df.printSchema()
    df.show()

    import spark.implicits._
    // Below are Array examples
    //explode
    df.select($"name",explode($"knownLanguages"))
      .show()

    //explode_outer
    df.select($"name",explode_outer($"knownLanguages"))
      .show()

    //posexplode
    df.select($"name",posexplode($"knownLanguages"))
      .show()

    //posexplode_outer
    df.select($"name",posexplode_outer($"knownLanguages"))
      .show()

    // Below are Map examples

    //explode
    df.select($"name",explode($"properties"))
      .show()
    //explode_outer
    df.select($"name",explode_outer($"properties"))
      .show()
    //posexplode
    df.select($"name",posexplode($"properties"))
      .show()

    //posexplode_outer
    df.select($"name",posexplode_outer($"properties"))
      .show()
  }
}
