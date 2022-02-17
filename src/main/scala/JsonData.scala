import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
object JsonData {

    def main(args:Array[String]): Unit = {

      Logger.getLogger("org").setLevel((Level.ERROR))

      val spark:SparkSession= SparkSession.builder()
        .master("local[3]")
        .appName("SparkForJson")
        .getOrCreate()

      //val df1=spark.read.json"E:\\sample_json.json"()
      //read json file into dataframe
      val df = spark.read.option("multiline", "true").json("E:\\sample_json.json")
      df.printSchema()
      df.show()
      //Rename
      val df1 = df.withColumnRenamed("id", "key")
      //menu DataFram
      val menu=df1.select("key","type","name","ppu").toDF()
      menu.show()
      val battersdf= df1.select("key","batters.batter")
//      battersdf.printSchema()
//      battersdf.show(1,false)
      //   val newbatters=battersdf.select("id",explode($"battersdf.batter").alias("newbatter")
      //      val newbatter=battersdf.select("key","batter.id","batter.type")
      //      newbatter.show()
      import spark.implicits._

      //batters DataFrame
      val FinalBattersDF=battersdf.select($"key",explode($"batter").alias("newbatter"))
      .select("key", "newbatter.*").withColumnRenamed("id","bat_id")
        .withColumnRenamed("type","bat_type")
      FinalBattersDF.show()

      //topping DataFrame

      val toppingsdf= df1.select("key","topping")

      val FinalToppingDF=toppingsdf.select($"key",explode($"topping").alias("newtopping"))
        .select("key", "newtopping.*").withColumnRenamed("id","top_id")
        .withColumnRenamed("type","top_type")
      FinalToppingDF.show()

      //insert into DataBase
      menu.write.format("jdbc")
        .option("url","jdbc:mysql://localhost:3306/" + "BatachTarget")
        .option("dbtable", "menu_Table")
        .option("user", "root")
        .option("password", "mysql1122")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode(SaveMode.Overwrite)
        .save()

      FinalBattersDF.write.format("jdbc")
        .option("url","jdbc:mysql://localhost:3306/" + "BatachTarget")
        .option("dbtable", "batters_Table")
        .option("user", "root")
        .option("password", "mysql1122")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode(SaveMode.Overwrite)
        .save()


      FinalToppingDF.write.format("jdbc")
        .option("url","jdbc:mysql://localhost:3306/" + "BatachTarget")
        .option("dbtable", "toppings_Table")
        .option("user", "root")
        .option("password", "mysql1122")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode(SaveMode.Overwrite)
        .save()
    }

}
