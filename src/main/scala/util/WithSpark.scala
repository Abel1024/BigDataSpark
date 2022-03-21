package util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WithSpark {

  def withSpark(block: SparkSession => Unit): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark: SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", 64)
        .getOrCreate()

    try {

      block(spark)

    } finally {
      spark.close()
    }
  }

}
