package functions

import org.apache.spark.sql.types.{DataType, DataTypes}
import spark.WithSpark.withSpark

object CastF extends App {

  withSpark { spark =>

    import spark.implicits._

    val df = (1 to 10).toDF
    df.show()

    df.withColumn("double", $"value".cast(DataTypes.DoubleType)).show()
    df.withColumn("double", $"value".cast("double")).show()

  }

}
