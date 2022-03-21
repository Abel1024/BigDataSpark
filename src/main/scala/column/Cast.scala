package column

import org.apache.spark.sql.types.DataTypes
import util.WithSpark.withSpark

object Cast extends App {

  withSpark { spark =>

    import spark.implicits._

    val df = (1 to 10).toDF
    df.show()

    df.withColumn("double", $"value".cast(DataTypes.DoubleType)).show()
    df.withColumn("double", $"value".cast("double")).show()

  }

}
