package types

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import util.WithSpark.withSpark
import scala.jdk.CollectionConverters._

object CreateDataFrameWithSchema extends App {

  val data: java.util.List[Row] =
    Seq(
      Row((1, 10, "a")),
      Row((2, 20, "b")),
      Row((3, 30, "c"))
    ).asJava

  withSpark { spark =>

    val schema = StructType(
      Array(
        StructField("values",
          StructType(
            Array(
              StructField("a", IntegerType),
              StructField("a", IntegerType),
              StructField("a", StringType)
            )))))

    schema.printTreeString()
    val df = spark.createDataFrame(data, schema)
    df.printSchema()
    df.show()

  }
}
