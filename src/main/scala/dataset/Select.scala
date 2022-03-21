package dataset

import util.WithSpark.withSpark

object Select extends App {

  val data = Seq(
    Tuple1((1, 10, "a")),
    Tuple1((2, 20, "b")),
    Tuple1((3, 30, "c")))

  withSpark { spark =>
    import spark.implicits._

    val df = data.toDF("value")

    df.printSchema()
    df.show()

    val resultDf = df.select("value.*")

    resultDf.printSchema()
    resultDf.show()
  }

}
