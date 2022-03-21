package functions

import functions.Random.rand
import util.WithSpark.withSpark

object ExplodeF extends App {

  case class Value(a: Int = rand(), b: Int = rand(), c: Int = rand())

  val data: Seq[Seq[Value]] = Seq(
    Seq(Value()),
    Seq(Value(), Value()),
    Seq(Value(), Value(), Value()),
    Seq(Value(), Value(), Value(), Value()),
    Seq(Value(), Value(), Value(), Value(), Value())
  )

  withSpark { spark =>
    import spark.implicits._
    val df = data.toDF
    df.printSchema()
    df.show(false)

    import org.apache.spark.sql.functions._
    val explodedDf = df.select(explode($"value"))
    explodedDf.printSchema()
    explodedDf.show()
  }

}

object Random {
  def rand(): Int = scala.util.Random.nextInt(10)
}