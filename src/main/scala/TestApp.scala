
import WithSpark.withSpark
import org.apache.spark.sql.{DataFrame, Dataset}

case class User(userId: String)
case class UserData(userId: String, userData: String)

object TestApp extends App {

  withSpark { spark =>

    import spark.implicits._

    def excludeUsers(userData: Dataset[UserData], toExclude: Dataset[User]): Dataset[UserData] = {
      val excludedIds: DataFrame = toExclude
        .map(_.userId)
        .withColumnRenamed("value", "excludedId")
      userData
        .join(
          right = excludedIds,
          joinExprs = userData("userId") === excludedIds("excludedId"),
          joinType = "left_outer"
        )
        .filter($"excludedId".isNull)
        .map(r => UserData(r.getString(0), r.getString(1)))
    }

    val userData = List(
      UserData("1", "1"),
      UserData("1", "33"),
      UserData("1", "34"),
      UserData("1", "36"),
      UserData("1", "37"),
      UserData("2", "1"),
      UserData("2", "2"),
      UserData("3", "3"),
      UserData("3", "4"),
      UserData("3", "5")
    )

    val toExclude = List(
      User("1"),
      User("3")
    )

    val expected = Set(
      UserData("2", "1"),
      UserData("2", "2")
    )

    val userDataDS = userData.toDS
    val toExcludeDS = toExclude.toDS
    val resultDS: Dataset[UserData] = excludeUsers(userDataDS, toExcludeDS)

    userDataDS.show()
    toExcludeDS.show()
    resultDS.show()

    assert(resultDS.collect.toSet == expected)

  }
}

