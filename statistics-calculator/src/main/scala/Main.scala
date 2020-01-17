import org.apache.spark.sql.SparkSession
import stats.DailyStatisticsCalculator

object Main extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .config("fs.s3a.endpoint", "http://127.0.0.1:9009")
    .config("fs.s3a.access.key", "access")
    .config("fs.s3a.secret.key", "topsecret")
    .config("fs.s3a.path.style.access", "true")
    .getOrCreate()


    val input = "s3a://Clickstream/*/*"
    val output = "s3a://Clickstream/Results"
    val daily = new DailyStatisticsCalculator(spark, spark.read.json(input), output, "2020-01-02")


}
