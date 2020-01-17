package stats

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

class DailyStatisticsCalculator(spark: SparkSession, inputData: DataFrame, outputPath: String, date: String) {

  private[stats] val customersData: Dataset[Row] = {
    SparkCalculator.customersData(inputData, dateIndex = 2, to_date(lit(date)), to_date(lit(date)))
      .persist()
  }

  private[stats] val activities = SparkCalculator.activities(customersData, count(Columns.Activity) as Columns.ActivityCount)
  private[stats] val modules = SparkCalculator.modules(customersData, count(Columns.Module) as Columns.ModuleCount)
  private[stats] val users = SparkCalculator.uniqueUsers(customersData, count(Columns.Module) as Columns.UniqueUsersCount)

  private def storeResults(dataset: DataFrame, query: String): Unit =
    dataset.write.mode(SaveMode.Overwrite).parquet(s"$outputPath/$query/$date")

  // Activity
  storeResults(activities, query = "activity")

  // Modules
  storeResults(modules, query = "module")

  // Unique Users
  storeResults(users, query = "users")

}
