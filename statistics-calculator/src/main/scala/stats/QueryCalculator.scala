package stats

import java.util.UUID

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

class QueryCalculator(spark: SparkSession,
                      inputData: DataFrame,
                      outputPath: String,
                      numOfDays: Int,
                      toDate: Column = current_timestamp()) {

  private[stats] val customersData: Dataset[Row] =
    SparkCalculator.customersData(inputData, dateIndex = 2, date_sub(toDate, numOfDays), toDate)
    .persist()

  private val uuid: String = UUID.randomUUID().toString

  private[stats] lazy val activities = SparkCalculator.activities(customersData, sum(Columns.ActivityCount))
  private[stats] lazy val modules = SparkCalculator.modules(customersData, sum(Columns.ModuleCount))
  private[stats] lazy val users = SparkCalculator.activities(customersData, sum(Columns.ActivityCount))


  private def storeResults(dataset: DataFrame, query: String): Unit =
    dataset.write.parquet(s"$outputPath/query/$query/$uuid")

  // Activity
  def calcStoreActivities(): Unit = storeResults(activities, query = "activity")

  // Modules
  def calcStoreModules(): Unit = storeResults(modules, query = "modules")

  // Unique Users
  def calcStoreUsers(): Unit =  storeResults(users, query = "users")

}
