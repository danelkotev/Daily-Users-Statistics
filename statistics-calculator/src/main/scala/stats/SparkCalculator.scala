package stats

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

object SparkCalculator {

  def inputDateByFile(initDate: DataFrame, dateIndex: Int = 1): DataFrame =
    initDate
      .withColumn(Columns.FileName, input_file_name())
      .withColumn(Columns.PathParts,  split(col(Columns.FileName), "/"))
      .withColumn(Columns.Date,  to_date(col(Columns.PathParts).apply(size(col(Columns.PathParts)) - dateIndex)))
      .drop(col(Columns.PathParts))


  def customersData(initDate: DataFrame, dateIndex: Int, fromDate: Column, toDate: Column): Dataset[Row] =
    initDate
      .filter(col(Columns.Date).geq(fromDate) && col(Columns.Date).leq(toDate))

  def activities(data: DataFrame, agg: Column): DataFrame =
    data.groupBy(col(Columns.UserId), col(Columns.AccountId), col(Columns.Module)).agg(agg)

  def modules(data: DataFrame, agg: Column): DataFrame =
    data.groupBy(col(Columns.UserId), col(Columns.AccountId)).agg(agg)

  def uniqueUsers(data: DataFrame, agg: Column): DataFrame =
    data.groupBy(col(Columns.UserId), col(Columns.AccountId)).agg(agg)

}
