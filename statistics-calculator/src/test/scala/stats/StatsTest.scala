package stats

import java.io.File

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

class StatsTest extends AnyFunSpec
  with SparkSessionWrapper
  with DataFrameComparer
  with BeforeAndAfterAll {

  import spark.implicits._

  val tempFolder: String = System.getProperty("java.io.tmpdir") + "/user"
  val tempFile: File = new File(tempFolder)

  val inputData_2020_01_01: DataFrame = Seq(
    (1234, "john@foo.com", 4567, "activity1", "module_1", "1/2/2020-01-01", "2020-01-01"),
    (1234, "john@foo.com", 4567, "activity2", "module_1", "1/2/2020-01-01", "2020-01-01"),
    (1234, "john@foo.com", 9876, "activity1", "module_1", "1/2/2020-01-01", "2020-01-01"),
    (1234, "doe@bar.com", 4567, "activity2", "module_1", "1/2/2020-01-01", "2020-01-01"),
    (1234, "doe@bar.com", 4567, "activity2", "module_1", "1/2/2020-01-01", "2020-01-01"),
    (1234, "doe@bar.com", 4567, "activity2", "module_3", "1/2/2020-01-01", "2020-01-01")
  ).toDF(Columns.ClientId, Columns.UserId, Columns.AccountId, Columns.Activity, Columns.Module, Columns.FileName, Columns.Date)

  val expectedActivities_2020_01_01: DataFrame = Seq(
    ("john@foo.com", 4567, "module_1", 2L),
    ("john@foo.com", 9876, "module_1", 1L),
    ("doe@bar.com", 4567, "module_3", 1L),
    ("doe@bar.com", 4567, "module_1", 2L)
  ).toDF(Columns.UserId, Columns.AccountId, Columns.Module, Columns.ActivityCount)

  val expectedModules_2020_01_01: DataFrame = Seq(
    ("john@foo.com", 9876, 1L),
    ("john@foo.com", 4567, 2L),
    ("doe@bar.com",  4567, 3L)
  ).toDF(Columns.UserId, Columns.AccountId, Columns.ModuleCount)

  val inputData_2020_01_02: DataFrame = Seq(
    (1234, "john@foo.com", 4567, "activity1", "module_1", "1/2/2020-01-02", "2020-01-02"),
    (1234, "john@foo.com", 4567, "activity2", "module_1", "1/2/2020-01-02", "2020-01-02"),
    (1234, "john@foo.com", 9876, "activity1", "module_2", "1/2/2020-01-02", "2020-01-02"),
    (1234, "john@foo.com", 9876, "activity1", "module_2", "1/2/2020-01-02", "2020-01-02"),
    (1234, "john@foo.com", 4567, "activity1", "module_2", "1/2/2020-01-02", "2020-01-02"),
    (1234, "john@foo.com", 4567, "activity1", "module_2", "1/2/2020-01-02", "2020-01-02")
  ).toDF(Columns.ClientId, Columns.UserId, Columns.AccountId, Columns.Activity, Columns.Module, Columns.FileName, Columns.Date)

  val expectedActivities_2020_01_02: DataFrame = Seq(
    ("john@foo.com", 4567, "module_1", 2L),
    ("john@foo.com", 4567, "module_2", 2L),
    ("john@foo.com", 9876, "module_2", 2L)
  ).toDF(Columns.UserId, Columns.AccountId, Columns.Module, Columns.ActivityCount)

  val expectedModules_2020_01_02: DataFrame = Seq(
    ("john@foo.com", 9876, 2L),
    ("john@foo.com", 4567, 4L)
  ).toDF(Columns.UserId, Columns.AccountId, Columns.ModuleCount)

  val dailyStats_2020_01_01 =
    new DailyStatisticsCalculator(spark, inputData_2020_01_01, tempFolder, "2020-01-01")

  it("daily activities 2020-01-01") {
    assertSmallDataFrameEquality(dailyStats_2020_01_01.activities, expectedDF = expectedActivities_2020_01_01)
  }

  it("daily modules 2020-01-01") {
    assertSmallDataFrameEquality(dailyStats_2020_01_01.modules, expectedDF = expectedModules_2020_01_01)
  }

  val dailyStats_2020_01_02 =
    new DailyStatisticsCalculator(spark, inputData_2020_01_02, tempFolder, "2020-01-02")

  it("daily activities 2020-01-02") {
    assertSmallDataFrameEquality(dailyStats_2020_01_02.activities, expectedDF = expectedActivities_2020_01_02)
  }

  it("daily modules 2020-01-02") {
    assertSmallDataFrameEquality(dailyStats_2020_01_02.modules, expectedDF = expectedModules_2020_01_02)
  }

  it("activities when querying in 3 days range" ) {
    val queryCalculator_3d: QueryCalculator =
      new QueryCalculator(spark, SparkCalculator.inputDateByFile(spark.read.parquet(s"$tempFolder/activity/*"), 2),
        s"$tempFolder/query", numOfDays = 3, to_date(lit("2020-01-03")))
    val expectedActivities: DataFrame = Seq(
      ("john@foo.com", 4567, "module_1", 4L),
      ("john@foo.com", 9876, "module_1", 1L),
      ("doe@bar.com", 4567, "module_3", 1L),
      ("doe@bar.com", 4567, "module_1", 2L),
      ("john@foo.com", 4567, "module_2", 2L),
      ("john@foo.com", 9876, "module_2", 2L)
    ).toDF(Columns.UserId, Columns.AccountId, Columns.Module, s"sum(${Columns.ActivityCount})")
    assertSmallDatasetEquality(queryCalculator_3d.activities, changeSchemaToNullable(expectedActivities))
  }

  it("modules when querying in 3 days range" ) {
    val queryCalculator_3d: QueryCalculator =
      new QueryCalculator(spark, SparkCalculator.inputDateByFile(spark.read.parquet(s"$tempFolder/module/*"), 2),
        s"$tempFolder/query", numOfDays = 3, to_date(lit("2020-01-03")))
    val expectedModules: DataFrame = Seq(
      ("john@foo.com", 9876, 3L),
      ("john@foo.com", 4567, 6L),
      ("doe@bar.com",  4567, 3L)
    ).toDF(Columns.UserId, Columns.AccountId, s"sum(${Columns.ModuleCount})")
    assertSmallDatasetEquality(queryCalculator_3d.modules, changeSchemaToNullable(expectedModules))
  }

  it("activities when querying in 1 days range" ) {
    val queryCalculator =
      new QueryCalculator(spark, SparkCalculator.inputDateByFile(spark.read.parquet(s"$tempFolder/activity/*"), 2),
      s"$tempFolder/query", numOfDays = 1, to_date(lit("2020-01-03")))
    val expectedActivities: DataFrame = Seq(
      ("john@foo.com", 4567, "module_1", 2L),
      ("john@foo.com", 4567, "module_2", 2L),
      ("john@foo.com", 9876, "module_2", 2L)
    ).toDF(Columns.UserId, Columns.AccountId, Columns.Module, s"sum(${Columns.ActivityCount})")
    assertSmallDatasetEquality(queryCalculator.activities, changeSchemaToNullable(expectedActivities))
  }

  it("modules when querying in 1 day range" ) {
    val queryCalculator_3d: QueryCalculator =
      new QueryCalculator(spark, SparkCalculator.inputDateByFile(spark.read.parquet(s"$tempFolder/module/*"), 2),
        s"$tempFolder/query", numOfDays = 1, to_date(lit("2020-01-03")))
    val expectedModules: DataFrame = Seq(
      ("john@foo.com", 9876, 2L),
      ("john@foo.com", 4567, 4L)
    ).toDF(Columns.UserId, Columns.AccountId, s"sum(${Columns.ModuleCount})")
    assertSmallDatasetEquality(queryCalculator_3d.modules, changeSchemaToNullable(expectedModules))
  }

  def delete(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(delete)
      file.delete
    }
    else file.delete
  }

  override protected def afterAll(): Unit = {
    delete(tempFile)
    super.afterAll()
  }

  private def changeSchemaToNullable(df: DataFrame): DataFrame = {
    val schema = df.schema
    val newSchema = StructType(schema.map {
      case StructField( c, t, n, m) if !n => StructField( c, t, !n, m)
      case y: StructField => y
    })
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

}
