import AnalyzeMovieRating.{MeanRating, MeanRatingDF, StdDevRating, StdDevRatingDF}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, flatspec}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class AnalyzeMovieRatingSpec extends flatspec.AnyFlatSpec with Matchers with BeforeAndAfter {

  implicit var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .appName("AnalyzeMovieRating")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }


  behavior of "AnalyzeMovieRating"

  it should "check mean value from df" in {
    val movieDF: DataFrame = spark.read.option("header", true).option("inferSchema", true).csv("/Users/surajvisvesh/IdeaProjects/CSYE7200/assignment-movie-database/src/test/resources/movie_metadata.csv")
    val expectedMeanValue = 6.452300995024876
    val actualMeanValue = MeanRating(movieDF)
    actualMeanValue shouldBe expectedMeanValue
  }

  it should "check stddev value from df" in {
    val movieDF: DataFrame = spark.read.option("header", true).option("inferSchema", true).csv("/Users/surajvisvesh/IdeaProjects/CSYE7200/assignment-movie-database/src/test/resources/movie_metadata.csv")
    val expectedStdDevValue = 0.9984653707821606
    val actualStdDevValue = StdDevRating(movieDF)
    actualStdDevValue shouldBe expectedStdDevValue
  }

  it should "check mean df" in {
    val movieDF: DataFrame = spark.read.option("header", true).option("inferSchema", true).csv("/Users/surajvisvesh/IdeaProjects/CSYE7200/assignment-movie-database/src/test/resources/movie_metadata.csv")
    val schema = StructType(Seq(StructField("stddev_column", DoubleType, nullable = false)))
    val data = Seq(Row(6.452300995024876))
    val expectedMeanDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val actualMeanDF = MeanRatingDF(movieDF)
    assert(expectedMeanDF.collect().sameElements(actualMeanDF.collect()))
  }

  it should "check stddev df" in {
    val movieDF: DataFrame = spark.read.option("header", true).option("inferSchema", true).csv("/Users/surajvisvesh/IdeaProjects/CSYE7200/assignment-movie-database/src/test/resources/movie_metadata.csv")
    val schema = StructType(Seq(StructField("stddev_column", DoubleType, nullable = false)))
    val data = Seq(Row(0.9984653707821606))
    val expectedStdDevDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val actualStdDevDF = StdDevRatingDF(movieDF)
    assert(expectedStdDevDF.collect().sameElements(actualStdDevDF.collect()))
  }
}

