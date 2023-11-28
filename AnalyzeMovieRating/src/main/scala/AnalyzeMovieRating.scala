import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{avg, _}


object AnalyzeMovieRating extends App {

  def MeanRating(movieDF: DataFrame): Double = {
    movieDF.select("imdb_score").agg(avg("imdb_score")).collect() match {
      case Array(Row(mean: Double)) => mean
      case _ => 0.0
    }
  }

  def StdDevRating(movieDF: DataFrame): Double = {
    movieDF.select(stddev("imdb_score")).collect() match {
      case Array(Row(stddev: Double)) => stddev
      case _ => 0.0
    }
  }

  def MeanRatingDF(movieDF: DataFrame): DataFrame = {
    movieDF.select(mean("imdb_score")).alias("Mean of Ratings")

  }

  def StdDevRatingDF(movieDF: DataFrame): DataFrame = {
    movieDF.select(stddev("imdb_score")).alias("Standard Deviation of Ratings")
  }

}
