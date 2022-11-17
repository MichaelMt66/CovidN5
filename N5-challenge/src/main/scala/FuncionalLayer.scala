import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.sum

object FuncionalLayer {

  def RunFuncionalLayer(spark: SparkSession) = {
    val countryDF = readFromCuratedLayer(spark, "country").as("country")
    val cityDF = readFromCuratedLayer(spark, "city").as("city")
    val covidTrackDF = readFromCuratedLayer(spark, "covid_track").as("covidtrack")
    dayWise(countryDF, cityDF, covidTrackDF)
    fullGrouped(countryDF, cityDF, covidTrackDF)
    covid19CleanComplete(countryDF, cityDF, covidTrackDF)
  }

  private def dayWise(countryDF: DataFrame, cityDF: DataFrame, covidTrackDF: DataFrame) = {

    val joinedDF = cityDF.join(covidTrackDF, col("city.UID") === col("covidtrack.UID")).as("joined")
      .join(countryDF, col("joined.countryRegion") === col("country.countryRegion"))

    val writeDF = joinedDF.groupBy("dataDate").agg(sum("deaths").alias("death"),sum("confirmed").alias("confirmed"))

    writeToFunctional(writeDF,"day_wise")

  }

  private def fullGrouped(countryDF: DataFrame, cityDF: DataFrame, covidTrackDF: DataFrame) = {

    val joinedDF = cityDF.join(covidTrackDF, col("city.UID") === col("covidtrack.UID"))

    val writeDF = joinedDF.groupBy("dataDate", "countryRegion").agg(sum("deaths").alias("death"),sum("confirmed").alias("confirmed")).as("joined")
      .join(countryDF.select("countryRegion","region"), col("joined.countryRegion") === col("country.countryRegion"))

    writeToFunctional(writeDF,"full_grouped")

  }

  private def covid19CleanComplete(countryDF: DataFrame, cityDF: DataFrame, covidTrackDF: DataFrame) = {

    val joinedDF = cityDF.join(covidTrackDF, col("city.UID") === col("covidtrack.UID"))

    val writeDF = joinedDF.groupBy("dataDate", "countryRegion").agg(sum("deaths").alias("death"),sum("confirmed").alias("confirmed")).as("joined")
      .join(countryDF.select("lat","lon","countryRegion","region"), col("joined.countryRegion") === col("country.countryRegion"))
      .drop("country.countryRegion")

    writeToFunctional(writeDF,"covid_19_clean")

  }

  def readFromCuratedLayer(spark: SparkSession, objectName: String): DataFrame = {
    spark.read.parquet(s"src/main/resources/curated/${objectName}/*.snappy.parquet")
  }

  def writeToFunctional(dfToWrite: DataFrame, objectName: String): Unit = {
    dfToWrite.write.mode(saveMode = "overwrite").partitionBy("dataDate").parquet(s"src/main/resources/functional/${objectName}")
  }
}
