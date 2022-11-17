import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.StructType

object RawLayer {

  private val cityRawSchema: StructType = Encoders.product[CityRaw].schema
  private val countryRawSchema: StructType = Encoders.product[CountryRaw].schema
  private val covidTrackRawSchema: StructType = Encoders.product[CovidTrackRaw].schema
  private val objects: Seq[(StructType, String)] = Seq((cityRawSchema,"city"),(countryRawSchema,"country"),(covidTrackRawSchema,"covid_track"))

  def runRawLayer(spark: SparkSession): Unit = {

    objects.foreach{case (schema,name) => writeToRaw(readCSVFile(spark,schema,name),name)}

  }

  def readCSVFile( spark: SparkSession, schema: StructType, objectName: String): DataFrame = {

    import spark.implicits._
    // read csv file as RDD
    val rddFromFile = spark.sparkContext.textFile(s"src/main/resources/data/${objectName}/*.csv")

//    // delete header
//    val rddWithoutHeader = rddFromFile.mapPartitionsWithIndex { (id_x, iter) =>
//      if (id_x == 0) iter.drop(1)
//      else iter
//    }

    // rdd to DF with string schema
    val cleanDF = rddFromFile.map(f => {
      val elements = f.split(",")
      Row.fromSeq(elements)
    })
    spark.createDataFrame(cleanDF,schema)

  }

  def writeToRaw(dfToWrite: DataFrame, objectName: String): Unit = {
    dfToWrite.write.mode(saveMode = "overwrite").parquet(s"src/main/resources/raw/${objectName}")
  }

}
