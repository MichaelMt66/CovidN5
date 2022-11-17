import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.types.StructType

object CuratedLayer {

  private val citySchema: StructType = Encoders.product[City].schema
  private val countrySchema: StructType = Encoders.product[Country].schema
  private val covidTrackSchema: StructType = Encoders.product[CovidTrack].schema
  private val objects: Seq[(StructType, String)] = Seq((citySchema, "city"), (countrySchema, "country"), (covidTrackSchema, "covid_track"))

  def runCuratedLayer(spark: SparkSession):Unit = {

    objects.foreach{case (schema,name) => writeToCuratedLayer(readFromRawLayer(spark,name),name,schema)}

  }

  def readFromRawLayer(spark: SparkSession, objectName: String): DataFrame = {
      spark.read.parquet(s"src/main/resources/raw/${objectName}/*.snappy.parquet")
  }

  def writeToCuratedLayer(dfToWrite: DataFrame, objectName: String, schema: StructType): Unit = {

    // cast raw data using caseclass schema
    val resultDF = schema.fields.foldLeft(dfToWrite)((df, c) => df.withColumn(c.name, col(c.name).cast(c.dataType)))
    resultDF.printSchema()
    resultDF.write.mode(saveMode = "overwrite")parquet(s"src/main/resources/curated/${objectName}")

  }

}
