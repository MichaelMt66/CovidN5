import org.apache.spark.sql.SparkSession
import RawLayer.runRawLayer
import CuratedLayer.runCuratedLayer
import FuncionalLayer.RunFuncionalLayer

object RunIt {

    def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Test")
      .master("local")
      .config("spark.debug.maxToStringFields", 2000)
      .getOrCreate()

      runRawLayer(spark)
      runCuratedLayer(spark)
      RunFuncionalLayer(spark)

    }
  }

