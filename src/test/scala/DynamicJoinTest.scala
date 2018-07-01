import com.holdenkarau.spark.testing.{SharedSparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DynamicJoinTest extends FunSuite with SharedSparkContext {

  test("really simple transformation") {

    val catalog = Seq(CatalogRecord("1", "HP", "A-Asin", "C"), CatalogRecord("2", "DELL", "B-Asin", null))
    val compatibility = Seq(CompatibilityRecord("2", "HP", "A-Model"), CompatibilityRecord("2", "DELL", "B-Model"))


    val sqlContext = SparkSession.builder().config(sc.getConf).getOrCreate()

    val frame1 = sqlContext.createDataFrame(compatibility)
    frame1.show(false)
    val frame2 = sqlContext.createDataFrame(catalog)
    frame2.show(false)

    val dynamicSchema = DynamicJoin.getDynamicSchema

    val result = DynamicJoin.join(dynamicSchema, frame1, frame2)

    result.show(false)
  }


  case class CompatibilityRecord(replacementPartNumber: String,brand: String, wholegoodmodel: String)
  case class CatalogRecord(part_number: String,brand_name: String, asin: String, series: String)
}
