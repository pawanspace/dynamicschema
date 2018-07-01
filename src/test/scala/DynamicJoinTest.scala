import com.holdenkarau.spark.testing.{SharedSparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DynamicJoinTest extends FunSuite with SharedSparkContext {

  test("really simple transformation") {

    val catalog = Seq(CatalogRecord("1", "HP", "A-Asin"), CatalogRecord("2", "DELL", "B-Asin"))
    val compatibility = Seq(CompatibilityRecord("2", "HP", "A-Model"), CompatibilityRecord("2", "DELL", "B-Model"))


    val sqlContext = SparkSession.builder().config(sc.getConf).getOrCreate()

    val frame1 = sqlContext.createDataFrame(compatibility)

    val frame2 = sqlContext.createDataFrame(catalog)

    val result = DynamicJoin.join(DynamicJoin.getDynamicSchema, frame1, frame2)

    result.show(false)
  }


  case class CompatibilityRecord(replacementPartNumber: String,brand: String, wholegoodmodel: String)
  case class CatalogRecord(part_number: String,brand_name: String, asin: String)
}
