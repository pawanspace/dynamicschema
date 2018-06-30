import scala.util.parsing.json.JSON

/**
  * Created by pawanc on 6/29/18.
  */
object DynamicJoin {


  val person_json = """{
                      "name": "Joe Doe",
                      "age": 45,
                      "kids": ["Frank", "Marta", "Joan"]
                    }"""


  def main(args: Array[String]): Unit = {
    val option = JSON.parseRaw(person_json)
    print(option)
  }

}
