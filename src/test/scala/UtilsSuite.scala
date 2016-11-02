import org.apache.spark.sql.Row
import org.scalatest.FunSuite
import Utils.Utils
class UtilsSuite extends FunSuite {

  test(s"parseJson -> with 2 fields ") {
    val json = """{"firstName": "144444", "lastName": "2",  "surName": "5"}"""
    assert(Utils.parseJSONField(json) == List("144444", "2", "5"))
  }

  test("toRow -> List(14,23,56)"){
    assert(Utils.toRow(List("14", "23", "56")) == Row("14", "23", "56"))
  }


}
