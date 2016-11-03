package Utils

import org.apache.spark.sql.Row

import scala.util.parsing.json.JSON

object Utils extends Serializable {

  def parseJSONField(jsonValue: String): List[Any] = {
    if(jsonValue.nonEmpty){
      val json = JSON.parseFull(jsonValue).get.asInstanceOf[Map[String, Any]]
      val keys = for((key,value) <- json) yield json.get(key).get
      keys.toList
    }
    else List.empty
  }

  def toRow(list: List[Any]): Row = {
    Row.fromSeq(list)
  }
}
