package Output

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

case class ParquetConnSettings(SQLContext: SQLContext) {

  val parquetSchema = new StructType(
  Array(
    StructField("firstName", StringType, false),
    StructField("lastName", StringType, false)))

  def save(rdd: RDD[Row]): Unit = {
    val df = SQLContext.createDataFrame(rdd, parquetSchema)
    df.show
    df.printSchema()
    df.write.mode(Append).parquet("hdfs://localhost:9000/user/hadoop/data")
  }
}
