import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object dataconversion
{
 def main(args: Array[String]) 
 {
  val conf = new SparkConf()
             .setMaster("local[*]")
             .setAppName("vicroads")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .option("mode", "DROPMALFORMED")
    .load("file:/home/cloudera/odev/vicr/data/vicroads_crash_data.csv") // read from local file
  df.saveAsParquetFile("dwh/vcd.parquet") // Write to hdfs

  sc.stop
 }
}
