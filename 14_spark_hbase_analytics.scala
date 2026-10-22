// EXPERIMENT 14 — SPARK ANALYTICS ON HBASE
// Aim: Read employee records from HBase and calculate record count and average age.
// Requires compatible Spark, Hadoop, HBase libraries and HBase configuration.

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

// HBASE SHELL SETUP (run in HBase shell, not Scala):
// create 'employees', 'info'
// put 'employees','1','info:name','John'
// put 'employees','1','info:age','30'
// put 'employees','2','info:name','Alice'
// put 'employees','2','info:age','28'
// scan 'employees'

// Scala application: create HBase configuration and name the source table.
val conf = HBaseConfiguration.create()
conf.set("hbase.zookeeper.quorum", "localhost")
conf.set(TableInputFormat.INPUT_TABLE, "employees")

// 'sc' must be an existing SparkContext configured with HBase dependencies.
val rdd = sc.newAPIHadoopRDD(
  conf,
  classOf[TableInputFormat],
  classOf[ImmutableBytesWritable],
  classOf[Result]
)

// Extract name and age from the 'info' column family.
val people = rdd.map { case (_, result) =>
  val name = Bytes.toString(result.getValue(
    Bytes.toBytes("info"), Bytes.toBytes("name")))
  val ageText = Bytes.toString(result.getValue(
    Bytes.toBytes("info"), Bytes.toBytes("age")))
  (name, ageText.toInt)
}

people.collect().foreach(println)
val count = people.count()
println("Total records: " + count)
if (count > 0) {
  println("Average age: " + people.map(_._2).sum / count.toDouble)
}

// Expected for the sample rows: Total records: 2; Average age: 29.0
// Execute Scala code in the lab's configured Spark/HBase application environment.
