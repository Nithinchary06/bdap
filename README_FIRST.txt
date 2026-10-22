BIG DATA PLATFORMS LAB — EXPERIMENT FILES
========================================

This folder contains 15 experiment files based on the uploaded
"Big Data Lab Full Solutions Cloudera VM" PDF.

Each experiment file includes:
- Experiment title and aim
- Program/commands with comments explaining execution
- Execution steps and verification notes

IMPORTANT:
- These are prepared from the supplied PDF. Match your college's Cloudera VM,
  installed versions, dataset columns, and instructor's record where they differ.
- Commands for Hive belong in Hive CLI/Beeline; HBase commands belong in HBase shell.
- PySpark programs use CSV files in the same working directory unless noted.
- Java MapReduce, Hive UDF, GraphX, and Spark-HBase tasks require compatible
  libraries/build configuration and are not standalone scripts.
- Do not format an existing NameNode or Kafka data directory.
- For MapReduce reruns, remove the old output directory first.

Experiments:
01 Hadoop pseudo-distributed mode and HDFS
02 Sqoop import/export and Flume log collection
03 MapReduce word frequency
04 Uber trips: busiest date by dispatching base
05 Hive database, tables and sales queries
06 Hive UDF (uppercase)
07 HBase employee_details CRUD
08 Hive and HBase data transfer
09 PySpark RDD operations
10 PySpark DataFrame analysis and missing-value cleaning
11 Spark Streaming word count
12 Spark ML linear regression and ID3
13 GraphX PageRank
14 Spark analytics on HBase employee_details
15 Kafka topic, producer and consumer

See the comments inside each file for where commands should be run.
