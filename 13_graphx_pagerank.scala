// EXPERIMENT 13 — GRAPHX PAGERANK
// Aim: Construct a small directed graph and calculate PageRank.
// GraphX is Scala/Java API (not native PySpark); use a compatible spark-shell/project.

import org.apache.spark._
import org.apache.spark.graphx._

// In spark-shell, an existing SparkContext may already be available as sc.
// If running as a standalone Scala application, create the SparkContext as shown here.
val conf = new SparkConf().setMaster("local[*]").setAppName("PageRank")
val graphSc = new SparkContext(conf)

// Define vertices with unique Long IDs and directed edges between them.
val vertices = graphSc.parallelize(Seq(
  (1L, "A"), (2L, "B"), (3L, "C")
))
val edges = graphSc.parallelize(Seq(
  Edge(1L, 2L, 1), Edge(2L, 3L, 1),
  Edge(3L, 1L, 1), Edge(1L, 3L, 1)
))

// Build graph and compute PageRank to the specified tolerance.
val graph = Graph(vertices, edges)
graph.pageRank(0.0001).vertices.collect().foreach(println)
graphSc.stop()

// EXECUTION: Run in a Scala application or adapt to the configured spark-shell.
// Do not create a second SparkContext if the shell already manages one.
