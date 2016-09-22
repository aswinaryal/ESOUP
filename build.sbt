name := "StackoverflowProject"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
 "org.apache.spark" %% "spark-sql" % "1.6.1",
 "com.databricks" %% "spark-csv" % "1.5.0",
 "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.1",
"org.apache.spark" %% "spark-streaming" % "1.6.1",
"org.apache.spark" %% "spark-streaming-kafka" % "1.6.1"
)
assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
