name := "flink-nyc"

version := "0.1"

scalaVersion := "2.12.9"

val flinkVersion = "1.9.0"
val jodaVersion = "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-cep" % flinkVersion,
  "org.apache.flink" %% "flink-cep-scala" % flinkVersion,
  "joda-time" % "joda-time" % jodaVersion
)
