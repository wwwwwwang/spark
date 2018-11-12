name := "etl_w"

version := "1.0"

scalaVersion := "2.11.8"

val helloTask = taskKey[Unit]("say hello")

helloTask := {
  println("Hello")
}

val gitHeadCommitSha = taskKey[String]("determine the current git commit SHA")
gitHeadCommitSha := Process("git rev-parse HEAD").lines.head


libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.4.17"
libraryDependencies += "commons-cli" % "commons-cli" % "1.2"
libraryDependencies += "commons-logging" % "commons-logging" % "1.1.1"
libraryDependencies += "net.minidev" % "json-smart" % "1.3.1"
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.4.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.1.0"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
/*mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
  case entry => {
    val strategy = mergeStrategy(entry)
    if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
    else strategy
  }
}}*/
    