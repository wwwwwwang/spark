name := "carpo"

version := "1.0.0"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark"   % "spark-core_2.11"        % "2.2.0"   % "provided"
libraryDependencies += "org.apache.spark"   % "spark-sql_2.11"         % "2.2.0"   % "provided"
libraryDependencies += "com.typesafe"        % "config"                 % "1.3.1"
libraryDependencies += "commons-cli"         % "commons-cli"            % "1.2"

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

    