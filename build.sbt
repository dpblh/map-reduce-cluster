name := "map-reduce-cluster"

version := "0.1"

scalaVersion := "2.11.12"

val akkaVersion = "2.5.8"

lazy val `akka-sample-cluster-scala` = project
  .in(file("."))
  .settings(
    organization := "simple",
    scalaVersion := "2.11.12",
    scalacOptions in Compile ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
    javacOptions in Compile ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),
    javaOptions in run ++= Seq("-Xms128m", "-Xmx1024m", "-Djava.library.path=./target/native"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion
    ),
    mainClass in (Compile, run) := Some("sample.cluster.mapReduce.Starter")//,
    // disable parallel tests
//    parallelExecution in Test := false
  )
