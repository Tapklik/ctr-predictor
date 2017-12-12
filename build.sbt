name := "CTR"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.2.0",
    "org.apache.spark" %% "spark-streaming" % "2.2.0",
    "org.apache.spark" %% "spark-sql" % "2.2.0",
    "org.apache.spark" %% "spark-mllib" % "2.2.0",
    "org.scalatest" %% "scalatest" % "2.1.5" % "test",
    "com.github.scopt" %% "scopt" % "3.3.0"

)

//mainClass in Compile := Some("tryout.Tryout")





