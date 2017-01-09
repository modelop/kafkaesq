
lazy val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.10.6"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "kafkaesq",
    libraryDependencies ++= Seq(
      //"org.apache.kafka" % "kafka_2.10" % "0.10.1.1"
      "org.apache.kafka" % "kafka_2.10" % "0.9.0.1"
    ),
    mainClass in assembly := Some("Kafkaesq"),
    assemblyJarName in assembly := "kafkaesq.jar"
  )

