name := "gaussianmixture"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies+="org.apache.spark"%"spark-core_2.12"%"3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % Test

mainClass in Compile := Some("GaussianMixture")

lazy val commonSettings = Seq(
    version := "0.1-SNAPSHOT",
    organization := "com.example",
    scalaVersion := "2.11.6",
    test in assembly := {}
)

lazy val app = (project in file("GaussianMixture")).
    settings(commonSettings: _*).
    settings(
        mainClass in assembly := Some("GaussianMixture")
    )

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _ *) => MergeStrategy.discard
   
    case x =>
        MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "project.jar"

fullClasspath in Runtime := (fullClasspath in (Compile, run)).value