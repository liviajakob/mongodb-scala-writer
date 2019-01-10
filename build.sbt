
name := "EarthWaveLoader"

scalaVersion := "2.12.7"

lazy val global = project
  .in(file("."))
  .settings(settings)
  .aggregate(
    EarthWaveLoader,
	StreamLibCore
  )

lazy val EarthWaveLoader = project.settings( name := "EarthWaveLoader,", settings, commonDependencies ).dependsOn(StreamLibCore)

lazy val StreamLibCore = project.settings(name := "StreamLibCore", settings, commonDependencies )  

lazy val settings = Seq()

lazy val commonDependencies = Seq()

  assemblyMergeStrategy in assembly := {
    case "BUILD" => MergeStrategy.discard
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
  }
