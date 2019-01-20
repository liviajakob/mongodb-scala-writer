
name := "EarthWaveLoader"

scalaVersion := "2.12.7"

libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.5.18"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

lazy val global = project
  .in(file("."))
  .settings(settings)
  .aggregate(
    EarthWaveLoader  )

lazy val EarthWaveLoader = project.settings( name := "EarthWaveLoader", settings, commonDependencies )

lazy val settings = Seq()

lazy val commonDependencies = Seq()

assemblyMergeStrategy in assembly := {
    case "BUILD" => MergeStrategy.discard
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
  }
