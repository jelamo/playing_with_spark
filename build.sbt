name := "snowplow_spark_aggregators"

version := "1.0"

scalaVersion in ThisBuild := "2.11.7"

// spark version to be used
val sparkVersion = "2.0.2"
val sparkDependencyScope = "provided"

resolvers += "Redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope,
  "com.amazon.redshift" % "redshift-jdbc41" % "1.2.1.1001",
  "com.github.nscala-time" %% "nscala-time" % "2.14.0",
  "com.frugalmechanic" %% "scala-optparse" % "1.1.2",
  "com.amazonaws" % "aws-java-sdk" % "1.11.66",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3" excludeAll(
    ExclusionRule(organization = ("javax.servlet")),
    ExclusionRule("commons-beanutils"),
    ExclusionRule("commons-collections")
  )
)
