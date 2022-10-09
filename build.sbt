name := "spark-cdm-connector"

//the groupid
organization := "com.microsoft.azure"

// skip all dependencies in the pom file. This is an uber jar
// refernce: https://stackoverflow.com/questions/41670018/how-to-prevent-sbt-to-include-test-dependencies-into-the-pom

import scala.xml.{Node => XmlNode, NodeSeq => XmlNodeSeq, _}
import scala.xml.transform.{RewriteRule, RuleTransformer}
pomPostProcess := { (node: XmlNode) =>
  new RuleTransformer(new RewriteRule {
    override def transform(node: XmlNode): XmlNodeSeq = node match {
      case e: Elem if e.label == "dependency" => scala.xml.NodeSeq.Empty
      case _ => node
    }
  }).transform(node).head
}

version := "spark3.2-1.19.3"

crossPaths := false
ThisBuild / scalaVersion := "2.12.15"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"

libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.13.3"
libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.13.3"

//these libraries already exist in spark HDI 2.4.0 - don't include them building the uber jar
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.3"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.3"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.13.3"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.13.3"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"  % "provided"
libraryDependencies += "log4j" % "log4j" % "1.2.17" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1" % "provided"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.6" % "provided"
libraryDependencies += "com.google.guava" % "guava" % "14.0.1" % "provided"
libraryDependencies += "commons-io" % "commons-io" % "2.4" % "provided"
libraryDependencies += "com.microsoft.azure" % "adal4j" % "1.6.3"
libraryDependencies += "com.microsoft.azure" % "msal4j" % "1.10.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "3.3.1" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.1" % "provided"

libraryDependencies += "org.wildfly.openssl" % "wildfly-openssl" % "1.0.7.Final" % "provided"

resolvers += "Maven Twitter Releases" at "https://maven.twttr.com/"
libraryDependencies += "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.20"

// The main module depends on the mock'd databricks DataricksokenProvider classes.
lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.microsoft.cdm"
  ).dependsOn(child)

// Create the Databricks mocking library without including DataBricks jars. Their only purpose is to enable
// compilation. When we deploy to Databricks, these classes will already exist as part of the Databricks runtime.
// Child and grandchild represent the two mocked Databricks libraries, whose classes are removed when building
// an uber jar -- see assemblyMergeStrategy below.
lazy val child = Project("DatabricksTokenProviderMock", file("DatabricksTokenProviderMock"))
  .settings().dependsOn(grandchild)
lazy val grandchild = Project("DatabricksADTokenMock", file("DatabricksTokenProviderMock/DatabricksADTokenMock"))
  .settings()

//assembly
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.fasterxml.jackson.**" -> "shadeio.@1").inAll,
  ShadeRule.rename("com.nimbusds.**" -> "shadeionimbusds.@1").inAll,
  ShadeRule.rename("com.microsoft.aad.adal4j.**" -> "shadeioadal4j.@1").inAll
)


assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { f =>
        f.data.getName.contains("tokenlibrary") ||
        f.data.getName.contains("SparkCustomEvents") ||
        f.data.getName.contains("hdinsight") ||
        f.data.getName.contains("peregrine") ||
        f.data.getName.contains("mdsdclient") ||
        f.data.getName.contains("spark-enhancement")
  }
}

// build an uber jar
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) =>  MergeStrategy.discard
  //the stubbed-out Databricks jars don't show up in "assemblyExcludedJars" to remove, so manually removing the mocking classes
  case "shaded/databricks/v20180920_b33d810/org/apache/hadoop/fs/azurebfs/oauth2/AzureADToken.class" => MergeStrategy.discard
  case "com/databricks/backend/daemon/data/client/adl/AdlGen2CredentialContextTokenProvider.class" => MergeStrategy.discard
  case x =>  MergeStrategy.first
}

// don't bring scala classes into uber jar
assemblyOption in assembly ~= { _.copy(includeScala = false) }

// don't run tests with "sbt assembly"
test in assembly := {}

// Below is for publishing
artifact in (Compile, packageBin) := {
  val art = (artifact in (Compile, packageBin)).value
  art.withClassifier(Some(""))
}

addArtifact(artifact in (Compile, packageBin), assembly)

// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "com.microsoft.azure"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// Open-source license of your choice
licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

// Where is the source code hosted: GitHub or GitLab?
import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("bissont", "Spark-CDM", "tibisso@microsoft.com"))

// or if you want to set these fields manually
homepage := Some(url("https://github.com/Azure/spark-cdm-connector"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/Azure/new-spark-cdm"),
    "scm:git@github.com:Azure/new-spark-cdm.git"
  )
)
developers := List(
  Developer(id="tibisso", name="Timothy Bisson", email="tibisso@microsoft.com", url=url("https://github.com/bissont")),
  Developer(id="srruj", name="Sricheta Ruj", email="Sricheta.Ruj@microsoft.com", url=url("https://github.com/sricheta92"))
)

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := sonatypePublishToBundle.value

ThisBuild / publishConfiguration := publishConfiguration.value.withOverwrite(true)
ThisBuild / publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
