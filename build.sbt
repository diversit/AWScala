import xerial.sbt.Sonatype.autoImport._

// TODO: 1.11.58 has a breaking change on EMR
lazy val awsJavaSdkVersion = "1.11.126"

lazy val root = (project in file(".")).settings(
  organization := "com.github.seratch",
  name := "awscala",
  version := "0.6.1-SNAPSHOT",
  scalaVersion := "2.12.1",
  crossScalaVersions := Seq("2.12.1", "2.11.8", "2.10.6"),
  publishMavenStyle := true,
  resolvers += "spray repo" at "http://repo.spray.io",
  libraryDependencies ++= Seq(
    "com.amazonaws"    %  "aws-java-sdk-iam"      % awsJavaSdkVersion exclude("commons-logging", "commons-logging"),
    "com.amazonaws"    %  "aws-java-sdk-sts"      % awsJavaSdkVersion exclude("commons-logging", "commons-logging"),
    "com.amazonaws"    %  "aws-java-sdk-ec2"      % awsJavaSdkVersion exclude("commons-logging", "commons-logging"),
    "com.amazonaws"    %  "aws-java-sdk-s3"       % awsJavaSdkVersion exclude("commons-logging", "commons-logging"),
    "com.amazonaws"    %  "aws-java-sdk-sqs"      % awsJavaSdkVersion exclude("commons-logging", "commons-logging"),
    "com.amazonaws"    %  "aws-java-sdk-emr"      % awsJavaSdkVersion exclude("commons-logging", "commons-logging"),
    "com.amazonaws"    %  "aws-java-sdk-redshift" % awsJavaSdkVersion exclude("commons-logging", "commons-logging"),
    "com.amazonaws"    %  "aws-java-sdk-dynamodb" % awsJavaSdkVersion exclude("commons-logging", "commons-logging"),
    "com.amazonaws"    %  "aws-java-sdk-simpledb" % awsJavaSdkVersion exclude("commons-logging", "commons-logging"),
    "org.slf4j"        %  "jcl-over-slf4j"        % "1.7.25", // do aws libs logging over slf4j
    "joda-time"        %  "joda-time"             % "2.9.6",
    "org.joda"         %  "joda-convert"          % "1.8.1",
    "com.github.seratch.com.veact" %% "scala-ssh" % "0.8.0-1" % "provided",
    "org.bouncycastle" %  "bcprov-jdk16"          % "1.46"             % "provided",
    "ch.qos.logback"   %  "logback-classic"       % "1.2.2"            % "test",
    "org.scalatest"    %% "scalatest"             % "3.0.1"            % "test",
    "io.findify"       %% "s3mock"                % "0.2.0"            % "test"
  ),
  sbtPlugin := false,
  transitiveClassifiers in Global := Seq(Artifact.SourceClassifier),
  incOptions := incOptions.value.withNameHashing(true),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { x => false },
  pomExtra := <url>https://github.com/seratch/awscala</url>
    <licenses>
      <license>
        <name>Apache License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:seratch/awscala.git</url>
      <connection>scm:git:git@github.com:seratch/awscala.git</connection>
    </scm>
    <developers>
      <developer>
        <id>seratch</id>
        <name>Kazuhiro Sera</name>
        <url>http://seratch.net/</url>
      </developer>
      <developer>
        <id>mslinn</id>
        <name>Mike Slinn</name>
        <url>https://github.com/mslinn</url>
      </developer>
      <developer>
        <id>Rheeseyb</id>
        <name>RheeseyB</name>
        <url>https://github.com/Rheeseyb</url>
      </developer>
      <developer>
        <id>gribeiro</id>
        <name>Georges Kovacs Ribeiro</name>
        <url>https://github.com/gribeiro</url>
      </developer>
    </developers>,
  organization := "com.github.seratch",
  sonatypeProfileName := "com.github.seratch"
).settings(scalariformSettings)
