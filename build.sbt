scalaVersion := "2.11.6"

mainClass in (Compile, run) := Some("re.toph.hybrid_db.HelloWorld")

resolvers ++= Seq(
  "anormcypher" at "http://repo.anormcypher.org/",
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "anorm" % "2.4.0",
"org.anormcypher" %% "anormcypher" % "0.9.0"
)
