import sbt._
import com.twitter.sbt.StandardProject


class SmileProject(info: ProjectInfo) extends StandardProject(info) {
  val scala0Repo = "scala0.net" at "http://scala0.net/repositories/"
  val specs = "org.scala-tools.testing" % "specs" % "1.6.2.1"
  val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7"
  val vscaladoc = "org.scala-tools" % "vscaladoc" % "1.1-md-3"

  val hamcrest  = "org.hamcrest" % "hamcrest-all" % "1.1"
  val jmock     = "org.jmock" % "jmock" % "2.4.0"
  val objenesis = "org.objenesis" % "objenesis" % "1.1"

  val configgy = "net.lag" % "configgy" % "1.5.3"
  val naggati = "net.lag" % "naggati" % "0.7.2"
  val mina = "org.apache.mina" % "mina-core" % "2.0.0-M6"
  val slf4j_api = "org.slf4j" % "slf4j-api" % "1.5.2"
  val slf4j_jdk14 = "org.slf4j" % "slf4j-jdk14" % "1.5.2"

  val util = "com.twitter" % "util" % "1.0-SNAPSHOT"

  override def pomExtra =
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>

  override def releaseBuild = true
}
