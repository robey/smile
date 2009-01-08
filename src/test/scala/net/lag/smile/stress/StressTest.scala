/*
 * Copyright (c) 2008, Robey Pointer <robeypointer@gmail.com>
 * ISC licensed. Please see the included LICENSE file for more information.
 */

package net.lag.smile.stress

import net.lag.configgy.Configgy
import net.lag.extensions._
import net.lag.logging.{Level, Logger}
import java.util.Random


trait StressTest {
  val rnd = new Random(System.currentTimeMillis)
  var hosts: Array[String] = _

  def setHosts(newHosts: Array[String]) {
    hosts = newHosts
  }

  def report(name: String, count: Int)(f: => Unit): Unit = {
    val start = System.currentTimeMillis
    f
    val duration = System.currentTimeMillis - start
    val average = duration * 1.0 / count
    println("%s: %d in %d msec (%.2f msec each)".format(name, count, duration, average))
  }

  def generateValue(size: Int): String = {
    val sb = new StringBuffer(size)
    for (idx <- 1 to size) sb.append(('a' + rnd.nextInt('z' - 'a')).asInstanceOf[Char])
    sb.toString()
  }

  def generateValues(size: Int, variance: Int, count: Int): List[String] = {
    (for (i <- 1 to count) yield generateValue(size + rnd.nextInt(variance))).toList
  }
}
