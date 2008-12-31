/*
 * Copyright (c) 2008, Robey Pointer <robeypointer@gmail.com>
 * ISC licensed. Please see the included LICENSE file for more information.
 */

package net.lag.smile.stress

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}
import net.lag.configgy.{Configgy, Config}
import net.lag.extensions._
import net.lag.logging.{Level, Logger}
import net.lag.smile.MemcacheClient
import scala.actors._
import scala.collection.mutable.ListBuffer

case class ReadRow(timestampMs:Long, value: String)
case class WrittenRow(timestampMs: Long, value: String)
case class InspectorFinished
case class ReaderFinished

class KestrelTest extends StressTest {
  val log = Logger.get
  val pollInitialMs = 10L
  val pollMaxMs = 250L
  val pollMultiplicand = 1.5
  val maxOutstanding = 1000  // Feather back writer throttle if it should get too far ahead
  val outstanding = new AtomicInteger(0)
  val errorBackoffMs = 500
  val config = new Config()

  class Writer(val queue: String, count: Int, size: Int, sizeVariance: Int, pauseMsOnTens: Int,
               startPauseMs: Int, inspector: Inspector) extends Actor
  {
    private val cache = MemcacheClient.create(config)
    private var id = 0
    private var outstanding = inspector.outstanding
    private var valueBase = generateValue(size + sizeVariance)
    private var timeouts = 0
    private var offline = 0
    val name = "Writer " + queue

    def act {
      Thread.sleep(startPauseMs)
      log.trace("%s starting", name)
      while (id < count) {
        val valueLen = size + nextInt(sizeVariance)
        val value = id.toString + " " + valueBase.substring(0, valueLen)
        val row = new WrittenRow(System.currentTimeMillis, value)

        inspector ! row

        var done = false
        while (!done) {
          try {
            cache.set(queue, value)
            done = true
          } catch {
            case e: MemcacheServerTimeout => {
              log.warning("%s got a timeout", name)
              timeouts += 1
              Thread.sleep(errorBackoffMs)
            }
            case e: MemcacheServerOffline => {
              log.warning("%s got an offline", name)
              offline += 1
              Thread.sleep(errorBackoffMs)
            }
            case e: Exception => {
              log.fatal("%s caught exception %s", name, e.toString)
              throw e
            }
          }
        }
        log.trace("%s wrote %s", name, row)

        id += 1
        if (id % 10 == 0) Thread.sleep(pauseMsOnTens + nextInt(pauseMsOnTens))
        var sleepMs = 400
        while (outstanding.get() > maxOutstanding) {
          log.debug("%s throttling %d", name, sleepMs)
          Thread.sleep(sleepMs)
          sleepMs = 3200 min sleepMs * 2
        }
      }
      if (timeouts > 0 || offline > 0) {
        log.warning("%s timeouts=%d offline=%d", name, timeouts, offline)
      }
      log.debug("%s exiting", name)
      cache.shutdown()
      exit()
    }
  }

  class Reader(val queue: String, pauseMsOnTens: Int, startPauseMs: Int,
               inspector: Inspector) extends Actor
  {
    private val cache = MemcacheClient.create(config)
    private var rowsRead = 0
    private var timeouts = 0
    private var offline = 0
    val name = "Reader " + queue

    var sleepMs = pollInitialMs

    def act {
      Thread.sleep(startPauseMs)
      log.trace("%s starting", name)
      while (true) {
        try {
          cache.get(queue) match {
            case None => {
              Thread.sleep(sleepMs)
              sleepMs = pollMaxMs min (sleepMs * pollMultiplicand).toLong
            }
            case Some(value) => {
              val row = new ReadRow(System.currentTimeMillis, value)
              log.trace("%s read %s", name, row)
              inspector ! row
              sleepMs = pollInitialMs
            }
          }
        } catch {
          case e: MemcacheServerTimeout => {
            log.warning("%s got a timeout", name)
            timeouts += 1
            Thread.sleep(errorBackoffMs)
          }
          case e: MemcacheServerOffline => {
            log.warning("%s got an offline", name)
            offline += 1
            Thread.sleep(errorBackoffMs)
          }
          case e: Exception => {
            log.fatal("%s caught exception %s", name, e)
            throw e
          }
        }
        receiveWithin(0) {
          case TIMEOUT => null
          case InspectorFinished => {
            if (timeouts > 0 || offline > 0) {
              log.warning("%s timeouts=%d offline=%d", name, timeouts, offline)
            }
            log.debug("%s exiting", name)
            reply(ReaderFinished)
            cache.shutdown()
            exit()
          }
          case unknown =>
            throw new Exception(name + " received unknown " + unknown.toString)
        }
        if (rowsRead % 10 == 0) Thread.sleep(pauseMsOnTens + nextInt(pauseMsOnTens))
      }
    }
  }

  class Inspector(val queue: String, count: Int) extends Actor {
    private var minLatencyMs = Math.MAX_LONG
    private var maxLatencyMs = Math.MIN_LONG
    private var totalLatencyMs = 0L
    private var rowsChecked = 0
    private var bytesRead = 0L
    private val writtenRows = new ListBuffer[WrittenRow]()
    private val readRows = new ListBuffer[ReadRow]()
    private var reader: Reader = _
    private var writer: Writer = _
    val finished = new CountDownLatch(1)
    val outstanding = new AtomicInteger()
    val name = "Inspector " + queue

    def set(reader: Reader, writer: Writer) {
      this.reader = reader
      this.writer = writer
    }

    def act {
      while (true) {
        val wSize = writtenRows.size
        val rSize = readRows.size
        outstanding.set(wSize - rSize)
        if (wSize > 0 && rSize > 0) {
          val readRow = readRows.remove(0)
          val writtenRow = writtenRows.remove(0)
          log.trace("%s found rows written=%s read=%s", name, writtenRow, readRow)

          val latencyMs = readRow.timestampMs - writtenRow.timestampMs
          minLatencyMs = minLatencyMs min latencyMs
          maxLatencyMs = maxLatencyMs max latencyMs
          totalLatencyMs += latencyMs

          if (writtenRow.value != readRow.value) {
            log.fatal("%s expected=%s received=%s", name, writtenRow, readRow)
            assert(false)
          }
          rowsChecked += 1
          bytesRead += readRow.value.length
          if (rowsChecked % 250 == 0) {
            log.debug("%s writeRows.size=%d readRows.size=%d", name, wSize, rSize)
          }

          if (rowsChecked >= count) {
            if (writtenRows.size > 0) {
              log.fatal("%s expect written empty, size=%d", name, writtenRows.size)
              assert(false)
            }
            if (readRows.size > 0) {
              log.fatal("%s expect readempty, size=", name, writtenRows.size)
              assert(false)
            }

            log.info("%s PASSED: latency min/avg/max = %d / %d / %d bytes avg/total %d %d",
                     name, minLatencyMs, (totalLatencyMs / rowsChecked).toInt, maxLatencyMs,
                     (bytesRead / rowsChecked).toInt, bytesRead)

            // Wait for reader to return to ensure it cannot bork next test.
            reader !? InspectorFinished
            finished.countDown()
            log.debug("%s exiting", name)
            exit()
          }
        }
        receive {
          case row: WrittenRow => writtenRows += row
          case row: ReadRow => readRows += row
          case unknown =>
            log.fatal("%s received unknown message %s", name, unknown.toString)
        }
      }
    }
  }

  def drain(queue: String) {
    log.debug("drain %s", queue)
    val cache = MemcacheClient.create(config)
    var done = false
    var rows = 0
    var timeout = 0
    var offline = 0

    while (!done) {
      try {
        cache.get(queue) match {
          case None => done = true
          case Some(value) => rows += 1
        }
      } catch {
        case e: MemcacheServerOffline => {
          log.debug("%s drain got an offline", queue)
          offline += 1
          Thread.sleep(errorBackoffMs)
        }
        case e: MemcacheServerTimeout => {
          log.debug("%s drain got a timeout", queue)
          timeout += 1
          Thread.sleep(errorBackoffMs)
        }
      }
    }
    log.debug("%s drained %d rows with %d timeouts %d offline", queue, rows, timeout, offline)
    cache.shutdown
  }

  def nextInt(range: Int) = if (range == 0) 0 else rnd.nextInt(range)

  def test(queues: List[String], count: Int, size: Int, sizeVariance: Int,
           writePauseOnTensMs: Int, readPauseOnTensMs: Int,
           writeStartPauseMs: Int, readStartPauseMs: Int)
  {
    log.info("TEST host=%s queues=%s", hosts(0), queues)
    log.info("count=%d size=%d sizeVariance=%d", count, size, sizeVariance)
    log.info("writePauseOnTensMs=%d readPauseOnTensMs=%d", writePauseOnTensMs, readPauseOnTensMs)
    log.info("writeStartPauseMs=%d readStartPauseMs=%d", writeStartPauseMs, readStartPauseMs)

    queues.foreach(queue => drain(queue))

    var inspectors = new ListBuffer[Inspector]

    for (queue <- queues) {
      var inspector = new Inspector(queue, count)
      inspectors += inspector
      var reader = new Reader(queue, readPauseOnTensMs, readStartPauseMs, inspector)
      var writer = new Writer(
        queue, count, size, sizeVariance, writePauseOnTensMs, writeStartPauseMs, inspector)
      inspector.set(reader, writer)

      inspector.start
      reader.start
      writer.start
    }

    for (inspector <- inspectors) {
      log.trace("go await inspector %s", inspector.name)
      while (!inspector.finished.await(10, TimeUnit.SECONDS)) {
        log.debug("go await inspector %s", inspector.name)
      }
    }
    log.trace("go finished")
  }

  def queueList(base: String, count: Int): List[String] = {
    val rv = new ListBuffer[String]
    for (idx <- 1 to count) {
      rv += base + ":" + idx
    }
    rv.toList
  }

  def suite(queues: Int, count: Int, size: Int, variance: Int, startPauseMs: Int, rowPauseMs: Int) {
    // Simultaneous producer consumer (with small counts, devolves to a late consumer)
    test(queueList("smile-simultaneous", queues), count, size, variance, 0, 0, 0, 0)
    // Late producer
    test(queueList("smile-lateproducer", queues), count, size, variance, 0, 0, startPauseMs, 0)
    // Late consumer
    test(queueList("smile-lateconsumer", queues), count, size, variance, 0, 0, 0, startPauseMs)
    // Slow producer
    test(queueList("smile-slowproducer", queues), count, size, variance, rowPauseMs, 0, 0, 0)
    // Slow consumer
    test(queueList("smile-slowconsumer", queues), count, size, variance, 0, rowPauseMs, 0, 0)
  }

  def go() {
    // The default retry_delay really drags a test out in the face of a network issue.
    config.setInt("retry_delay", 5)
    // The default read_timeout is a bit too tetchy when faced with even moderate network latency
    config.setInt("read_timeout", 8)
    config.setString("servers", hosts(0))
    config.setString("distribution", "default")
    config.setString("hash", "crc32-itu")

    //
    // Unit-like tests
    //
    suite(1, 3, 10, 0, 250, 250)

    //
    // Single-Queue tests
    //
    suite(1, 10000, 256, 8000, 250, 10)

    //
    // 3-Queue tests
    //
    suite(3, 10000, 256, 8000, 250, 10)
    suite(3, 10000, 2, 4, 250, 5)
    suite(3, 10000, 2, 4, 250, 25)

    // 12-Queue tests
    suite(12, 10000, 2, 4, 1000, 10)
    List(2, 15, 25, 50, 75).foreach(pause => suite(12, 500, 2, 4, pause * 10, pause))

    log.info("TESTS PASS")
  }
}

object Kestrel {
  def main(args: Array[String]): Unit = {
    if (args.size != 1) {
      println("Must specify 1 host")
    } else {
      Configgy.configure("test.conf")
      val k = new KestrelTest()
      k.setHosts(args)
      k.go()
    }
  }
}
