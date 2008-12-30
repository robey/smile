/*
 * Copyright (c) 2008, Robey Pointer <robeypointer@gmail.com>
 * ISC licensed. Please see the included LICENSE file for more information.
 */

package net.lag.smile.stress

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch
import net.lag.configgy.Configgy
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

  class Writer(queue: String, count: Int, size: Int, sizeVariance: Int, pauseMsOnTens: Int,
               startPauseMs: Int, inspector: Inspector) extends Actor
  {
    private val cache = MemcacheClient.create(Array(hosts(0)), "default", "crc32-itu")
    private var id = 0
    private var outstanding = inspector.outstanding
    private var valueBase = generateValue(size + sizeVariance)

    def act {
      Thread.sleep(startPauseMs)
      log.trace("Writer %s starting", queue)
      while (id < count) {
        val valueLen = size + (if (sizeVariance > 0) rnd.nextInt(sizeVariance) else 0)
        val value = id.toString + " " + valueBase.substring(0, valueLen)
        val row = new WrittenRow(System.currentTimeMillis, value)

        inspector ! row

        try {
          cache.set(queue, value)
        } catch {
          case e: Exception => {
            log.fatal("Writer %s caught exception %s", queue, e.toString)
            throw e
          }
        }
        log.trace("Writer %s wrote %s", queue, row)

        id += 1
        if (id % 10 == 0) Thread.sleep(pauseMsOnTens)
        var sleepMs = 400
        while (outstanding.get() > maxOutstanding) {
          log.debug("Writer %s throttling %d", queue, sleepMs)
          Thread.sleep(sleepMs)
          sleepMs = 3200 min sleepMs * 2
        }
      }
      log.debug("Writer %s exiting", queue)
      cache.shutdown()
      exit()
    }
  }

  class Reader(queue: String, pauseMsOnTens: Int, startPauseMs: Int,
               inspector: Inspector) extends Actor
  {
    private val cache = MemcacheClient.create(Array(hosts(0)), "default", "crc32-itu")
    private var rowsRead = 0

    var sleepMs = pollInitialMs

    def act {
      Thread.sleep(startPauseMs)
      log.trace("Reader %s starting", queue)
      while (true) {
        try {
          cache.get(queue) match {
            case None => {
              Thread.sleep(sleepMs)
              sleepMs = pollMaxMs min (sleepMs * pollMultiplicand).toLong
            }
            case Some(value) => {
              val row = new ReadRow(System.currentTimeMillis, value)
              log.trace("Reader %s read %s", queue, row)
              inspector ! row
              sleepMs = pollInitialMs
            }
          }
        } catch {
          case e: Exception => {
            log.fatal("Reader " + queue + " caught exception " + e)
            throw e
          }
        }
        receiveWithin(0) {
          case TIMEOUT => null
          case InspectorFinished => {
            log.debug("%s reader exiting", queue)
            reply(ReaderFinished)
            cache.shutdown()
            exit()
          }
          case unknown =>
            throw new Exception("Reader " + queue + " received unknown " + unknown.toString)
        }
        if (rowsRead % 10 == 0) Thread.sleep(pauseMsOnTens)
      }
    }
  }

  class Inspector(queue: String, count: Int) extends Actor {
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
          log.trace("Inspector %s found rows written=%s read=%s", queue, writtenRow, readRow)

          val latencyMs = readRow.timestampMs - writtenRow.timestampMs
          minLatencyMs = minLatencyMs min latencyMs
          maxLatencyMs = maxLatencyMs max latencyMs
          totalLatencyMs += latencyMs

          if (writtenRow.value != readRow.value) {
            log.fatal("Inspector %s expected=%s received=%s", queue, writtenRow, readRow)
            assert(false)
          }
          rowsChecked += 1
          bytesRead += readRow.value.length
          if (rowsChecked % 250 == 0) {
            log.debug("Inspector %s writeRows.size=%d readRows.size=%d", queue, wSize, rSize)
          }

          if (rowsChecked >= count) {
            if (writtenRows.size > 0) {
              log.fatal("Inspector %s expect written empty, size=%d", queue, writtenRows.size)
              assert(false)
            }
            if (readRows.size > 0) {
              log.fatal("Inspector %s expect readempty, size=", queue, writtenRows.size)
              assert(false)
            }

            log.info("%s PASSED: latency min/avg/max = %d / %d / %d bytes avg/total %d %d",
                     queue, minLatencyMs, (totalLatencyMs / rowsChecked).toInt, maxLatencyMs,
                     (bytesRead / rowsChecked).toInt, bytesRead)

            // Wait for reader to return to ensure it cannot bork next test.
            reader !? InspectorFinished
            finished.countDown()
            log.debug("%s inspector exiting", queue)
            exit()
          }
        }
        receive {
          case row: WrittenRow => writtenRows += row
          case row: ReadRow => readRows += row
          case unknown =>
            log.fatal("Inspector %s received unknown message %s", queue, unknown.toString)
        }
      }
    }
  }

  def drain(queue: String) {
    val cache = MemcacheClient.create(Array(hosts(0)), "default", "crc32-itu")
    var done = false
    var rows = 0
    while (!done) {
      cache.get(queue) match {
        case None => done = true
        case Some(value) => rows += 1
      }
    }
    log.debug("%s drained %d rows", queue, rows)
    cache.shutdown
  }

  def go(queue: String, count: Int, size: Int, sizeVariance: Int,
         writePauseOnTensMs: Int, readPauseOnTensMs: Int,
         writeStartPauseMs: Int, readStartPauseMs: Int)
  {
    log.info(
      "go host=%s queue=%s count=%d size=%d sizeVariance=%d",
      hosts(0), queue, count, size, sizeVariance)
    log.info("writePauseOnTensMs=%d readPauseOnTensMs=%d", writePauseOnTensMs, readPauseOnTensMs)
    log.info("writeStartPauseMs=%d readStartPauseMs=%d", writeStartPauseMs, readStartPauseMs)

    drain(queue)

    var inspector = new Inspector(queue, count)
    var reader = new Reader(queue, readPauseOnTensMs, readStartPauseMs, inspector)
    var writer = new Writer(
      queue, count, size, sizeVariance, writePauseOnTensMs, writeStartPauseMs, inspector)
    inspector.set(reader, writer)

    inspector.start
    reader.start
    writer.start

    log.trace("go await inspector")
    inspector.finished.await()
    log.trace("go finished")
  }

  def test(count: Int, size: Int, variance: Int, startPauseMs: Int, rowPauseMs: Int) = {
    // Simulatenous producer consumer (with small counts, devolves to a late consumer)
/*    go("smile-simultaneous", count, size, variance, 0, 0, 0, 0)
    // Late producer
    go("smile-lateproducer", count, size, variance, 0, 0, startPauseMs, 0)
    // Late consumer
    go("smile-lateconsumer", count, size, variance, 0, 0, 0, startPauseMs)
    // Slow producer
    go("smile-slowproducer", count, size, variance, rowPauseMs, 0, 0, 0)
    // Slow consumer
*/    go("smile-slowconsumer", count, size, variance, 0, rowPauseMs, 0, 0)
  }

  def test() {
    //
    // Unit-like tests
    //
    test(3, 10, 0, 250, 250)

    //
    // Single-Queue tests
    //
    test(10000, 256, 8000, 250, 20)


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
      k.test()
    }
  }
}
