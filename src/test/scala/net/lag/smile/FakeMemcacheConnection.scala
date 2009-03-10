/*
 * Copyright (c) 2008, Robey Pointer <robeypointer@gmail.com>
 * ISC licensed. Please see the included LICENSE file for more information.
 */

package net.lag.smile

import java.io.{InputStream, IOException, OutputStream}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.{CountDownLatch, Semaphore, TimeUnit}
import scala.collection.mutable


abstract case class Task()
case class Receive(count: Int) extends Task
case class Send(data: Array[Byte]) extends Task
case class Sleep(ms: Int) extends Task
case object Disconnect extends Task
case object KillListenSocket extends Task


class FakeMemcacheConnection(tasks: List[Task]) extends Runnable {
  val socket = new ServerSocket(0, 100)
  val port = socket.getLocalPort

  val gotConnection = new Semaphore(0)
  val disconnected = new CountDownLatch(1)
  val thread = new Thread(this)
  thread.setDaemon(true)

  val dataRead = new mutable.ListBuffer[Array[Byte]]

  var client: Socket = null
  var inStream: InputStream = null
  var outStream: OutputStream = null

  override def run() = try {
    while (true) {
      getClient

      for (t <- tasks) {
        t match {
          case Receive(count) =>
            var sofar = 0
            val buffer = new Array[Byte](count)
            while (sofar < count) {
              val n = inStream.read(buffer, sofar, count - sofar)
              if (n < 0) {
                throw new IOException("eof")
              }
              sofar += n
            }
            dataRead += buffer
          case Send(data) =>
            outStream.write(data)
          case Sleep(n) =>
            try {
              Thread.sleep(n)
            } catch {
              case x: InterruptedException =>
            }
          case Disconnect =>
            while (gotConnection.availablePermits > 0) {
              Thread.sleep(50)
            }
            client.close
            disconnected.countDown
            getClient
          case KillListenSocket =>
            socket.close
        }
      }
    }
  } catch {
    case e: Exception =>
  }

  def start = {
    thread.start
  }

  def stop = {
    thread.interrupt
  }

  def getClient = {
    client = socket.accept
    inStream = client.getInputStream
    outStream = client.getOutputStream
    gotConnection.release
  }

  def awaitConnection(msec: Int) = {
    gotConnection.tryAcquire(msec, TimeUnit.MILLISECONDS)
  }

  def awaitDisconnected(msec: Int) = {
    disconnected.await(msec, TimeUnit.MILLISECONDS)
  }

  def fromClient(): List[String] = {
    dataRead map (new String(_)) toList
  }
}
