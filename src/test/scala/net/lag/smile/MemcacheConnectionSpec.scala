/*
 * Copyright (c) 2008, Robey Pointer <robeypointer@gmail.com>
 * ISC licensed. Please see the included LICENSE file for more information.
 */

package net.lag.smile

import net.lag.naggati.Steps._
import java.nio.ByteOrder
import org.apache.mina.core.buffer.IoBuffer
import org.apache.mina.core.session.{AbstractIoSession, DummySession, IoSession}
import org.apache.mina.filter.codec._
import org.specs._


object MemcacheConnectionSpec extends Specification {

  val pool = new ServerPool
  var server: FakeMemcacheConnection = null
  var conn: MemcacheConnection = null


  def data(v: Option[MemcacheResponse.Value]) = {
    v match {
      case None => ""
      case Some(value) => new String(value.data, "utf-8")
    }
  }

  def data(map: Map[String, MemcacheResponse.Value]) = {
    Map.empty[String, String] ++ (for ((k, v) <- map) yield (k, new String(v.data, "utf-8")))
  }


  "MemcacheConnection" should {
    doAfter {
      server.stop
      if (conn != null) {
        conn.shutdown
      }
      conn = null
    }


    "connect to localhost" in {
      server = new FakeMemcacheConnection(Nil)
      server.start

      conn = new MemcacheConnection("localhost", server.port, 1)
      conn.pool = pool
      conn.ensureConnected mustBe true
      server.awaitConnection(500) mustBe true
    }

    "correctly indicate a failed connection" in {
      server = new FakeMemcacheConnection(Nil)
      server.start

      conn = new MemcacheConnection("localhost", server.port + 1, 1)
      conn.pool = pool
      conn.ensureConnected mustBe false
      server.awaitConnection(500) mustBe false
    }

    "attempt a reconnect if a server disconnects" in {
      server = new FakeMemcacheConnection(Disconnect :: Send("VALUE fail 0 2\r\nno\r\nEND\r\n".getBytes) :: Nil)
      server.start

      conn = new MemcacheConnection("localhost", server.port, 1)
      conn.pool = pool
      conn.ensureConnected mustBe true
      server.awaitConnection(500) mustBe true
      server.awaitDisconnected(500) mustBe true
      server.awaitConnection(1) mustBe false
      data(conn.get("fail")) mustEqual "no"

      conn.ensureConnected mustBe true
      server.awaitConnection(500) mustBe true
    }

    "mark a server as dead when it vanishes, and try again after a delay" in {
      server = new FakeMemcacheConnection(Receive(10) :: Send("VALUE fail 0 2\r\nno\r\nEND\r\n".getBytes) ::
        KillListenSocket :: Disconnect :: Nil)
      server.start

      conn = new MemcacheConnection("localhost", server.port, 1)
      conn.pool = pool
      conn.ensureConnected mustBe true
      server.awaitConnection(500) mustBe true
      data(conn.get("fail")) mustEqual "no"

      server.awaitDisconnected(500) mustBe true
      server.awaitConnection(1) mustBe false

      conn.ensureConnected mustBe false
      conn.delaying must beSome[Long]
      conn.session mustEqual None
      server.stop

      // now verify that the server comes back.
      Time.advance(pool.retryDelay + 1)
      server = new FakeMemcacheConnection(Receive(10) :: Send("VALUE fail 0 3\r\nyes\r\nEND\r\n".getBytes) :: Nil, server.port)
      server.start
      conn.ensureConnected mustBe true
      server.awaitConnection(500) mustBe true
      data(conn.get("fail")) mustEqual "yes"
    }

    "get" in {
      "a single value" in {
        server = new FakeMemcacheConnection(Receive(9) ::
          Send("VALUE cat 0 5\r\nhello\r\nEND\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        data(conn.get("cat")) mustEqual "hello"
        server.fromClient mustEqual List("get cat\r\n")
      }

      "an empty value" in {
        server = new FakeMemcacheConnection(Receive(9) :: Send("END\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        conn.get("cat") mustEqual None
        server.fromClient mustEqual List("get cat\r\n")
      }

      "multiple values" in {
        server = new FakeMemcacheConnection(Receive(13) ::
          Send("VALUE cat 0 5\r\nhello\r\nVALUE dog 0 7\r\ngoodbye\r\nEND\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        data(conn.get(Array("cat", "dog"))) mustEqual Map("cat" -> "hello", "dog" -> "goodbye")
        server.fromClient mustEqual List("get cat dog\r\n")
      }

      "two on the same connection" in {
        server = new FakeMemcacheConnection(Receive(9) ::
          Send("VALUE cat 0 5\r\nhello\r\nEND\r\n".getBytes) ::
          Receive(9) ::
          Send("VALUE dog 0 7\r\ngoodbye\r\nEND\r\n".getBytes) ::
          Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        data(conn.get("cat")) mustEqual "hello"
        data(conn.get("dog")) mustEqual "goodbye"
        server.fromClient mustEqual List("get cat\r\n", "get dog\r\n")
      }

      "timeout" in {
        server = new FakeMemcacheConnection(Receive(9) :: Sleep(1200) :: Send("END\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        // it bothers me that this has to be a whole second. but mina doesn't support msec yet.
        conn.pool.readTimeout = 1000
        data(conn.get("cat")) must throwA(new MemcacheServerTimeout)
      }

      "throw an exception for a bad server" in {
        server = new FakeMemcacheConnection(Receive(9) ::
          Send("CLIENT_ERROR i feel ill\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        data(conn.get("cat")) must throwA(new MemcacheServerException(""))
      }
    }

    "set" in {
      "a single value" in {
        server = new FakeMemcacheConnection(Receive(24) :: Send("STORED\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        conn.set("cat", "hello".getBytes, 0, 500)
        server.fromClient mustEqual List("set cat 0 500 5\r\nhello\r\n")
      }

      "with server error" in {
        server = new FakeMemcacheConnection(Receive(24) :: Send("ERROR\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        conn.set("cat", "hello".getBytes, 0, 500) must throwA(new MemcacheServerException(""))
        server.fromClient mustEqual List("set cat 0 500 5\r\nhello\r\n")
      }
    }

    // impl is identical to "set"
    "add" in {
      "a single value" in {
        server = new FakeMemcacheConnection(Receive(24) :: Send("STORED\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        conn.add("cat", "hello".getBytes, 0, 500) mustBe true
        server.fromClient mustEqual List("add cat 0 500 5\r\nhello\r\n")
      }

      "which returns NOT_STORED" in {
        server = new FakeMemcacheConnection(Receive(24) :: Send("NOT_STORED\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        conn.add("cat", "hello".getBytes, 0, 500) mustBe false
        server.fromClient mustEqual List("add cat 0 500 5\r\nhello\r\n")
      }
    }

    // impl is identical to "set"
    "replace" in {
      "a single value" in {
        server = new FakeMemcacheConnection(Receive(28) :: Send("STORED\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        conn.replace("cat", "hello".getBytes, 0, 500) mustBe true
        server.fromClient mustEqual List("replace cat 0 500 5\r\nhello\r\n")
      }

      "which returns NOT_STORED" in {
        server = new FakeMemcacheConnection(Receive(28) :: Send("NOT_STORED\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        conn.replace("cat", "hello".getBytes, 0, 500) mustBe false
        server.fromClient mustEqual List("replace cat 0 500 5\r\nhello\r\n")
      }
    }

    // impl is identical to "set"
    "append" in {
      "a single value" in {
        server = new FakeMemcacheConnection(Receive(27) :: Send("STORED\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        conn.append("cat", "hello".getBytes, 0, 500)
        server.fromClient mustEqual List("append cat 0 500 5\r\nhello\r\n")
      }
    }

    // impl is identical to "set"
    "prepend" in {
      "a single value" in {
        server = new FakeMemcacheConnection(Receive(28) :: Send("STORED\r\n".getBytes) :: Nil)
        server.start

        conn = new MemcacheConnection("localhost", server.port, 1)
        conn.pool = pool
        conn.prepend("cat", "hello".getBytes, 0, 500)
        server.fromClient mustEqual List("prepend cat 0 500 5\r\nhello\r\n")
      }
    }
  }
}
