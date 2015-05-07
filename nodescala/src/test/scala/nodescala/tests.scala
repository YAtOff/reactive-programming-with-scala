package nodescala

import org.scalatest.concurrent.AsyncAssertions

import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be completed") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }

  test("A Future should never be completed") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("All futures return") {
    val list: List[Future[Int]] = List(
      Future.always(1),
      Future.always(2),
      Future.delay(1 second) continueWith { _ => 3 }
    )
    val all: Future[List[Int]] = Future.all(list)
    assert(Await.result(all, 1.1 seconds).sum == 6)
  }

  test("Any retruns first future result") {
    val list: List[Future[Int]] = List(
      Future.delay(1 second) continueWith { _ => 1 },
      Future.delay(2 second) continueWith { _ => 2 }
    )
    val first: Future[Int] = Future.any(list)
    assert(Await.result(first, 1.1 seconds) == 1)
  }

  test("Future.continue should handle exceptions thrown by the user specified continuation function") {
    val f = Future.always(1)
    val c1 = f continue { _ => throw new UnsupportedOperationException("...") }
    intercept[java.lang.UnsupportedOperationException] {
      Await.result(c1, 1 second)
    }
  }

  test("Future.continueWith should handle exceptions thrown by the user specified continuation function") {
    val f = Future.always(1)
    val c1 = f continueWith { _ => throw new UnsupportedOperationException("...") }
    intercept[java.lang.UnsupportedOperationException] {
      Await.result(c1, 1 second)
    }
  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }
  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}

class AsyncNodeScalaSuite extends FunSuite with AsyncAssertions {

  test("A Future should be completed after 1s delay") {
    val w = new Waiter
    val start = System.currentTimeMillis()

    Future.delay(1 second) onComplete { case _ =>
      val duration = System.currentTimeMillis() - start
      assert(duration >= 1000L)
      assert(duration < 1100L)

      w.dismiss()
    }

    w.await(timeout(2 seconds))
  }

  /*
  test("Two sequential delays of 1s should delay by 2s") {
    val w = new Waiter
    val start = System.currentTimeMillis()

    val combined = for {
      f1 <- Future.delay(1 second)
      f2 <- Future.delay(1 second)
    } yield ()

    combined onComplete { case _ =>
      val duration = System.currentTimeMillis() - start
      assert(duration >= 2000L)
      assert(duration < 2100L)

      w.dismiss()
    }

    w.await(timeout(3 seconds))
  }
  */
}



