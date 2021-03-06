package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var keySeq = Map.empty[String, Long]

  var persistence = context.system.actorOf(persistenceProps)


  override def preStart(): Unit = {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = {
    case Replicas(replicas) => {
      // TODO: store it
    }
    case Insert(k, v, id) => {
      kv += (k -> v)
      sender ! OperationAck(id)
    }
    case Remove(k, id) => {
      kv -= k
      sender ! OperationAck(id)
    }
    case Get(k, id) => sender ! GetResult(k, kv.get(k), id)
  }

  val replica: Receive = {
    case Get(k, id) => sender ! GetResult(k, kv.get(k), id)
    case Snapshot(k, vo, seq) => {
      val expectedSeq = keySeq.getOrElse(k, 0L)
      if (seq < expectedSeq) {
        sender ! SnapshotAck(k, seq)
      } else if (seq == expectedSeq) {
        vo match {
          case Some(v) => kv += (k -> v)
          case None => kv -= k
        }
        secondaries += (self -> sender)
        val persist = Persist(k, vo, seq)
        persistence ! persist
        context.system.scheduler.scheduleOnce(100.milliseconds, self, persist)
        keySeq += (k -> (seq + 1L))
      }
    }
    case persist: Persist => {
      persistence ! persist
      context.system.scheduler.scheduleOnce(100.milliseconds, self, persist)
    }
    case Persisted(k, id) => {
      secondaries(self) ! SnapshotAck(k, id)
    }
  }

}

