/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = false))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => {
      log.debug("do operation")
      root ! op
    }
    case GC => {
      log.debug("start copying ...")
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation => {
      log.debug(s"enqueue pending operation $op")
      pendingQueue = pendingQueue.enqueue(op)
    }
    case CopyFinished => {
      log.debug("copy finished")
      pendingQueue.foreach {
        newRoot ! _
      }
      pendingQueue = Queue.empty[Operation]
      root = newRoot
      context.unbecome()
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case msg @ Insert(requester, id, elem) => {
      log.debug(s"inserting $msg ...")
      if (elem == this.elem) {
        removed = false
        requester ! OperationFinished(id)
      }
      else if (elem < this.elem)
        subtrees.get(Left) match {
          case Some(node) => node ! msg
          case None => {
            val node = context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false))
            subtrees += (Left -> node)
            requester ! OperationFinished(id)
          }
        }
      else
        subtrees.get(Right) match {
          case Some(node) => node ! msg
          case None => {
            val node = context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false))
            subtrees += (Right -> node)
            requester ! OperationFinished(id)
          }
        }
    }
    case msg @ Contains(requester, id, elem) => {
      log.debug(s"contains $msg ...")
      if (elem == this.elem)
        requester ! ContainsResult(id, !removed)
      else if (elem < this.elem)
        subtrees.get(Left) match {
          case Some(node) => node ! msg
          case None => requester ! ContainsResult(id, false)
        }
      else
        subtrees.get(Right) match {
          case Some(node) => node ! msg
          case None => requester ! ContainsResult(id, false)
        }
    }
    case msg @ Remove(requester, id, elem) => {
      log.debug(s"remove $msg ...")
      if (elem == this.elem) {
        removed = true
        requester ! OperationFinished(id)
      } else if (elem < this.elem)
        subtrees.get(Left) match {
          case Some(node) => node ! msg
          case None => requester ! OperationFinished(id)
        }
      else
        subtrees.get(Right) match {
          case Some(node) => node ! msg
          case None => requester ! OperationFinished(id)
        }
    }
    case CopyTo(other) =>
      if (!removed) {
        log.debug(s"elem $elem; not removed; insert itself")
        other ! Insert(context.self, elem, elem)
        context.become(copying(Set.empty[ActorRef], false))
      } else if (!subtrees.isEmpty) {
        log.debug(s"elem $elem; removed; copy children")
        subtrees.values.foreach {
          _ ! CopyTo(other)
        }
        context.become(copying(subtrees.values.toSet, true))
      } else {
        log.debug(s"elem $elem; removed; no children")
        context.self ! PoisonPill
        sender ! CopyFinished
      }
    }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) if !insertConfirmed => {
      log.debug("inserted; copying children")
      subtrees.values.foreach {
        _ ! CopyTo(sender)
      }
      val newExpected = subtrees.values.toSet
      if (newExpected.isEmpty) {
        context.self ! PoisonPill
        context.parent ! CopyFinished
      } else
        context.become(copying(newExpected, true))
    }
    case CopyFinished if insertConfirmed => {
      log.debug("child copied")
      val newExpected = expected - sender
      if (newExpected.isEmpty) {
        context.self ! PoisonPill
        context.parent ! CopyFinished
      } else
        context.become(copying(newExpected, insertConfirmed))
    }
    case msg => {
      log.debug(s"unexpected $msg")
    }
  }

}
