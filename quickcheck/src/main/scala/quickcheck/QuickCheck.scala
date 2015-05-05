package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min of 1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min of 2") = forAll { (a: Int, b: Int) =>
    val h = insert(a, insert(b, empty))
    findMin(h) == {if (a < b) a else b}
  }

  property("cleaning heap") = forAll { a: Int =>
    val h = insert(a, empty)
    val h1 = deleteMin(h)
    isEmpty(h1)
  }

  property("extracting all elements result in ordered seq") = forAll { h: H =>
    ordered(extractAll(h))
  }

  property("extracting all elements after delete result in ordered seq") = forAll { h: H =>
    val h1 = deleteMin(h)
    ordered(extractAll(h1))
  }

  property("extracting all elements from melded heaps result in ordered seq") = forAll { (h1: H, h2: H) =>
    ordered(extractAll(meld(h1, h2)))
  }

  property("meld with empty") = forAll { h: H =>
    val m = meld(h, empty)
    extractAll(h) == extractAll(m)
  }

  property("min of 2 melded heaps") = forAll { (h1: H, h2: H)  =>
    val (m1, m2) = (findMin(h1), findMin(h2))
    val m = findMin(meld(h1, h2))
    m == m1 || m == m2
  }

  lazy val genHeap: Gen[H] = for {
    v <- arbitrary[Int]
    h <- oneOf(const(empty), genHeap)
  } yield insert(v, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  def extractAll(h: H): List[Int] =
    if (isEmpty(h)) Nil
    else findMin(h) :: extractAll(deleteMin(h))

  def ordered(l: List[Int]) = l == l.sorted
}
