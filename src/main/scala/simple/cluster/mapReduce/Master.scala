package simple.cluster.mapReduce

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.UUID

import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import akka.routing.FromConfig

//#service
class Master extends Actor {

  val batchSize = 10


  // This router is used both with lookup and deploy of routees. If you
  // have a router with only lookup of routees you can use Props.empty
  // instead of Props[StatsWorker.class].
  val mapperRouter = context.actorOf(
    Props[MapperWorker],
    name = "reducerRouter"
  )
  val reducerRouter = context.actorOf(
    FromConfig.props(Props[ReducerWorker]),
    name = "workerRouter"
  )

  def receive = {
    case ReadFromFile(filePath, start, end) =>

      val client = sender()

      val iterator = scala.io.Source.fromFile(filePath).getLines
      // todo wtf
      val size = Math.ceil(scala.io.Source.fromFile(filePath).getLines.size / batchSize.toDouble).toInt


      val supervisor = context.actorOf(Props(
        classOf[SupervisorMapper], size, self, client)
      )

      foreachLine(iterator) { array =>
        println("99999999999")
        mapperRouter.tell(Mapper(array, start, end), supervisor)
      }

    case MapperFinal(uuid, client) =>

      val iterator = scala.io.Source.fromFile(uuid).getLines
      // todo wtf
      val size = Math.ceil(scala.io.Source.fromFile(uuid).getLines.size / batchSize.toDouble).toInt

      val supervisor = context.actorOf(Props(
        classOf[SupervisorReducer], size, client)
      )

      foreachLine(iterator) { array =>
        println("888888888")
        reducerRouter.tell(
          ConsistentHashableEnvelope(Reduce(array), array), supervisor)
      }

      Fs.remove(uuid)
  }

  def foreachLine[A](iterator: Iterator[A])(apply: List[A] => Unit) = {
    val bat = scala.collection.mutable.ArrayBuffer[A]()
    for (line <- iterator) {
      bat += line
      if (bat.size >= batchSize) {
        apply(bat.toList)
        bat.clear()
      }
    }
    if (bat.nonEmpty) {
      apply(bat.toList)
    }
  }

}

class SupervisorReducer(expectedResults: Int, client: ActorRef) extends Actor {
//  var results = IndexedSeq.empty[List[(String, List[String], String, String)]]
  var size = 0
  val uuid = UUID.randomUUID().toString

  def receive = {
    case ReduceResult(lines) =>
      size += 1

      println(lines)

      Fs.write(uuid, lines.map(_.toLine).mkString("\n"))

      if (size == expectedResults) {

        client ! ReduceFinal(uuid)
        context.stop(self)
      }
  }
}

class SupervisorMapper(expectedResults: Int, replyTo: ActorRef, client: ActorRef) extends Actor {
//  var results = scala.collection.mutable.Map[String, List[List[String]]]()
  var uuids = IndexedSeq.empty[String]
//  var all = 0

  def receive = {
    case MapperResult(lines) =>

      val uuid = UUID.randomUUID().toString

      Fs.write(uuid, lines.map(_.toLine).mkString("\n"))

      uuids = uuids :+ uuid

//      all += 1
//      lines foreach {
//        line =>
//          val (key, value) = line
//          results.put(key, value:: results.getOrElse(key, List.empty))
//      }
      if (uuids.size == expectedResults) {

        val uuid = UUID.randomUUID().toString

        val stateIterators = uuids.toList.map(Fs.source).map(IteratorWithParse(_)).map(StateIterator(_))

        mergeSortIterators(stateIterators, uuid)

        uuids.map(Fs.remove)

        println("SupervisordSupervisordSupervisordSupervisord")
        replyTo ! MapperFinal(uuid, client)
        context.stop(self)

      }
  }

  def mergeSortIterators(iterators: List[StateIterator[Mapped]], uuid: String) = {
    // инициализация итераторов
    val initializedIterator = iterators.collect {
      case i if i.hasNext => i.next()
    }

    var currentIterator = initializedIterator

    // рекурсия неудобна для записи
    while (currentIterator.nonEmpty) {
      val (iterators, forReduce) = mergeSort(currentIterator)
      currentIterator = iterators
      val head::tail = forReduce.toList
      val key = head.key
      val values = (head::tail).map(_.value)
      val merged = Merged(key, values)
      Fs.append(uuid, merged.toLine + "\n")
      println(merged.toLine)
    }

    def mergeSort(iterators: List[StateIterator[Mapped]]) = {
      val forReduce = collection.mutable.ArrayBuffer[Mapped]()
      // сортируем итераторы по текущему итему
      val sortedIterator = iterators.sortBy(a => a.item)
      // фильтруем одинаковые значения из отсортированного списка
      val head::tail = sortedIterator
      val similar = head::tail.filter(_.item == head.item)
      // складываем в буфер первые одинаковые значения
      forReduce ++= similar.map(_.item)
      // ищем оставшиеся такие же значения
      val headItem = head.item
      similar.foreach {
        iterator =>
          while (iterator.item == headItem && iterator.hasNext) {
            forReduce += iterator.item
            iterator.next()
          }
      }
      // как результат: только непустые итераторы и данные для редьюса
      (iterators.filter(_.hasNext), forReduce)
    }

  }

  //a = [1,2,3,3]
  //b = [1,2,2,4]
  //c = [1,1,3,4]



//  trait IIterator[A] extends Iterator[A]
//
//  class NonStateIterator[A] extends IIterator[A] {
//    override def hasNext: Boolean = iterator.hasNext
//
//    override def next(): A = {
//      val a = iterator.next()
//      item = a
//      a
//    }
//  }

//  class StateIterator[A](iterator: Iterator[A]) extends IIterator[A] {
//    var item:A = (A)null
////    def self(iterator: Iterator[A]): Unit = {
////      this.item = iterator.next()
////    }
//    override def hasNext: Boolean = iterator.hasNext
//
//    override def next(): A = iterator.next()
//  }


}

//object Line {
//  implicit def orderingByIp: Ordering[Line] = Ordering.by(e => e.ip)
//}



object IteratorWithParse {
  def apply(iterator: Iterator[String]): IteratorWithParse = new IteratorWithParse(iterator)
}
class IteratorWithParse(iterator: Iterator[String]) extends Iterator[Mapped] {
  override def hasNext: Boolean = iterator.hasNext
  override def next(): Mapped = Mapped.fromLine(iterator.next())
}


object StateIterator {
  def apply[A](iterator: Iterator[A]): StateIterator[A] = new StateIterator(iterator)
}
class StateIterator[A](var item:A, iterator: Iterator[A]) extends Iterator[StateIterator[A]] {
  def this(iterator: Iterator[A]) {
    this(null.asInstanceOf[A], iterator)
  }
  override def hasNext: Boolean = iterator.hasNext

  override def next(): StateIterator[A] = {
    val a = iterator.next()
    this.item = a
    this
  }
}
//class SupervisorMapper(expectedResults: Int, replyTo: ActorRef, client: ActorRef) extends Actor {
//  var results = scala.collection.mutable.Map[String, List[List[String]]]()
//  var all = 0
//
//  def receive = {
//    case MapperResult(lines) =>
//
//      all += 1
//      lines foreach {
//        line =>
//          val (key, value) = line
//          results.put(key, value:: results.getOrElse(key, List.empty))
//      }
//      if (all == expectedResults) {
//        println("SupervisordSupervisordSupervisordSupervisord")
//        replyTo ! MapperFinal(results.toMap, client)
//        context.stop(self)
//
//      }
//  }
//
//}

object Fs {
  def write(uuid: String, data: String) = {
    new PrintWriter(uuid) { write(data); close() }
  }
  def append(uuid: String, data: String) = {
    new FileWriter(uuid, true) { write(data); close() }
  }
  def source(uuid: String) = {
    scala.io.Source.fromFile(uuid).getLines()
  }
  def remove(uuid: String) = {
    new File(uuid).delete()
  }
}

//#service

object Test extends App {

  //a = [1,2,3,3]
  val a = List("1","2","3","3").toIterator
  //b = [1,2,2,4]
  val b = List("2","2","2","4").toIterator
  //c = [1,1,3,4]
  val c = List("1","1","3","4").toIterator

  val iterators = List(new StateIterator(a), new StateIterator(b), new StateIterator(c))

  mergeSortIterators(iterators)

  def mergeSortIterators(iterators: List[StateIterator[String]]) = {
    // инициализация итераторов
    val initializedIterator = iterators.collect {
      case i if i.hasNext => i.next()
    }

    var currentIterator = initializedIterator

    // рекурсия неудобна для записи
    while (currentIterator.nonEmpty) {
      val (iterators, forReduce) = mergeSort(currentIterator)
      currentIterator = iterators
      println(forReduce)
    }

    def mergeSort(iterators: List[StateIterator[String]]) = {
      val forReduce = collection.mutable.ArrayBuffer[String]()
      // сортируем итераторы по текущему итему
      val sortedIterator = iterators.sortBy(a => a.item)
      // фильтруем одинаковые значения из отсортированного списка
      val head::tail = sortedIterator
      val similar = head::tail.filter(_.item == head.item)
      // складываем в буфер первые одинаковые значения
      forReduce ++= similar.map(_.item)
      // ищем оставшиеся такие же значения
      val headItem = head.item
      similar.foreach {
        iterator =>
          while (iterator.item == headItem && iterator.hasNext) {
            forReduce += iterator.item
            iterator.next()
          }
      }
      // как результат: только непустые итераторы и данные для редьюса
      (iterators.filter(_.hasNext), forReduce)
    }

  }

}