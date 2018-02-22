package simple.cluster.mapReduce

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import simple.cluster.mapReduce.iterators.{IteratorWithParse, StateIterator}

/**
  * Ведет учет проделаной работы.
  * back pressure
  */
class SupervisorMapper
(
  iterator: Iterator[List[String]],
  start: java.util.Date,
  end: java.util.Date,
  replyTo: ActorRef,
  client: ActorRef
) extends Actor {

  // Указатели на место в файловой системе
  // Результат работы каждого маппера будет записан в файловую систему
  var uuids = IndexedSeq.empty[String]

  // количество воркеров в работе
  var inProgress = 0

  val mapper = context.actorOf(
    Props(classOf[MapperWorker], self).withMailbox("bounded-mailbox"),
    name = "mapper"
  )

  def receive = {
    case Continue =>
      // читаем, пока не закончится файл
      if (iterator.hasNext) {
        inProgress += 1
        // раскидываем мапперам
        mapper ! Mapper(iterator.next(), start, end)
      } else if (!iterator.hasNext && inProgress == 0) {
        // если файл закончился и форкеры все вернулись
        // пора делать сортировку и слияние нескольких файлов в один
        val uuid = UUID.randomUUID().toString

        // достаем все итераторы и применяем к ним надстройки
        val stateIterators = uuids.toList.map(Fs.source).map(IteratorWithParse(_)).map(StateIterator(_))

        // та самая сортировка слиянием
        // на вход приходят ирераторы, значения которых предварительно мапперами были отсортированы
        mergeSortIterators(stateIterators, uuid)

        // чистим файловую систему
        uuids.map(Fs.remove)

        // указатель на место в памяти передаем редьюсеру
        replyTo ! MapperFinal(uuid, client)
        context.stop(self)

      }
      // данные с мапперов стекаются на мастер ноду
      // без этого пришлось бы работать с распределенной файловой системой (чего придумывать не хочеться :D)
      // да и сортировка в кластере немного трудозатратнее
    case MapperResult(lines) =>
      inProgress -= 1
      val uuid = UUID.randomUUID().toString

      // результат скидываем на диск
      Fs.write(uuid, lines.map(_.toLine).mkString("\n"))

      uuids = uuids :+ uuid

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

}
