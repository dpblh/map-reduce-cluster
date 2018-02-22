package simple.cluster.mapReduce

import akka.actor.{Actor, Props}
import akka.routing.FromConfig
import simple.cluster.mapReduce.iterators.ChunkIterator

/**
  * Мастер процесс.
  */
class Master extends Actor {

  val batchSize = 1000

  val reducerRouter = context.actorOf(
    FromConfig.props(Props[ReducerWorker]),
    name = "workerRouter"
  )

  def receive = {
    case ReadFromFile(filePath, start, end) =>
      // по-хорошему, для этой задачи должен хорошо подходить akka stream
      // если бы на практике умел рабоать с кластером
      val client = sender()

      // для удобства итерируемся не по строкам, а по чанкам
      val iterator = new ChunkIterator(scala.io.Source.fromFile(filePath).getLines, batchSize)

      // создаем и запускаем Mapper
      // маппер запускается в пределах одного кластера
      // затраты на тренсфер данных по кластеру слишком дорогие
      context.actorOf(Props(
        classOf[SupervisorMapper], iterator, start, end, self, client)
      )

      // ответ от маппера
      // все данные обработаны и отфильтрованы в первом приближении
    case MapperFinal(uuid, client) =>

      val iterator = new ChunkIterator(scala.io.Source.fromFile(uuid).getLines, batchSize)

      // создание и запуск редьюсера
      // редьюсер запускается в кластере, но данные возвращает на мастер ноду. Не хотелось изобретать распределенную файловую систему :D
      context.actorOf(Props(
        classOf[SupervisorReducer], iterator, reducerRouter, client, uuid)
      )

  }

}

