package simple.cluster.mapReduce

import java.util.UUID

import akka.actor.{Actor, ActorRef}
import akka.routing.Broadcast
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope

/**
  * Ведет учет проделаной работы.
  * back pressure
  * запускается в кластере
  */
class SupervisorReducer(iterator: Iterator[List[String]], reducerRouter: ActorRef, client: ActorRef, forRemoved: String) extends Actor {

  // узнаем, кто живой в кластере
  reducerRouter.tell(Broadcast(YauAlready), self)

  val uuid = UUID.randomUUID().toString
  var inProgress = 0

  def receive = {
    case Continue =>
      // читаем пока есть, что читать
      if (iterator.hasNext) {
        inProgress += 1
        val array = iterator.next()
        reducerRouter ! ConsistentHashableEnvelope(Reduce(array), array)
      } else if (!iterator.hasNext && inProgress == 0) {
        // удаляем то, откуда читали
        Fs.remove(forRemoved)
        // отправляем клиенту ссылку на файл
        client ! ReduceFinal(uuid)
        context.stop(self)

      }
    case ReduceResult(lines) =>
      inProgress -= 1
      // результат скидываем на диск
      Fs.write(uuid, lines.map(_.toLine).mkString("\n"))

  }
}