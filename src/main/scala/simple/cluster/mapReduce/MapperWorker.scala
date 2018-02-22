package simple.cluster.mapReduce

import akka.actor.{Actor, ActorRef}

/**
  * Маппер. всегда запускается на той же ноде, что и мастер
  * после старта регистрирует себя на наблюдателе
  * реализует часть функционала back pressure
  * производит начальную фильтрацию по времени
  */
class MapperWorker(supervisor: ActorRef) extends Actor {

  override def preStart(): Unit = {
    super.preStart()
    supervisor ! Continue
  }

  def receive = {
    case Mapper(lines, start, end) =>
      supervisor ! MapperResult(lines
        .map { Mapped.fromLine }
        .filter {
          tp =>
            val timestamp = tp.value._2
            timestamp >= start.getTime && timestamp <= end.getTime
        }
        .sortBy(_.key)
      )
      // сигнализирует что готов читать дальше
      supervisor ! Continue
  }
}
