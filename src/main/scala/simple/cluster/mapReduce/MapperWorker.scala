package simple.cluster.mapReduce

import java.text.SimpleDateFormat

import akka.actor.{Actor, ActorRef}

//#worker
class MapperWorker extends Actor {

//  context.parent ! Ready

  val formater = new SimpleDateFormat("dd.MM.yyyy")

  def receive = {
    case Mapper(lines, start, end) =>
      println("111111111----")
      sender() ! MapperResult(lines
        .map { Mapped.fromLine }
        .filter {
          tp =>
            val timestamp = formater.parse(tp.value._2)
            timestamp.getTime >= start.getTime && timestamp.getTime <= end.getTime
        }
        .sortBy(_.key)
      )
  }
}

//#worker