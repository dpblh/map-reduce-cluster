package simple.cluster.mapReduce

import java.text.SimpleDateFormat

import akka.actor.Actor

//#worker
class ReducerWorker extends Actor {

  val formater = new SimpleDateFormat("dd.MM.yyyy")

  def receive = {
    case Reduce(lines) =>
      sender() ! ReduceResult(
        lines
          .map(Merged.fromLine)
          .filter(_.values.size > 1)
          .map {
            item =>
              Reduced(
                item.key,
                item.values.map(_._1),
                item.values.map(_._2).min,
                item.values.map(_._2).max
              )
          }
      )
  }

}

//#worker