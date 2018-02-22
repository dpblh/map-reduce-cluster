package simple.cluster.mapReduce

import akka.actor.Actor

/**
  * Редьюсер.
  * Вторая часть фильтра. Оставляем только записи со множественным входом
  * и приводим к финальному виду
  */
class ReducerWorker extends Actor {

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
                item.values,
                item.values.map(_._2).min,
                item.values.map(_._2).max
              )
          }
      )
      sender() ! Continue
    case YauAlready =>
      // отвесь, если жив
      sender() ! Continue
  }

}
