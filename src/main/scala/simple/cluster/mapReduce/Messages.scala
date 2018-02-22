package simple.cluster.mapReduce

import akka.actor.ActorRef

final case class CountDocument(text: String)
final case class StatsResult(meanWordLength: List[(String, List[String], String, String)])

case class ReadFromFile(filePath: String, start: java.util.Date, end: java.util.Date)

case class Mapper(lines: List[String], start: java.util.Date, end: java.util.Date)
case class MapperResult(values: List[Mapped])
case class MapperFinal(uuid: String, client: ActorRef)
object Mapped {
  def fromLine(line: String): Mapped = {
    val ip :: user :: time :: Nil = line.split(";").toList
    new Mapped(ip, (user, time))
  }
}
class Mapped(val key: String, val value: (String, String)) extends Ordered[Mapped] {
  override def equals(obj: Any): Boolean = obj match {
    case that: Mapped => this.key == that.key
    case _ => false
  }
  override def compare(that: Mapped): Int = this.key compare that.key
  def toLine: String = s"$key;${value._1};${value._2}"
}

object Merged {
  def apply(key: String, values: List[(String, String)]): Merged = new Merged(key, values)
  def fromLine(line: String): Merged = {
    val key :: values = line.split("\\|").toList
    Merged(key, values.map {
      value =>
        val user :: time :: Nil = value.split(";").toList
        (user, time)
    })
  }
}
class Merged(val key: String, val values: List[(String, String)]) {
  def toLine: String = s"$key|${values.map(value => s"${value._1};${value._2}").mkString("|")}"
}

case class Reduce(lines: List[String])
case class ReduceResult(lines: List[Reduced])
case class ReduceFinal(uuid: String)
case class Reduced(key: String, users: List[String], start: String, end: String) {
  def toLine: String = s"$key;${users.mkString("|")};$start;$end"
}


case class Result(values: List[(String, List[String])])


case object Start
case object MapDone