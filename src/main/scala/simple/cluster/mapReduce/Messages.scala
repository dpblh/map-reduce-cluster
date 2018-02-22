package simple.cluster.mapReduce

import java.text.SimpleDateFormat

import akka.actor.ActorRef

case class ReadFromFile(filePath: String, start: java.util.Date, end: java.util.Date)

case class Mapper(lines: List[String], start: java.util.Date, end: java.util.Date)
case class MapperResult(values: List[Mapped])
case class MapperFinal(uuid: String, client: ActorRef)
object Mapped {
  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def fromLine(line: String): Mapped = {
    val user :: ip :: time :: Nil = line.split(",").map(_.replaceAll("\"", "")).toList
    new Mapped(ip, (user, formatter.parse(time).getTime))
  }
}
class Mapped(val key: String, val value: (String, Long)) extends Ordered[Mapped] {
  override def equals(obj: Any): Boolean = obj match {
    case that: Mapped => this.key == that.key
    case _ => false
  }
  override def compare(that: Mapped): Int = this.key compare that.key
  def toLine: String = s"${value._1},$key,${Mapped.formatter.format(value._2)}"
}

object Merged {
  def apply(key: String, values: List[(String, Long)]): Merged = new Merged(key, values)
  def fromLine(line: String): Merged = {
    val key :: values = line.split("\\|").toList
    Merged(key, values.map {
      value =>
        val user :: time :: Nil = value.split(";").toList
        (user, time.toLong)
    })
  }
}
class Merged(val key: String, val values: List[(String, Long)]) {
  def toLine: String = s"$key|${values.map(value => s"${value._1};${value._2}").mkString("|")}"
}

case class Reduce(lines: List[String])
case class ReduceResult(lines: List[Reduced])
case class ReduceFinal(uuid: String)
object Reduced {
  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
}
case class Reduced(key: String, users: List[(String, Long)], start: Long, end: Long) {
  def toLine: String = s""""$key","${users.map(t => s"${t._1}:${Reduced.formatter.format(new java.util.Date(t._2))}").mkString(";")}","${Reduced.formatter.format(new java.util.Date(start))}","${Reduced.formatter.format(new java.util.Date(end))}""""
}

case object Continue
case object YauAlready