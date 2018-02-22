package simple.cluster.mapReduce.iterators

import simple.cluster.mapReduce.Mapped

object IteratorWithParse {
  def apply(iterator: Iterator[String]): IteratorWithParse = new IteratorWithParse(iterator)
}

/**
  * удобно итерироваться по строкам, а на выходе получать десереализованный объект
  */
class IteratorWithParse(iterator: Iterator[String]) extends Iterator[Mapped] {
  override def hasNext: Boolean = iterator.hasNext
  override def next(): Mapped = Mapped.fromLine(iterator.next())
}
