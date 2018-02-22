package simple.cluster.mapReduce.iterators

/**
  * просто так читать удобнее
  */
class ChunkIterator[A](iterator: Iterator[A], chunkSize: Int) extends Iterator[List[A]] {
  override def hasNext: Boolean = iterator.hasNext

  override def next(): List[A] = {
    (0 to chunkSize).collect {
      case _ if hasNext => iterator.next()
    }.toList
  }
}
