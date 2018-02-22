package simple.cluster.mapReduce.iterators

object StateIterator {
  def apply[A](iterator: Iterator[A]): StateIterator[A] = new StateIterator(iterator)
}

/**
  * Магический итератор :D
  * для mergeSort было удобно хранить ссылку на последний прочитанный элемент, не теряя ссылку на итератор
  */
class StateIterator[A](var item:A, iterator: Iterator[A]) extends Iterator[StateIterator[A]] {
  def this(iterator: Iterator[A]) {
    this(null.asInstanceOf[A], iterator)
  }
  override def hasNext: Boolean = iterator.hasNext

  override def next(): StateIterator[A] = {
    val a = iterator.next()
    this.item = a
    this
  }
}
