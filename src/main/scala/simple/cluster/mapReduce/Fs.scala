package simple.cluster.mapReduce

import java.io.{File, FileWriter, PrintWriter}

/**
  * Наша файловая система ;)
  * По-хорошему, не нужно сразу скидывать на диск, можно какое-то время держать в кеше
  * пока не накопится приличное колличество данных. А с другой стороны, не угадаешь, когда память закончится
  */
object Fs {
  /**
    * просто пишем целиком
    */
  def write(uuid: String, data: String) = {
    new PrintWriter(uuid) { write(data); close() }
  }

  /**
    * с возможностью добавления
    */
  def append(uuid: String, data: String) = {
    new FileWriter(uuid, true) { write(data); close() }
  }

  /**
    * получение итератора по ссылке на файл
    */
  def source(uuid: String) = {
    scala.io.Source.fromFile(uuid).getLines()
  }

  /**
    * ну и не забыть подчистить за собой
    */
  def remove(uuid: String) = {
    new File(uuid).delete()
  }
}
