## Кластерная обработка файла

Конфигурация приложения разбита на две части

[application.conf](src/main/resources/application.conf)

[actors.conf](src/main/resources/actors.conf)

В дефолтном состоянии кластер запускается командой `sbt "runMain simple.cluster.mapReduce.Starter"`

При этом поднимается три сиид нобы, которые образуют кластер в пределах одной JVM.

Или можно запустить каждую ноду, как отдельный процесс

`sbt "runMain simple.cluster.mapReduce.Starter 2551"`

`sbt "runMain simple.cluster.mapReduce.Starter 2552"`

`sbt "runMain simple.cluster.mapReduce.Starter 0"`

и запустить клиента

`sbt "runMain simple.cluster.mapReduce.Client"`

[Starter](src/main/scala/simple/cluster/mapReduce/Starter.scala).
[Starter#Client](src/main/scala/simple/cluster/mapReduce/Starter.scala).

## Описание работы кластера

Каждая нода запускает [Master](src/main/scala/simple/cluster/mapReduce/Master.scala) процесс, [MapperWorker](src/main/scala/simple/cluster/mapReduce/MapperWorker.scala) и [ReducerWorker](src/main/scala/simple/cluster/mapReduce/ReducerWorker.scala)(в кластере)

[Master](src/main/scala/simple/cluster/mapReduce/Master.scala) получает от клиента сообщение с именем файла и фильтрами по дате

В ответ на это сообщение [Master](src/main/scala/simple/cluster/mapReduce/Master.scala) создает [SupervisorMapper](src/main/scala/simple/cluster/mapReduce/SupervisorMapper.scala)(наблюдателя реализующиго back pressure) который отвечает за корректную работу [MapperWorker](src/main/scala/simple/cluster/mapReduce/MapperWorker.scala) и ограничивает расход памяти

[MapperWorker](src/main/scala/simple/cluster/mapReduce/MapperWorker.scala) запускаются на одной ноде с текущим [Master](src/main/scala/simple/cluster/mapReduce/Master.scala). Это снижает затраты на трансфер ещё неотфильтрованных данных

Результатом работы [MapperWorker](src/main/scala/simple/cluster/mapReduce/MapperWorker.scala) будет отсортированный по ip список строк, который [SupervisorMapper](src/main/scala/simple/cluster/mapReduce/SupervisorMapper.scala) запишет в файловую систему и сохранит ссылку неё.

По исчерпанию файла [SupervisorMapper](src/main/scala/simple/cluster/mapReduce/SupervisorMapper.scala) запустит mergeSort. Имея все ссылки от предыдущей работы мы создаем итераторы на эти файлы.

Т.к. итератор не выгружает весь файл целиком - у нас нету перерасхода по памяти.

mergeSort построчно записывает результат своей работы в файл. Указатель на файл передает на вход [SupervisorReducer](src/main/scala/simple/cluster/mapReduce/SupervisorReducer.scala)(наблюдателя реализующиго back pressure) и соответственно [ReducerWorker](src/main/scala/simple/cluster/mapReduce/ReducerWorker.scala) (запускается в кластере)

Который в свою очередь запускает вторую часть фильтра. Нужно оставить только ip с множественным входом. И производит агрегацию строк.

Результат каждого [ReducerWorker](src/main/scala/simple/cluster/mapReduce/ReducerWorker.scala) дописывается в один файл. Файл и есть конец задачи.

