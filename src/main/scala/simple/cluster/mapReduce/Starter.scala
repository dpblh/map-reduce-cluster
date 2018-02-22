package simple.cluster.mapReduce

import java.util.concurrent.ThreadLocalRandom

import akka.actor.{Actor, ActorSystem, Address, Props, RelativeActorPath, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, MemberStatus}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object Starter {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      // если нет аргументов стартуем кластер подефолту
      startup(Seq("2551", "2552", "0"))
      // запускаем потребителя кластера
      Client.main(Array.empty)
    } else {
      startup(args)
    }
  }

  // запуск члена кластера
  def startup(ports: Seq[String]): Unit = {
    ports foreach { port =>
      val config = ConfigFactory.parseString(s"""
        akka.remote.netty.tcp.port=$port
        akka.remote.artery.canonical.port=$port
        """
      ).withFallback(ConfigFactory.parseString("akka.cluster.roles = [compute]")
      ).withFallback(ConfigFactory.load("actors"))

      val system = ActorSystem("ClusterSystem", config)

      // master mapper reducer запускаются на каждой ноде
      system.actorOf(Props[MapperWorker], name = "mapperWorker")
      system.actorOf(Props[ReducerWorker], name = "reducerWorker")
      system.actorOf(Props[Master], name = "master")
    }
  }
}

// потребитель услуг от нашего кластера.
// просто запускает собвытие на обработку и ждет завершения
object Client {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("ClusterSystem")
    system.actorOf(Props(classOf[Client], "/user/master"), "client")
  }
}

class Client(servicePath: String) extends Actor {

  val cluster = Cluster(context.system)
  val servicePathElements = servicePath match {
    case RelativeActorPath(elements) => elements
    case _ => throw new IllegalArgumentException(
      "servicePath [%s] is not a valid relative actor path" format servicePath)
  }
  import context.dispatcher

  // швыряем событие условно после инициализации кластера
  val tickTask = context.system.scheduler.schedule(5.seconds, 5.seconds, self, "tick")

  var nodes = Set.empty[Address]

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent])
  }
  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def receive = {
    case "tick" if nodes.nonEmpty =>
      // очень грубо считаем что кластер инициализировался и запускаем задачу в кластер
      // убиваем шедулер
      tickTask.cancel()

      // выбираем зарегистрированную ноду
      val address = nodes.toIndexedSeq(ThreadLocalRandom.current.nextInt(nodes.size))
      val service = context.actorSelection(RootActorPath(address) / servicePathElements)

      //todo не забыть принимать из командной строки
      val start = java.util.Calendar.getInstance()
      start.set(java.util.Calendar.YEAR, 1970)

      val end = java.util.Calendar.getInstance()
      end.set(java.util.Calendar.YEAR, 2020)


      //todo и наименование файла
      service ! ReadFromFile("./logins.csv", start.getTime, end.getTime)
    case ReduceFinal(result) =>
      println(
        "Cluster result",
        result
      )
      // останавливаем клиент
      context.stop(self)
//      System.exit(-1)
//      context.system.terminate()

    case state: CurrentClusterState =>
      nodes = state.members.collect {
        case m if m.hasRole("compute") && m.status == MemberStatus.Up => m.address
      }
    case MemberUp(m) if m.hasRole("compute")        => nodes += m.address
    case other: MemberEvent                         => nodes -= other.member.address
    case UnreachableMember(m)                       => nodes -= m.address
    case ReachableMember(m) if m.hasRole("compute") => nodes += m.address
    case unknown => println(unknown)
  }

}
