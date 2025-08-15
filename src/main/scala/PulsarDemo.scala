

import java.util.UUID
import scala.util.{Failure, Random, Success}
import com.sksamuel.pulsar4s.{ConsumerConfig, DefaultProducerMessage, EventTime, Producer, ProducerConfig, PulsarClient, Subscription, Topic}
import io.circe.generic.auto._
import com.sksamuel.pulsar4s.circe._
import org.apache.pulsar.client.api.{SubscriptionInitialPosition, SubscriptionType}

import java.util.concurrent.Executors
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.concurrent.ExecutionContext.Implicits.global

object SensorDomain {
  // ADTs
  sealed trait EventStatus

  case object Starting extends EventStatus

  case object Stopped extends EventStatus

  case object Running extends EventStatus

  case class SensorEvent(
                          sensorId: String,
                          status: EventStatus,
                          startupTime: Long,
                          eventTime: Long,
                          reading: Double
                        )

  // Sensor metadata
  private val startupTime = System.currentTimeMillis()
  private val sensorIds: List[String] = (0 to 10).map(_ => UUID.randomUUID().toString).toList

  /** Generates an infinite lazy stream of SensorEvents */
  def generate(
                ids: List[String] = sensorIds,
                off: Set[String] = sensorIds.toSet,
                on: Set[String] = Set.empty,
                rnd: Random = new Random()
              ): Iterator[SensorEvent] = {

    // Pick a random sensor
    val index = rnd.nextInt(ids.size)
    val sensorId = ids(index)

    // Generate event based on current sensor status
    val event = if (off.contains(sensorId)) {
      println(s"Staring the Sensor And Changing EventStatus: $index")
      SensorEvent(sensorId, Starting, startupTime, System.currentTimeMillis(), 0.0)
    } else {
      val temp = BigDecimal(40 + rnd.nextGaussian())
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
      SensorEvent(sensorId, Running, startupTime, System.currentTimeMillis(), temp)
    }

    // Recursively build the infinite lazy stream
    Iterator.single(event) ++ generate(ids, off - sensorId, on + sensorId, rnd)
  }

}

object Producer {

  import SensorDomain._

  private val executor = Executors.newFixedThreadPool(4)
  implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(executor)
  private val pulsarClient = PulsarClient("pulsar://localhost:6650")
  private val topic = Topic("sensor-events")
  // create the producer
  private val eventProducer: Producer[SensorEvent] =
    pulsarClient
      .producer[SensorEvent](
        ProducerConfig(
          topic,
          producerName = Some("sensor-producer"),
          enableBatching = Some(true),
          blockIfQueueFull = Some(true)
        )
      )

  def main(args: Array[String]): Unit = {
    // sent 100 messages
    SensorDomain.generate().take(100).foreach {
      sensorEvent =>
        val message =
          DefaultProducerMessage(
            Some(sensorEvent.sensorId), //key of message
            sensorEvent, //payload of message i.e actual Event
            eventTime = Some(EventTime(sensorEvent.eventTime))) // Event-time from source systems

        eventProducer.sendAsync(message) // use the async method to sent the message to Pulsar
    }
  }
}

object PulsarConsumer {

  import SensorDomain._

  private val pulsarClient = PulsarClient("pulsar://localhost:6650")
  private val topic = Topic("sensor-events")
  private val consumerConfig =
    ConsumerConfig(
      Subscription("sensor-event-subscription"),
      Seq(Topic("sensor-events")),
      consumerName = Some("sensor-event-consumer"),
      subscriptionInitialPosition = Some(SubscriptionInitialPosition.Earliest),
      subscriptionType = Some(SubscriptionType.Exclusive)
    )
  //bootstrapping Consumer with Config
  private val consumerFn = pulsarClient.consumer[SensorEvent](consumerConfig)

  @tailrec
  private def receiveAll(totalMessageCount: Int = 0): Unit = {
    //its blocking algo it will block the calling thread
    consumerFn.receive match {
      case Success(message) => println(s"Total messages Count : -> $totalMessageCount - Acked Message ${message.messageId} :- and Received event is ${message.value} ")
        consumerFn.acknowledge(message.messageId) // sensorid is message id here
        receiveAll(totalMessageCount + 1)
      case Failure(ex) => println(s"Failed to receive Message ${ex.getMessage}  ")
    }
  }

  def main(args: Array[String]): Unit = {
    receiveAll()
  }
}

object PulsarDemo {

  import SensorDomain._

  def main(args: Array[String]): Unit = {
    //  generate().take(100).foreach(println)

  }
}
