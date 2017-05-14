package awscala

import awscala._
import com.amazonaws.auth.{ AWSCredentials, AWSCredentialsProvider, AnonymousAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import io.findify.sqsmock.SQSService
import sqs._
import org.slf4j._
import org.scalatest._

class SQSSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  var sqsMock: SQSService = _

  val builder = AmazonSQSClientBuilder.standard()
    // Note: url must NOT be 'localhost' since then an isValidIp check inside AWS classes fails
    //       and then virtual addressing is used which means the bucketname is prepended to the domainname
    //       which then cannot be resolved anymore to the local SQSService.
    .withEndpointConfiguration(new EndpointConfiguration("http://127.0.0.1:8002", "us-east-1"))
    .withCredentials(new AWSCredentialsProvider {
      val credentials = new AnonymousAWSCredentials()
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = ()
    })

  override protected def beforeAll(): Unit = {
    sqsMock = new SQSService(port = 8002, account = 1)
    sqsMock.start
  }

  override protected def afterAll(): Unit = {
    sqsMock.stop
  }

  behavior of "SQS"

  val log = LoggerFactory.getLogger(this.getClass)

  it should "provide cool APIs" in {
    implicit val sqs = SQS(builder)

    // queues
    val queues = sqs.queues
    log.info(s"Queues : ${queues}")

    // create new queue
    val newQueueName = s"sample-queue-${System.currentTimeMillis}"
    val queue = sqs.createQueueAndReturnQueueName(newQueueName)
    val url = sqs.queueUrl(newQueueName)
    log.info(s"Created queue: ${queue}, url: ${url}")

    // get queue attributes before inserting any message
    val attribute = sqs.queueAttributes(queue, "ApproximateNumberOfMessages")
    log.info(s"Attribute for queue before inserting any message")
    attribute.keys.foreach { i =>
      log.info(s"Attribute Name = ${i}")
      log.info(s"Value = ${attribute}(${i})")
    }

    // send messages
    val sent = queue.add("some message!")
    log.info(s"Sent : ${sent}")
    val sendMessages = queue.add("first", "second", "third")
    log.info(s"Batch Sent : ${sendMessages}")

    // get queue attributes after inserting any message
    val attribute2 = sqs.queueAttributes(queue, "ApproximateNumberOfMessages")
    log.info(s"Attribute for queue after inserting any message")
    attribute2.keys.foreach { i =>
      log.info(s"Attribute Name = ${i}")
      log.info(s"Value = ${attribute2}(${i})")
    }

    // receive messages
    val receivedMessages = queue.messages // or sqs.receiveMessage(queue)
    log.info(s"Received : ${receivedMessages}")

    // delete messages
    queue.removeAll(receivedMessages)

    // working with specified queue
    sqs.withQueue(queue) { s =>
      s.sendMessage("some message!")
      s.sendMessages("first", "second", "third")
      s.receiveMessage.foreach(msg => s.deleteMessage(msg))
    }

    // delete a queue
    queue.destroy() // or sqs.deleteQueue(queue)
    log.info(s"Deleted queue: ${queue}")

  }

}
