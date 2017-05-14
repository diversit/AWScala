package awscala

import com.amazonaws.auth.{ AWSCredentials, AWSCredentialsProvider, AnonymousAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.findify.s3mock.S3Mock
import s3._
import org.slf4j._
import org.scalatest._

class S3Spec extends FlatSpec with Matchers with BeforeAndAfterAll {

  var s3mock: S3Mock = _

  val builder = AmazonS3ClientBuilder.standard()
    // Note: url must NOT be 'localhost' since then an isValidIp check inside AWS classes fails
    //       and then virtual addressing is used which means the bucketname is prepended to the domainname
    //       which then cannot be resolved anymore to the local S3Mock.
    .withEndpointConfiguration(new EndpointConfiguration("http://127.0.0.1:8001", "us-east-1"))
    .withCredentials(new AWSCredentialsProvider {
      val credentials = new AnonymousAWSCredentials()
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = ()
    })

  override protected def beforeAll(): Unit = {
    s3mock = S3Mock(port = 8001, dir = "/tmp/s3")
    s3mock.start
  }

  override protected def afterAll(): Unit = {
    s3mock.stop
  }

  behavior of "S3"

  val log = LoggerFactory.getLogger(this.getClass)

  it should "handle buckets with > 1000 objects in them " in {

    implicit val s3 = S3.apply(builder)

    // buckets
    val buckets = s3.buckets
    log.info(s"Buckets: ${buckets}")

    val newBucketName = s"awscala-unit-test-${System.currentTimeMillis}"
    val bucket = s3.createBucket(newBucketName)
    log.info(s"Created Bucket: ${bucket}")

    // create/update objects
    val file = new java.io.File("src/main/scala/awscala/s3/S3.scala")
    for (i <- 1 to 1002) {
      bucket.put("S3.scala-" + i, file)
    }

    // delete objects
    val summaries = bucket.objectSummaries.toList

    summaries foreach {
      o => { log.info(s"deleting ${o.getKey}"); s3.client.deleteObject(bucket.name, o.getKey) }
    }
    bucket.destroy()
  }

  it should "provide cool APIs" in {
    implicit val s3 = S3.apply(builder)

    // buckets
    val buckets = s3.buckets
    log.info(s"Buckets: ${buckets}")

    val newBucketName = s"awscala-unit-test-${System.currentTimeMillis}"
    val bucket = s3.createBucket(newBucketName)
    log.info(s"Created Bucket: ${bucket}")

    // create/update objects
    bucket.put("S3.scala", new java.io.File("src/main/scala/awscala/s3/S3.scala"))
    bucket.putAsPublicRead("S3.scala", new java.io.File("src/main/scala/awscala/s3/S3.scala"))
    bucket.put("S3Spec.scala", new java.io.File("src/test/scala/awscala/S3Spec.scala"))

    // get objects
    val s3obj: Option[S3Object] = bucket.get("S3.scala")
    log.info(s"Object: ${s3obj}")
    val summaries = bucket.objectSummaries
    log.info(s"Object Summaries: ${summaries}")

    // delete objects
    s3obj.foreach(o => { o.content.close(); bucket.delete(o) })
    bucket.get("S3Spec.scala").map { o => o.content.close(); o.destroy() } // working with implicit S3 instance

    bucket.destroy()
  }

}
