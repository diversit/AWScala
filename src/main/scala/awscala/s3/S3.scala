package awscala.s3

import java.io.{ ByteArrayInputStream, File, InputStream }

import awscala._
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3ClientBuilder }
import com.amazonaws.services.{ s3 => aws }

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object S3 {

  def apply(credentials: Credentials)(implicit region: Region): S3 = apply(BasicCredentialsProvider(credentials.getAWSAccessKeyId, credentials.getAWSSecretKey))(region)

  def apply(accessKeyId: String, secretAccessKey: String)(implicit region: Region): S3 = apply(BasicCredentialsProvider(accessKeyId, secretAccessKey))(region)

  def apply(credentialsProvider: CredentialsProvider = CredentialsLoader.load())(implicit region: Region = Region.default()): S3 = new S3Client(credentialsProvider).at(region)

  def apply(clientConfiguration: ClientConfiguration, credentials: Credentials)(implicit region: Region): S3 = apply(clientConfiguration, BasicCredentialsProvider(credentials.getAWSAccessKeyId, credentials.getAWSSecretKey))(region)

  def apply(clientConfiguration: ClientConfiguration, accessKeyId: String, secretAccessKey: String)(implicit region: Region): S3 = apply(clientConfiguration, BasicCredentialsProvider(accessKeyId, secretAccessKey))(region)

  def apply(clientConfiguration: ClientConfiguration, credentialsProvider: CredentialsProvider)(implicit region: Region): S3 = new ConfiguredS3Client(clientConfiguration, credentialsProvider).at(region)

  def apply(amazonS3ClientBuilder: AmazonS3ClientBuilder): S3 = new BuildS3Client(amazonS3ClientBuilder)

  def at(region: Region): S3 = apply()(region)
}

/**
 * Amazon S3 Java client wrapper
 * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/]]
 */
trait S3 {

  val client: aws.AmazonS3

  /*
   * This method is a workaround for the fact that the AWS S3 SDK represents the us-east-1 region via either null or "US",
   * but the AWS Core SDK represents the region as "us-east-1". The AWS S3 SDK (com.amazonaws.services.s3.model.Region) is not able
   * to determine the region when it is specified as "us-east-1".
   */
  private def s3RegionHack(regionName: String): String = {
    regionName match {
      case "us-east-1" => "US"
      case _ => regionName
    }
  }

  private[this] var region: aws.model.Region = aws.model.Region.fromValue(s3RegionHack(Region.default.getName))

  def at(region: Region): S3 = {
    client.setRegion(region)
    this.region = aws.model.Region.fromValue(s3RegionHack(region.getName))
    this
  }

  def s3AccountOwner: Owner = Owner(client.getS3AccountOwner)

  // ------------------------------------------
  // Buckets
  // ------------------------------------------

  def buckets: Seq[Bucket] = client.listBuckets.asScala.toSeq.map(b => Bucket(b))

  def bucket(name: String): Option[Bucket] = buckets.find(_.name == name)

  def acl(bucket: Bucket): AccessControlList = bucketAcl(bucket.name)

  def bucketAcl(name: String): AccessControlList = AccessControlList(client.getBucketAcl(name))

  def bucketAcl(bucket: Bucket, acl: AccessControlList) = client.setBucketAcl(bucket.name, acl)

  def location(bucket: Bucket): String = client.getBucketLocation(bucket.name)

  def crossOriginConfig(bucket: Bucket) = BucketCrossOriginConfiguration(bucket, client.getBucketCrossOriginConfiguration(bucket.name))

  def lifecycleConfig(bucket: Bucket) = BucketLifecycleConfiguration(bucket, client.getBucketLifecycleConfiguration(bucket.name))

  def policy(bucket: Bucket) = BucketPolicy(bucket, client.getBucketPolicy(bucket.name))

  def loggingConfig(bucket: Bucket) = BucketLoggingConfiguration(client.getBucketLoggingConfiguration(bucket.name))

  def notificationConfig(bucket: Bucket) = BucketNotificationConfiguration(bucket, client.getBucketNotificationConfiguration(bucket.name))

  def taggingConfig(bucket: Bucket) = BucketTaggingConfiguration(bucket, client.getBucketTaggingConfiguration(bucket.name))

  def versioningConfig(bucket: Bucket) = BucketVersioningConfiguration(bucket, client.getBucketVersioningConfiguration(bucket.name))

  def websiteConfig(bucket: Bucket) = BucketWebsiteConfiguration(bucket, client.getBucketWebsiteConfiguration(bucket.name))

  def createBucket(name: String): Bucket = Bucket(client.createBucket(new aws.model.CreateBucketRequest(name, region)))

  def delete(bucket: Bucket): Unit = deleteBucket(bucket)

  def deleteBucket(bucket: Bucket): Unit = client.deleteBucket(bucket.name)

  def deleteCrossOriginConfig(bucket: Bucket): Unit = client.deleteBucketCrossOriginConfiguration(bucket.name)

  def deleteLifecycleConfig(bucket: Bucket): Unit = client.deleteBucketLifecycleConfiguration(bucket.name)

  def deletePolicy(bucket: Bucket): Unit = client.deleteBucketPolicy(bucket.name)

  def deleteTaggingConfig(bucket: Bucket): Unit = client.deleteBucketTaggingConfiguration(bucket.name)

  def deleteWebsiteConfig(bucket: Bucket): Unit = client.deleteBucketWebsiteConfiguration(bucket.name)

  // ------------------------------------------
  // Objects
  // ------------------------------------------

  // get
  def get(bucket: Bucket, key: String) = getObject(bucket, key)

  def get(bucket: Bucket, key: String, versionId: String) = getObject(bucket, key, versionId)

  def getObject(bucket: Bucket, key: String): Option[S3Object] = try {
    Option(client.getObject(new aws.model.GetObjectRequest(bucket.name, key))).map(obj => S3Object(bucket, obj))
  } catch {
    case e: aws.model.AmazonS3Exception => None
  }

  def getObject(bucket: Bucket, key: String, versionId: String): Option[S3Object] = try {
    Option(client.getObject(new aws.model.GetObjectRequest(bucket.name, key, versionId))).map(obj => S3Object(bucket, obj))
  } catch {
    case e: aws.model.AmazonS3Exception => None
  }

  def metadata(bucket: Bucket, key: String) = client.getObjectMetadata(bucket.name, key)

  // listObjects
  def objectSummaries(bucket: Bucket): Seq[S3ObjectSummary] = objectSummaries(bucket, "")

  def objectSummaries(bucket: Bucket, prefix: String): Stream[S3ObjectSummary] = {
    import com.amazonaws.services.s3.model.ObjectListing

    case class Placeholder(objectSummaries: List[S3ObjectSummary], prefixes: List[String], objectListing: Option[ObjectListing])

    def getSummaries(listing: ObjectListing) = (listing.getObjectSummaries().asScala map { s => S3ObjectSummary(bucket, s) }).toList
    def getPlaceholder(listing: ObjectListing) = Placeholder(getSummaries(listing), getPrefixes(listing), Some(listing))
    def getPrefixes(listing: ObjectListing) = listing.getCommonPrefixes().asScala.toList
    def getListing(prefix: String) = client.listObjects(bucket.name, prefix)

    def stream(work: List[Placeholder]): Stream[S3ObjectSummary] = next(work) match {
      case (Some(e), more) => Stream.cons(e, stream(more))
      case (None, more) => Stream.empty
    }

    @tailrec
    def next(work: List[Placeholder]): (Option[S3ObjectSummary], List[Placeholder]) = {
      work match {
        case Nil => (None, Nil)
        case (head :: tail) => {
          head match {
            case Placeholder(shead :: stail, prefixes, listing) => (Some(shead), Placeholder(stail, prefixes, listing) :: tail)
            case Placeholder(Nil, phead :: ptail, listing) => next(getPlaceholder(getListing(phead)) :: Placeholder(Nil, ptail, listing) :: tail)
            case Placeholder(Nil, Nil, Some(listing)) if listing.isTruncated() => next(getPlaceholder(client.listNextBatchOfObjects(listing)) :: tail)
            case Placeholder(Nil, Nil, _) => next(tail)
          }
        }
      }
    }

    stream(Placeholder(Nil, prefix :: Nil, None) :: Nil)
  }

  def keys(bucket: Bucket): Seq[String] = objectSummaries(bucket).map(os => os.getKey)

  def keys(bucket: Bucket, prefix: String): Seq[String] = objectSummaries(bucket, prefix).map(os => os.getKey)

  // ls
  /**
   *  List the directories and objects under a prefix, use "/" as delimiter.
   *
   *  Here is how to show the directories and objects as Strings:
   *  {{{
   *  ls(bucket, "my-directory/").map {
   *    case Left(directoryPrefix) => directoryPrefix
   *    case Right(s3ObjectSummary) => s3ObjectSummary.getKey
   *  }
   *  }}}
   */
  def ls(bucket: Bucket, prefix: String): Stream[Either[String, S3ObjectSummary]] = {
    import com.amazonaws.services.s3.model.{ ListObjectsRequest, ObjectListing }

    val request = new ListObjectsRequest().withBucketName(bucket.getName).withPrefix(prefix).withDelimiter("/")

    val firstListing = client.listObjects(request)

    def completeStream(listing: ObjectListing): Stream[Either[String, S3ObjectSummary]] = {
      val prefixes = listing.getCommonPrefixes.asScala.map(pre => Left(pre)).toStream
      val objects = listing.getObjectSummaries.asScala.map(s => Right(S3ObjectSummary(bucket, s))).toStream

      prefixes #::: objects #::: (if (listing.isTruncated) completeStream(client.listNextBatchOfObjects(listing)) else Stream.empty)
    }

    completeStream(firstListing)
  }

  // acl
  def acl(obj: S3Object): AccessControlList = acl(obj.bucket, obj.key)

  def acl(bucket: Bucket, key: String): AccessControlList = AccessControlList(client.getObjectAcl(bucket.name, key))

  def acl(obj: S3Object, acl: AccessControlList): Unit = client.setObjectAcl(obj.bucket.name, obj.key, acl)

  def acl(obj: S3Object, acl: CannedAccessControlList): Unit = client.setObjectAcl(obj.bucket.name, obj.key, acl)

  def acl(bucket: Bucket, key: String, acl: AccessControlList): Unit = client.setObjectAcl(bucket.name, key, acl)

  def acl(bucket: Bucket, key: String, acl: CannedAccessControlList): Unit = client.setObjectAcl(bucket.name, key, acl)

  // put
  def put(bucket: Bucket, key: String, file: File): PutObjectResult = putObject(bucket, key, file)

  def putAsPublicRead(bucket: Bucket, key: String, file: File): PutObjectResult = putObjectAsPublicRead(bucket, key, file)

  def putAsPublicReadWrite(bucket: Bucket, key: String, file: File): PutObjectResult = putObjectAsPublicReadWrite(bucket, key, file)

  def putObject(bucket: Bucket, key: String, file: File): PutObjectResult = PutObjectResult(bucket, key, client.putObject(bucket.name, key, file))

  def putObjectAsPublicRead(bucket: Bucket, key: String, file: File): PutObjectResult = {
    PutObjectResult(bucket, key, client.putObject(
      new aws.model.PutObjectRequest(bucket.name, key, file).withCannedAcl(aws.model.CannedAccessControlList.PublicRead)
    ))
  }

  def putObjectAsPublicReadWrite(bucket: Bucket, key: String, file: File): PutObjectResult = {
    PutObjectResult(bucket, key, client.putObject(
      new aws.model.PutObjectRequest(bucket.name, key, file).withCannedAcl(aws.model.CannedAccessControlList.PublicReadWrite)
    ))
  }

  // putting a byte array
  def put(bucket: Bucket, key: String, bytes: Array[Byte], metadata: aws.model.ObjectMetadata): PutObjectResult = putObject(bucket, key, bytes, metadata)

  def putAsPublicRead(bucket: Bucket, key: String, bytes: Array[Byte], metadata: aws.model.ObjectMetadata): PutObjectResult = putObjectAsPublicRead(bucket, key, bytes, metadata)

  def putObject(bucket: Bucket, key: String, bytes: Array[Byte], metadata: aws.model.ObjectMetadata): PutObjectResult =
    putObject(bucket, key, new ByteArrayInputStream(bytes), metadata)

  def putObject(bucket: Bucket, key: String, inputStream: InputStream, metadata: aws.model.ObjectMetadata): PutObjectResult =
    PutObjectResult(bucket, key, client.putObject(
      new aws.model.PutObjectRequest(bucket.name, key, inputStream, metadata)
    ))

  def putObjectAsPublicRead(bucket: Bucket, key: String, bytes: Array[Byte], metadata: aws.model.ObjectMetadata): PutObjectResult = {
    PutObjectResult(bucket, key, client.putObject(
      new aws.model.PutObjectRequest(bucket.name, key,
        new ByteArrayInputStream(bytes),
        metadata).withCannedAcl(aws.model.CannedAccessControlList.PublicRead)
    ))
  }

  // copy
  def copy(from: S3Object, to: S3Object): PutObjectResult = copyObject(from, to)

  def copyObject(from: S3Object, to: S3Object): PutObjectResult = {
    val result = client.copyObject(from.bucket.name, from.key, to.bucket.name, to.key)
    PutObjectResult(to.bucket, to.key, result)
  }

  // delete
  def delete(obj: S3Object): Unit = deleteObject(obj)

  def deleteObject(obj: S3Object): Unit = client.deleteObject(obj.bucket.name, obj.key)

  def deleteVersion(obj: S3Object, versionId: String): Unit = deleteObjectVersion(obj, versionId)

  def deleteObjectVersion(obj: S3Object, versionId: String): Unit = {
    client.deleteVersion(new aws.model.DeleteVersionRequest(obj.bucket.name, obj.key, versionId))
  }

  def deleteObjects(objs: Seq[S3Object]): Unit = {
    objs
      .groupBy(_.bucket)
      .foreach {
        case (bucket, s3objects) =>

          // Batch deletion is limited to 1000 elements per job
          val keyVersions = s3objects.map(obj => new aws.model.DeleteObjectsRequest.KeyVersion(obj.key, obj.versionId))

          keyVersions.grouped(1000).foreach { kvs =>

            val req = new aws.model.DeleteObjectsRequest(bucket.name)
            req.setKeys(kvs.asJava)
            client.deleteObjects(req)
          }

      }
  }

  // presignedUrl
  def generatePresignedUrl(obj: S3Object, expiration: DateTime): java.net.URL = {
    client.generatePresignedUrl(obj.bucket.name, obj.key, expiration.toDate)
  }

}

/**
 * Default Implementation
 *
 * @param credentialsProvider credentialsProvider
 */
class S3Client(credentialsProvider: CredentialsProvider = CredentialsLoader.load()) extends S3 {

  override val client: AmazonS3 = aws.AmazonS3ClientBuilder.standard()
    .withCredentials(credentialsProvider)
    .build()

  //  override def createBucket(name: String): Bucket = super.createBucket(name)
}

/**
 * Configured Implementation
 *
 * @param clientConfiguration ClientConfiguration
 * @param credentialsProvider CredentialsProvider
 */
class ConfiguredS3Client(clientConfiguration: ClientConfiguration, credentialsProvider: CredentialsProvider = CredentialsLoader.load())
    extends S3 {

  override val client: AmazonS3 = aws.AmazonS3ClientBuilder.standard()
    .withCredentials(credentialsProvider)
    .withClientConfiguration(clientConfiguration)
    .build()

  //  override def createBucket(name: String): Bucket = super.createBucket(name)
}

class BuildS3Client(builder: AmazonS3ClientBuilder) extends S3 {
  override val client: AmazonS3 = builder.build()
}
