package awscala.dynamodbv2

import awscala._

import scala.collection.JavaConverters._
import com.amazonaws.ClientConfiguration
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.{ dynamodbv2 => aws }
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes

object DynamoDB {

  def apply(credentials: Credentials)(implicit region: Region): DynamoDB = apply(BasicCredentialsProvider(credentials.getAWSAccessKeyId, credentials.getAWSSecretKey))(region)
  def apply(accessKeyId: String, secretAccessKey: String)(implicit region: Region): DynamoDB = apply(BasicCredentialsProvider(accessKeyId, secretAccessKey))(region)
  def apply(credentialsProvider: CredentialsProvider = CredentialsLoader.load())(implicit region: Region = Region.default()): DynamoDB = {
    new DynamoDBClient(credentialsProvider, region)
  }

  def apply(clientConfiguration: ClientConfiguration, credentials: Credentials)(implicit region: Region): DynamoDB = apply(clientConfiguration, BasicCredentialsProvider(credentials.getAWSAccessKeyId, credentials.getAWSSecretKey))(region)
  def apply(clientConfiguration: ClientConfiguration, accessKeyId: String, secretAccessKey: String)(implicit region: Region): DynamoDB = apply(clientConfiguration, BasicCredentialsProvider(accessKeyId, secretAccessKey))(region)
  def apply(clientConfiguration: ClientConfiguration, credentialsProvider: CredentialsProvider)(implicit region: Region): DynamoDB = new ConfiguredDynamoDBClient(clientConfiguration, credentialsProvider, region)

  def apply(amazonDynamoDBClientBuilder: AmazonDynamoDBClientBuilder): DynamoDB = new BuildDynamoClient(amazonDynamoDBClientBuilder)

  def at(region: Region): DynamoDB = apply()(region)

  def local(): DynamoDB = {
    val builder = AmazonDynamoDBClientBuilder.standard()
      .withRegion(Region.default().getName)
      .withEndpointConfiguration(
        new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")
      )

    DynamoDB(builder)
  }
}

/**
 * Amazon DynamoDB Java client wrapper
 * @see [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/]]
 */
trait DynamoDB {

  val client: aws.AmazonDynamoDB

  private[this] var consistentRead = false

  def consistentRead(consistentRead: Boolean): DynamoDB = {
    this.consistentRead = consistentRead
    this
  }

  // ------------------------------------------
  // Tables
  // ------------------------------------------

  def tableNames: Seq[String] = client.listTables.getTableNames.asScala
  def lastEvaluatedTableName: Option[String] = Option(client.listTables.getLastEvaluatedTableName)

  def describe(table: Table): Option[TableMeta] = describe(table.name)
  def describe(tableName: String): Option[TableMeta] = try {
    Option(TableMeta(client.describeTable(new aws.model.DescribeTableRequest().withTableName(tableName)).getTable))
  } catch { case e: aws.model.ResourceNotFoundException => None }

  /**
   * Gets the table by name if it exists.
   * This is an expensive operation since it queries the table schema each time it is called.
   * @see [[http://docs.aws.amazon.com/cli/latest/reference/dynamodb/describe-table.html]]
   */
  def table(name: String): Option[Table] = describe(name).map(_.table)

  def createTable(
    name: String,
    hashPK: (String, aws.model.ScalarAttributeType)
  ): TableMeta = {
    create(Table(
      name = name,
      hashPK = hashPK._1,
      rangePK = None,
      attributes = Seq(AttributeDefinition(hashPK._1, hashPK._2))
    ))
  }

  def createTable(
    name: String,
    hashPK: (String, aws.model.ScalarAttributeType),
    rangePK: (String, aws.model.ScalarAttributeType),
    otherAttributes: Seq[(String, aws.model.ScalarAttributeType)],
    indexes: Seq[LocalSecondaryIndex]
  ): TableMeta = {
    create(Table(
      name = name,
      hashPK = hashPK._1,
      rangePK = Some(rangePK._1),
      attributes = Seq(
        AttributeDefinition(hashPK._1, hashPK._2),
        AttributeDefinition(rangePK._1, rangePK._2)
      ) ++: otherAttributes.map(a => AttributeDefinition(a._1, a._2)),
      localSecondaryIndexes = indexes
    ))
  }

  def create(table: Table): TableMeta = createTable(table)

  def createTable(table: Table): TableMeta = {
    val keySchema: Seq[aws.model.KeySchemaElement] = Seq(
      Some(KeySchema(table.hashPK, aws.model.KeyType.HASH)),
      table.rangePK.map(n => KeySchema(n, aws.model.KeyType.RANGE))
    ).flatten.map(_.asInstanceOf[aws.model.KeySchemaElement])

    val req = new aws.model.CreateTableRequest()
      .withTableName(table.name)
      .withAttributeDefinitions(table.attributes.map(_.asInstanceOf[aws.model.AttributeDefinition]).asJava)
      .withKeySchema(keySchema.asJava)
      .withProvisionedThroughput(
        table.provisionedThroughput.map(_.asInstanceOf[aws.model.ProvisionedThroughput]).getOrElse {
          ProvisionedThroughput(readCapacityUnits = 10, writeCapacityUnits = 10)
        }
      )

    if (!table.localSecondaryIndexes.isEmpty) {
      req.setLocalSecondaryIndexes(table.localSecondaryIndexes.map(_.asInstanceOf[aws.model.LocalSecondaryIndex]).asJava)
    }
    if (!table.globalSecondaryIndexes.isEmpty) {
      req.setGlobalSecondaryIndexes(table.globalSecondaryIndexes.map(_.asInstanceOf[aws.model.GlobalSecondaryIndex]).asJava)
    }

    TableMeta(client.createTable(req).getTableDescription)
  }

  def updateTableProvisionedThroughput(table: Table, provisionedThroughput: ProvisionedThroughput): TableMeta = {
    TableMeta(client.updateTable(
      new aws.model.UpdateTableRequest(table.name, provisionedThroughput)
    ).getTableDescription)
  }

  def delete(table: Table): Unit = deleteTable(table)
  def deleteTable(table: Table): Unit = client.deleteTable(table.name)

  // ------------------------------------------
  // Items
  // ------------------------------------------

  def get(table: Table, hashPK: Any): Option[Item] = getItem(table, hashPK)

  def getItem(table: Table, hashPK: Any): Option[Item] = try {
    val attributes = client.getItem(new aws.model.GetItemRequest()
      .withTableName(table.name)
      .withKey(Map(table.hashPK -> AttributeValue.toJavaValue(hashPK)).asJava)
      .withConsistentRead(consistentRead)).getItem

    Option(attributes).map(Item(table, _))
  } catch { case e: aws.model.ResourceNotFoundException => None }

  def get(table: Table, hashPK: Any, rangePK: Any): Option[Item] = getItem(table, hashPK, rangePK)

  def getItem(table: Table, hashPK: Any, rangePK: Any): Option[Item] = {
    rangePK match {
      case None => getItem(table, hashPK)
      case _ =>
        try {
          val attributes = client.getItem(new aws.model.GetItemRequest()
            .withTableName(table.name)
            .withKey(Map(
              table.hashPK -> AttributeValue.toJavaValue(hashPK),
              table.rangePK.get -> AttributeValue.toJavaValue(rangePK)
            ).asJava)
            .withConsistentRead(consistentRead)).getItem

          Option(attributes).map(Item(table, _))
        } catch { case e: aws.model.ResourceNotFoundException => None }
    }
  }

  def batchGet(tableAndAttributes: Map[Table, List[(String, Any)]]): Seq[Item] = {
    import com.amazonaws.services.dynamodbv2.model.{ BatchGetItemRequest, BatchGetItemResult }

    case class State(items: List[Item], keys: java.util.Map[String, KeysAndAttributes])

    @scala.annotation.tailrec
    def next(state: State): (Option[Item], State) =
      state match {
        case State(head :: tail, remaining) => (Some(head), State(tail, remaining))
        case State(Nil, remaining) if !remaining.isEmpty => {
          val result = client.batchGetItem(new BatchGetItemRequest(remaining))
          next(State(toItems(result).toList, result.getUnprocessedKeys()))
        }
        case State(Nil, remaining) if remaining.isEmpty => (None, state)
      }

    def toStream(state: State): Stream[Item] =
      next(state) match {
        case (Some(item), nextState) => Stream.cons(item, toStream(nextState))
        case (None, _) => Stream.Empty
      }

    def toItems(result: BatchGetItemResult): Seq[Item] = {
      result.getResponses.asScala.toSeq.flatMap {
        case (t, as) => { table(t).map(table => as.asScala.toSeq.map { a => Item(table, a) }).getOrElse(Nil) }
      }
    }

    def toJava(tableAndAttributes: Map[Table, List[(String, Any)]]) =
      tableAndAttributes.map {
        case (table, attributes) =>
          table.name -> new KeysAndAttributes().withKeys(
            attributes.map {
            case (k, v) => Map(k -> AttributeValue.toJavaValue(v)).asJava
          }.asJava
          )
      }.asJava

    toStream(State(Nil, toJava(tableAndAttributes)))
  }

  def put(table: Table, hashPK: Any, attributes: (String, Any)*): Unit = {
    putItem(table, hashPK, attributes: _*)
  }
  def putItem(table: Table, hashPK: Any, attributes: (String, Any)*): Unit = {
    put(table, Seq(table.hashPK -> hashPK) ++: attributes: _*)
  }

  def put(table: Table, hashPK: Any, rangePK: Any, attributes: (String, Any)*): Unit = {
    putItem(table, hashPK, rangePK, attributes: _*)
  }
  def putItem(table: Table, hashPK: Any, rangePK: Any, attributes: (String, Any)*): Unit = {
    put(table, Seq(table.hashPK -> hashPK, table.rangePK.get -> rangePK) ++: attributes: _*)
  }

  def attributeValues(attributes: Seq[(String, Any)]): java.util.Map[String, aws.model.AttributeValue] =
    attributes.toMap.mapValues(AttributeValue.toJavaValue(_)).asJava

  def put(table: Table, attributes: (String, Any)*): Unit = putItem(table.name, attributes: _*)
  def putItem(tableName: String, attributes: (String, Any)*): Unit = {
    client.putItem(new aws.model.PutItemRequest()
      .withTableName(tableName)
      .withItem(attributeValues(attributes)))
  }

  def putConditional(tableName: String, attributes: (String, Any)*)(cond: Seq[(String, aws.model.ExpectedAttributeValue)]): Unit = {
    client.putItem(new aws.model.PutItemRequest()
      .withTableName(tableName)
      .withItem(attributeValues(attributes))
      .withExpected(cond.toMap.asJava))
  }

  def addAttributes(table: Table, hashPK: Any, attributes: (String, Any)*): Unit = {
    updateAttributes(table, hashPK, None, aws.model.AttributeAction.ADD, attributes)
  }
  def addAttributes(table: Table, hashPK: Any, rangePK: Any, attributes: (String, Any)*): Unit = {
    updateAttributes(table, hashPK, Some(rangePK), aws.model.AttributeAction.ADD, attributes)
  }

  def deleteAttributes(table: Table, hashPK: Any, attributes: (String, Any)*): Unit = {
    updateAttributes(table, hashPK, None, aws.model.AttributeAction.DELETE, attributes)
  }
  def deleteAttributes(table: Table, hashPK: Any, rangePK: Any, attributes: (String, Any)*): Unit = {
    updateAttributes(table, hashPK, Some(rangePK), aws.model.AttributeAction.DELETE, attributes)
  }

  def putAttributes(table: Table, hashPK: Any, attributes: (String, Any)*): Unit = {
    updateAttributes(table, hashPK, None, aws.model.AttributeAction.PUT, attributes)
  }
  def putAttributes(table: Table, hashPK: Any, rangePK: Any, attributes: (String, Any)*): Unit = {
    updateAttributes(table, hashPK, Some(rangePK), aws.model.AttributeAction.PUT, attributes)
  }

  private[dynamodbv2] def updateAttributes(
    table: Table, hashPK: Any, rangePK: Option[Any], action: AttributeAction, attributes: Seq[(String, Any)]
  ): Unit = {

    val tableKeys = Map(table.hashPK -> AttributeValue.toJavaValue(hashPK)) ++ rangePK.flatMap(rKey => table.rangePK.map(_ -> AttributeValue.toJavaValue(rKey)))

    client.updateItem(new aws.model.UpdateItemRequest()
      .withTableName(table.name)
      .withKey(tableKeys.asJava)
      .withAttributeUpdates(attributes.map {
        case (key, value) =>
          (key, new aws.model.AttributeValueUpdate().withAction(action).withValue(AttributeValue.toJavaValue(value)))
      }.toMap.asJava))
  }

  def deleteItem(table: Table, hashPK: Any): Unit = {
    client.deleteItem(new aws.model.DeleteItemRequest()
      .withTableName(table.name)
      .withKey(Map(table.hashPK -> AttributeValue.toJavaValue(hashPK)).asJava))
  }
  def deleteItem(table: Table, hashPK: Any, rangePK: Any): Unit = {
    client.deleteItem(new aws.model.DeleteItemRequest()
      .withTableName(table.name)
      .withKey(Map(
        table.hashPK -> AttributeValue.toJavaValue(hashPK),
        table.rangePK.get -> AttributeValue.toJavaValue(rangePK)
      ).asJava))
  }

  def queryWithIndex(
    table: Table,
    index: SecondaryIndex,
    keyConditions: Seq[(String, aws.model.Condition)],
    select: Select = aws.model.Select.ALL_ATTRIBUTES,
    attributesToGet: Seq[String] = Nil,
    scanIndexForward: Boolean = true,
    consistentRead: Boolean = false,
    limit: Int = 1000,
    pageStatsCallback: (PageStats => Unit) = null
  ): Seq[Item] = try {

    val req = new aws.model.QueryRequest()
      .withTableName(table.name)
      .withIndexName(index.name)
      .withKeyConditions(keyConditions.toMap.asJava)
      .withSelect(select)
      .withScanIndexForward(scanIndexForward)
      .withConsistentRead(consistentRead)
      .withLimit(limit)
      .withReturnConsumedCapacity(aws.model.ReturnConsumedCapacity.TOTAL)
    if (attributesToGet.nonEmpty) {
      req.setAttributesToGet(attributesToGet.asJava)
    }

    val pager = new QueryResultPager(table, client.query, req, pageStatsCallback)
    pager.toSeq // will return a Stream[Item]
  } catch { case e: aws.model.ResourceNotFoundException => Nil }

  def query(
    table: Table,
    keyConditions: Seq[(String, aws.model.Condition)],
    select: Select = aws.model.Select.ALL_ATTRIBUTES,
    attributesToGet: Seq[String] = Nil,
    scanIndexForward: Boolean = true,
    consistentRead: Boolean = false,
    limit: Int = 1000,
    pageStatsCallback: (PageStats => Unit) = null
  ): Seq[Item] = try {

    val req = new aws.model.QueryRequest()
      .withTableName(table.name)
      .withKeyConditions(keyConditions.toMap.asJava)
      .withSelect(select)
      .withScanIndexForward(scanIndexForward)
      .withConsistentRead(consistentRead)
      .withLimit(limit)
      .withReturnConsumedCapacity(aws.model.ReturnConsumedCapacity.TOTAL)
    if (attributesToGet.nonEmpty) {
      req.setAttributesToGet(attributesToGet.asJava)
    }

    val pager = new QueryResultPager(table, client.query, req, pageStatsCallback)
    pager.toSeq // will return a Stream[Item]
  } catch { case e: aws.model.ResourceNotFoundException => Nil }

  def scan(
    table: Table,
    filter: Seq[(String, aws.model.Condition)],
    limit: Int = 1000,
    segment: Int = 0,
    totalSegments: Int = 1,
    select: Select = aws.model.Select.ALL_ATTRIBUTES,
    attributesToGet: Seq[String] = Nil,
    consistentRead: Boolean = false,
    pageStatsCallback: (PageStats => Unit) = null
  ): Seq[Item] = try {

    val req = new aws.model.ScanRequest()
      .withTableName(table.name)
      .withScanFilter(filter.toMap.asJava)
      .withSelect(select)
      .withLimit(limit)
      .withSegment(segment)
      .withTotalSegments(totalSegments)
      .withConsistentRead(consistentRead)
      .withReturnConsumedCapacity(aws.model.ReturnConsumedCapacity.TOTAL)
    if (attributesToGet.nonEmpty) {
      req.setAttributesToGet(attributesToGet.asJava)
    }

    val pager = new ScanResultPager(table, client.scan, req, pageStatsCallback)
    pager.toSeq // will return a Stream[Item]
  } catch { case e: aws.model.ResourceNotFoundException => Nil }
}

/**
 * Default Implementation
 *
 * @param credentialsProvider credentialsProvider
 */
class DynamoDBClient(
  credentialsProvider: CredentialsProvider = CredentialsLoader.load(),
  region: Region
)
    extends DynamoDB {

  override val client = aws.AmazonDynamoDBClientBuilder.standard()
    .withCredentials(credentialsProvider)
    .withRegion(region.getName)
    .build()
}

/**
 * Configured Implementation
 *
 * @param clientConfiguration clientConfiguration
 * @param credentialsProvider credentialsProvider
 */
class ConfiguredDynamoDBClient(
  clientConfiguration: ClientConfiguration,
  credentialsProvider: CredentialsProvider = CredentialsLoader.load(),
  region: Region
)
    extends DynamoDB {

  override val client = aws.AmazonDynamoDBClientBuilder.standard()
    .withClientConfiguration(clientConfiguration)
    .withCredentials(credentialsProvider)
    .withRegion(region.getName)
    .build()
}

/**
 * New [[awscala.dynamodbv2.DynamoDB]] implementation based on a [[com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder]]
 * which is the new way of constructing a AWS DynamoDB Client.
 *
 * @since Oct 22 2017
 * @param builder [[com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder]] to build an [[com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient]] instance.
 */
class BuildDynamoClient(builder: AmazonDynamoDBClientBuilder) extends DynamoDB {
  override val client = builder.build()
}

