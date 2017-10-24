package awscala

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import org.scalatest.{ BeforeAndAfterAll, Suite }

import scala.util.Try

/**
 * Helper trait to start a local DynamoDB instance for testing.
 */
trait LocalDynamoDB extends BeforeAndAfterAll {
  this: Suite =>

  private var localDynamoServer: DynamoDBProxyServer = _

  /**
   * Start the local dynamo db instance in memory, on port 8000
   */
  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // add libraries to java.library.path
    addToUsrPathsAtRuntime("./lib/DynamoDBLocal_lib/")

    // starting local dynamodb instance with '-port <randomPort> -inMemory'
    localDynamoServer = ServerRunner.createServerFromCommandLineArgs(Array("-port", "8000", "-inMemory"))
    localDynamoServer.start()
  }

  /**
   * Stopping the dynamo db instance
   */
  override protected def afterAll(): Unit = {
    // stopping dynamodb instance
    localDynamoServer.stop()

    super.afterAll()
  }

  /**
   * Workaround to, at runtime, add a path to the java.library.path.
   * Adds the given paths to the `usr_paths` property of the current [[ClassLoader]].
   *
   * Note: does currently not check if given paths are already set on `usr_paths`.
   *
   * @param additionalPaths The paths to add.
   * @return A [[scala.util.Success]] with all paths of `usr_paths` after adding given paths.
   *         Or a [[scala.util.Failure]] when adding the paths failed somehow.
   */
  def addToUsrPathsAtRuntime(additionalPaths: String*): Try[Array[String]] = Try {
    // access the `usr_paths` field
    val usrPathsField = classOf[ClassLoader].getDeclaredField("usr_paths")
    usrPathsField.setAccessible(true)

    // get current value
    val paths = usrPathsField.get(null).asInstanceOf[Array[String]]

    // add and set additional values
    val newPaths = paths ++ additionalPaths
    usrPathsField.set(null, newPaths)

    // return all values
    newPaths
  }
}
