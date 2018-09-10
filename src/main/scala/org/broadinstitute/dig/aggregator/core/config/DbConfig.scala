package org.broadinstitute.dig.aggregator.core.config

import doobie.util.transactor.Transactor
import cats.effect.IO
import org.json4s.Serializer
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JValue
import org.json4s.Formats
import org.json4s.TypeHints
import org.json4s.DefaultFormats

/**
 * @author clint
 * Sep 7, 2018
 */
sealed abstract class DbConfig {
  
  def dbType: DbType
  def url: String
  def schema: String
  def user: String
  def password: String
  
  /**
   * Create a new connection to the database for running queries.
   *
   * A connection pool here isn't really all that big a deal because queries
   * are run serially while processing Kafka messages.
   */
  final def newTransactor: Transactor[IO] = {
    val connectionString = dbType.connectionString(this)
    
    // create the connection
    Transactor.fromDriverManager[IO](
      dbType.driver,
      connectionString,
      user,
      password)
  }
}

object DbConfig {
  protected abstract class WithDbType(override val dbType: DbType) extends DbConfig
  
  /**
   * MysQL configuration settings.
   */
  final case class MySQL(
      url: String,
      schema: String,
      user: String,
      password: String
  ) extends WithDbType(DbType.MySql)
  
  final case class H2(
      url: String,
      schema: String,
      user: String,
      password: String
  ) extends WithDbType(DbType.H2)

  private val hints: TypeHints = new TypeHints {
    private[this] val mapping: Map[String, Class[_]] = {
      Map(DbType.MySql.name -> classOf[MySQL], DbType.H2.name -> classOf[H2])
    }
    
    override val hints: List[Class[_]] = mapping.values.toList

    /** Return hint for given type. */
    override def hintFor(clazz: Class[_]): String = mapping.collectFirst { case (name, cl) if cl == clazz => name }.get 

    /** Return type for given hint. */
    override def classFor(hint: String): Option[Class[_]] = mapping.get(hint)
  }
  
  val formats: Formats = DefaultFormats.withHints(hints).withTypeHintFieldName("dbType")
}
