package org.broadinstitute.dig.aggregator.core.config

import doobie.util.transactor.Transactor
import cats.effect.IO
import org.json4s.JsonAST.JValue
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JObject
import org.json4s.JsonAST.JString
import org.json4s.Formats
import org.json4s.DefaultFormats
import org.json4s.Serializer

/**
 * @author clint
 * Sep 7, 2018
 */
sealed trait DbType {
  def name: String
  
  def driver: String
  
  def connectionString(dbConfig: DbConfig): String
}

object DbType {
  object MySql extends DbType {
    override def name = "mysql"
    
    override val driver: String = "com.mysql.jdbc.Driver"
    
    override def connectionString(dbConfig: DbConfig): String = {
      val queryParams: String = toParamString("useCursorFetch" -> true, "useSSL" -> false)
      
      s"jdbc:mysql://${dbConfig.url}/${dbConfig.schema}?${queryParams}"
    }
    
    private[config] def toParamString(tuples: (String, Any)*): String = {
      tuples.map { case (key, value) => s"${key}=${value}" }.mkString("&")
    }
  }
  
  object H2 extends DbType {
    override def name = "h2"
    
    override val driver: String = "org.h2.Driver"
   
    override def connectionString(dbConfig: DbConfig): String = {
      s"jdbc:h2:mem:${dbConfig.schema};DB_CLOSE_DELAY=-1;mode=MySQL"
    }
  }
  
  val serializer: Serializer[DbType] = new CustomSerializer[DbType](formats => {
    val ser: PartialFunction[JValue, DbType] = {
      case JString(n) if n == MySql.name => MySql
      case JString(n) if n == H2.name => H2
    }
    
    val deser: PartialFunction[Any, JValue] = {
      case dbType: DbType => JString(dbType.name)
    }
    
    (ser, deser)
  })
  
  implicit val formats: Formats = DefaultFormats + serializer
}
