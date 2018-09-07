package org.broadinstitute.dig.aggregator.core.config

/**
 * @author clint
 * Sep 7, 2018
 */
sealed trait DbType {
  def driver: String
  
  def connectionString(dbConfig: DbConfig): String
}

object DbType {
  object MySql extends DbType {
    override val driver: String = "com.mysql.jdbc.Driver"
    
    override def connectionString(dbConfig: DbConfig): String = {
      val queryParams: String = toParamString("useCursorFetch" -> true, "useSSL" -> false)
      
      s"jdbc:mysql://${dbConfig.url}/${dbConfig.schema}?${queryParams}"
    }
    
    private def toParamString(tuples: (String, Any)*): String = {
      tuples.map { case (key, value) => s"${key}=${value}" }.mkString("&")
    }
  }
  
  object H2 extends DbType {
    override val driver: String = "org.h2.Driver"
   
    override def connectionString(dbConfig: DbConfig): String = {
      s"jdbc:h2:mem:inMemoryH2;DB_CLOSE_DELAY=-1;mode=MySQL"
    }
  }
}
