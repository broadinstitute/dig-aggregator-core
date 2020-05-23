package org.broadinstitute.dig.aggregator.core

import java.util.UUID

trait ProvidesH2Transactor {
  private def dbName: String = UUID.randomUUID.toString

  protected lazy val pool: DbPool = {
    new DbPool(s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1;mode=MySQL", "h2", "", "")
  }
}
