package org.broadinstitute.dig.aggregator.core

import doobie.util.transactor.Transactor
import cats.effect.IO
import java.util.UUID

/**
  * @author clint
  * Aug 27, 2018
  */
trait ProvidesH2Transactor {
  private def dbName: String = UUID.randomUUID.toString

  protected val pool: DbPool = {
    import Implicits.contextShift

    //Note mode=MySQL. This allows MySQL-dialect queries to be run against H2.
    new DbPool("org.h2.Driver", s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1;mode=MySQL", "", "")
  }
}
