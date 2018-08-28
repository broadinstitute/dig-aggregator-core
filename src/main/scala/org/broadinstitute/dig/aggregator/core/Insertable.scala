package org.broadinstitute.dig.aggregator.core

import doobie.util.transactor.Transactor
import cats.effect.IO

/**
 * @author clint
 * Aug 28, 2018
 */
trait Insertable {
  def insert(xa: Transactor[IO]): IO[Int]
}
