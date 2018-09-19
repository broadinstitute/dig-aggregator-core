package org.broadinstitute.dig.aggregator.core

import cats.effect.IO
import doobie.util.transactor.Transactor

/**
 * @author clint
 * Sep 11, 2018
 */
final case class Xas(read: Transactor[IO], write: Transactor[IO])
