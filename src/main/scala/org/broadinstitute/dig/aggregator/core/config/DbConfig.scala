package org.broadinstitute.dig.aggregator.core.config

trait DbConfig {
  def url: String
  def driver: String
  def schema: String
  def user: String
  def password: String
}
