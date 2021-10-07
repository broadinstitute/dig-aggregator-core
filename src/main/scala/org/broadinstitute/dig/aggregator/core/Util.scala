package org.broadinstitute.dig.aggregator.core

/**
  * @author clint
  * @date Sep 13, 2021
  * 
  */
object Util {
  def time[A](msg: String, doLog: String => Unit = println(_))(body: => A): A = {
    val start = System.currentTimeMillis

    try {
      body
    } finally {
      val end = System.currentTimeMillis

      val elapsed = end - start

      doLog(s"${msg} took ${elapsed} ms")
    }
  }
}
