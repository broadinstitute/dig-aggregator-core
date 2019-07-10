package org.broadinstitute.dig.aggregator.core

import java.nio.file.FileSystems
import java.nio.file.Paths

/** A simple "glob-like" pattern matcher for strings. */
class Glob(pattern: String) {

  // the matcher used to compare against strings
  private val matcher = FileSystems.getDefault.getPathMatcher(s"glob:$pattern")

  /** Returns true if the pattern successfully matches the string. */
  def matches(s: String): Boolean = matcher.matches(Paths.get(s))
}

/** Companion object for constructing globs. */
object Glob {

  /** A glob that matches everything. */
  object True extends Glob("*") {
    override def matches(s: String): Boolean = true
  }

  /** A glob that matches nothing. */
  object False extends Glob("") {
    override def matches(s: String): Boolean = false
  }

  /** Helper implicit so "foo".toGlob is possible. */
  final implicit class String2Glob(val s: String) extends AnyVal {
    def toGlob: Glob = new Glob(s)
  }
}
