package org.broadinstitute.dig.aggregator.core

import java.nio.file.FileSystems
import java.nio.file.Paths

import scala.util.matching.Regex

/** A simple "glob-like" pattern matcher for strings. */
case class Glob(pattern: String) {

  // the matcher used to compare against strings
  private lazy val matcher = FileSystems.getDefault.getPathMatcher(s"glob:$pattern")

  // used for pattern extraction of a path
  private lazy val matchPattern = {
    val wild = "([^*]*)\\*".r
    val paths = pattern.split('/').map {
      case "*"          => "([^/]+)"
      case "..."        => "[^/]+(?:/[^/]+)*"
      case wild(prefix) => Regex.quote(prefix) + "([^/]+)"
      case path         => Regex.quote(path)
    }

    paths.mkString("/").r
  }

  /** Returns true if the pattern successfully matches the string. */
  def matches(s: String): Boolean = matcher.matches(Paths.get(s))

  /** Extractor method for pattern matching.
    *
    * Allows taking a path-like string and matching path elements with
    * the glob. For example:
    *
    * val pattern = Glob("out/*/phenotype=*/.../log/...")
    *
    * "out/regions/phenotype=T2D/gregor/summary/log/warnings.txt" match {
    *   case pattern(source, phenotype) => println(source, phenotype)
    * }
    *
    * The above will output "(regions, T2D)".
    *
    * Path elements are matched exactly. Wildcard elements are matched
    * and returned in the pattern. Ellipses are ignored, but allow for
    * matching of an arbitrary number of path elements until the next
    * path element is matched.
    */
  def unapplySeq(path: String): Option[List[String]] = {
    matchPattern.unapplySeq(path)
  }
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
