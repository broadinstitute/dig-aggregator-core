package org.broadinstitute.dig.aggregator.core

/** A simple, glob-like, pattern matcher for strings. */
case class Glob(glob: String, pathSep: Char = '/') {
  import atto._, Atto._

  /** Create a parse combinator for a prefix pattern. */
  private val parser =
    if (glob.isEmpty) {
      err("Empty glob")
    } else {
      val validChar = letterOrDigit | oneOf("-._~:/?#[]@!$&'()+,;=")

      // exact text match
      val exact = validChar.many1.map { s =>
        string(s.toList.mkString) ~> ok(None: Option[String])
      }

      // wildcard capture glob text up to next character or path separator
      val capture = char('*') ~> opt(validChar).map {
        case Some(c) => takeWhile(n => n != c && n != pathSep).map(Some(_)) <~ char(c)
        case _       => takeWhile(_ != pathSep).map(Some(_))
      }

      // start with an exact match, intersperse captures
      val ParseResult.Done("", parts) = ((exact | capture).many1 <~ endOfInput).parseOnly(glob)

      // fold all the parts together into a single parser
      parts.foldLeft(ok(List.empty[String])) { (a, p) =>
        for (xs <- a; capture <- p) yield {
          capture match {
            case Some(s) => xs :+ s
            case None    => xs
          }
        }
      }
    }

  /** Extractor method for pattern matching.
    *
    * Allows taking a path-like string and matching path elements with
    * the glob. Wildcard elements are captured and returned in the match.
    * Everything else is an exact match and ignored.
    */
  def unapplySeq(path: String): Option[List[String]] = {
    (parser <~ endOfInput).parseOnly(path) match {
      case ParseResult.Done(_, captures) => Some(captures)
      case _                             => None
    }
  }

  /** Returns true if the pattern successfully matches the string. */
  def matches(path: String): Boolean = unapplySeq(path).isDefined
}

/** Companion object for constructing globs. */
object Glob {

  /** A glob that matches everything.
    *
    * This is a special case since Glob.parsers would fail to parse this
    * pattern. This is intentional, though, so that stage input sources
    * cannot match everything.
    */
  object True extends Glob("*")

  /** A glob that matches nothing.
    *
    * This is a special case since Glob.parsers would fail to parse this
    * pattern. This is intentional, though, so that stage input sources
    * cannot match nothing.
    */
  object False extends Glob("")
}
