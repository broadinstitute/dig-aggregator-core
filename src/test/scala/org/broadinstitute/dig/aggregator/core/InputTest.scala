package org.broadinstitute.dig.aggregator.core

import java.time.LocalDateTime

import org.scalatest.funsuite.AnyFunSuite

final class InputTest extends AnyFunSuite {
  import Implicits.S3Key

  // create a dummy input
  def input(name: String): Input = Input(name, LocalDateTime.now)

  test("s3 key implicits - simple") {
    val key = "foo.txt"

    assert(key.basename == "foo.txt")
    assert(key.commonPrefix.isEmpty)
  }

  test("s3 key implicits - exact") {
    val key = "variants/dataset/phenotype/metadata"

    assert(key.basename == "metadata")
    assert(key.commonPrefix == "variants/dataset/phenotype/")
  }

  test("s3 key implicits - wildcard") {
    val key = "out/metaanalysis/*/_SUCCESS"

    assert(key.basename == "_SUCCESS")
    assert(key.commonPrefix == "out/metaanalysis/")
  }

  test("input parts") {
    val i = input("a/foo/bar/baz")

    assert(i.basename == "baz")
    assert(i.dirname == "a/foo/bar/")
  }

  test("source prefix must end with /") {
    assertThrows[IllegalArgumentException] {
      Input.Source("foo", "bar", None)
    }
  }

  test("source basename must not end with /") {
    assertThrows[IllegalArgumentException] {
      Input.Source("foo", "bar/", None)
    }
  }

  test("source match inputs") {
    val a = Input.Source("a/*/", "bar", None)
    val b = Input.Source("b/test=*/", "*", None)

    assert(a.matches(input("a/foo/bar")))
    assert(b.matches(input("b/test=foo/any")))
  }

  test("source pattern matching prefix") {
    val source = Input.Source("a/test=*/bar/*/", "baz", None)

    input("a/test=foo/bar/test/baz") match {
      case source(foo, test) => assert(foo == "foo" && test == "test")
      case _                 => fail("match failed")
    }
  }

  test("source pattern matching basename") {
    val source = Input.Source("a/test=*/bar/*/", "b*", None)

    input("a/test=foo/bar/test/baz") match {
      case source(foo, test, az) => assert(foo == "foo" && test == "test" && az == "az")
      case _                     => fail("match failed")
    }
  }
}
