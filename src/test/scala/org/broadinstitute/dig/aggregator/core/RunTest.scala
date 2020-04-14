package org.broadinstitute.dig.aggregator.core

import cats.data.NonEmptyList
import java.util.UUID

import org.broadinstitute.dig.aggregator.core.config.BaseConfig

/**
  * @author clint
  * Aug 28, 2018
  */
final class RunTest extends DbFunSuite {
  def makeInput(): UUID = UUID.randomUUID()

  // used for getting the work of a processor
  val dummyOpts: Processor.Opts = Processor.Opts(
    reprocess = false,
    insertRuns = false,
    noInsertRuns = false,
    only = None,
    exclude = None,
  )

  dbTest("insert") {
    assert(allResults.isEmpty)

    insertRun(TestProcessor.a, NonEmptyList(makeInput(), Nil))

    assert(allResults.size == 1)

    insertRun(TestProcessor.b, NonEmptyList(makeInput(), Nil))
    insertRun(TestProcessor.b, NonEmptyList(makeInput(), Nil))

    assert(allResults.size == 3)
  }

  dbTest("insert - multiple inputs") {
    val inputs = NonEmptyList(makeInput(), List(makeInput(), makeInput()))
    val uuid   = insertRun(TestProcessor.a, inputs)

    // there should be 3 rows inserted - 1 per input
    assert(runResults(uuid).size == 3)
  }

  /* FOR ALL TESTS BELOW THIS POINT, PLEASE TAKE CARE TO LOOK AT THE TestProcessor
   * objects and their dependencies. These are fixed so that it's easy to test
   * several cases (chains of dependencies and multiple dependencies).
   */

  dbTest("lookup work to be done 1") {
    val a_run = insertRun(TestProcessor.a, NonEmptyList(makeInput(), Nil))
    val b_run = insertRun(TestProcessor.b, NonEmptyList(a_run, Nil))

    // get the work that processor b needs to do
    val b      = Processor(TestProcessor.b)(TestProcessor.dummyConfig, pool).get
    val b_work = b.getWork(dummyOpts).unsafeRunSync()

    // make sure that processor b is up to date
    assert(b_work.isEmpty)

    // get the work that processor c needs to do
    val c      = Processor(TestProcessor.c)(TestProcessor.dummyConfig, pool).get
    val c_work = c.getWork(dummyOpts).unsafeRunSync()

    // ensure that C_output needs to be made with the input B_output
    assert(c_work.contains("C_output"))
    assert(c_work("C_output") == Set(b_run))
  }

  dbTest("lookup work to be done 2") {
    val c_run = insertRun(TestProcessor.c, NonEmptyList(makeInput(), Nil))
    val d_run = insertRun(TestProcessor.d, NonEmptyList(makeInput(), Nil))

    // get the work that processor e needs to do
    val e      = Processor(TestProcessor.e)(TestProcessor.dummyConfig, pool).get
    val e_work = e.getWork(dummyOpts).unsafeRunSync()

    // ensure that it needs to process both c and d output
    assert(e_work.contains("E_output"))
    assert(e_work("E_output") == Set(c_run, d_run))

    // pretend that E ran
    insertRun(TestProcessor.e, NonEmptyList(c_run, List(d_run)))

    // update the run for d and get the work of processor e again
    val d_run2  = insertRun(TestProcessor.d, NonEmptyList(makeInput(), Nil))
    val e_work2 = e.getWork(dummyOpts).unsafeRunSync()

    // ensure it needs to run and only process d's output
    assert(e_work2.contains("E_output"))
    assert(e_work2("E_output") == Set(d_run2))

    // pretend that E ran again
    insertRun(TestProcessor.e, NonEmptyList(d_run2, Nil))

    // get the work and ensure it's empty
    val e_work3 = e.getWork(dummyOpts).unsafeRunSync()
    assert(e_work3.isEmpty)
  }
}
