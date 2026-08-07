package io.ignifyr.common.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * `extractExceptionMessages` is what turns a nested failure into the single `description` a
 * `FhirMappingError` carries, so what it drops is what the user never sees in an execution log.
 */
class ExceptionUtilTest extends AnyFlatSpec with Matchers {

  "extractExceptionMessages" should "return the message of an exception with no cause" in {
    ExceptionUtil.extractExceptionMessages(new RuntimeException("only message")) shouldBe "only message"
  }

  it should "join the whole cause chain, outermost first" in {
    val root = new IllegalStateException("root cause")
    val middle = new IllegalArgumentException("middle cause", root)
    val top = new RuntimeException("top level", middle)
    ExceptionUtil.extractExceptionMessages(top) shouldBe "top level\nmiddle cause\nroot cause"
  }

  it should "skip a null message in the middle of the chain" in {
    val root = new IllegalStateException("root cause")
    val middle = new IllegalArgumentException(null: String, root)
    val top = new RuntimeException("top level", middle)
    ExceptionUtil.extractExceptionMessages(top) shouldBe "top level\nroot cause"
  }

  it should "skip an empty message" in {
    ExceptionUtil.extractExceptionMessages(new RuntimeException("", new IllegalStateException("root"))) shouldBe "root"
  }

  // Note the explicit null messages: the RuntimeException(Throwable) constructor would otherwise set the
  // message to the cause's toString, which is exactly the noise this helper is meant to avoid emitting.
  it should "return an empty string when nothing in the chain has a message" in {
    val cause = new IllegalStateException(null: String)
    ExceptionUtil.extractExceptionMessages(new RuntimeException(null: String, cause)) shouldBe ""
  }

  it should "keep the cause's toString when it was used as the wrapper's message" in {
    val messages = ExceptionUtil.extractExceptionMessages(new RuntimeException(new IllegalStateException("root")))
    messages shouldBe "java.lang.IllegalStateException: root\nroot"
  }
}
