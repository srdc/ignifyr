package io.ignifyr.connector.sql

import io.ignifyr.engine.model.{SqlSource, SqlSourceSettings}
import io.ignifyr.engine.spi.ExtensionRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Verifies the SQL connector is discovered through ServiceLoader when this module is on the
 * classpath (no Docker required — this only inspects the extension registry).
 */
class SqlConnectorExtensionSpec extends AnyFlatSpec with Matchers {

  "The SQL connector extension" should "register a SqlSource connector through ServiceLoader" in {
    val connector = ExtensionRegistry.sourceConnectors.get(classOf[SqlSource])
    connector.map(_.id) shouldBe Some("sql")
    connector.map(_.settingsClass) shouldBe Some(classOf[SqlSourceSettings])
  }
}
