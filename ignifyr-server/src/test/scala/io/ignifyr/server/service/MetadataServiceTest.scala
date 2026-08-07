package io.ignifyr.server.service

import com.typesafe.config.ConfigFactory
import io.ignifyr.engine.config.IgnifyrEngineConfig
import io.ignifyr.server.common.config.WebServerConfig
import io.ignifyr.server.common.spi.IgnifyrServerExtension
import io.onfhir.definitions.resource.fhir.FhirDefinitionsConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future}

/**
 * `/metadata` is fetched on every load of the Ignifyr frontend, so the extension lookup behind it has a
 * hard 1-second bound and swallows every failure. Both properties are invisible from the endpoint test
 * (which only sees a 200), and both are what keep a hung external service from blocking the UI.
 *
 * The lookup is also **not** generic: only the extension whose id is exactly "redcap" is ever asked.
 */
class MetadataServiceTest extends AnyFlatSpec with Matchers {

  private val rootConfig = ConfigFactory.load()

  private def serviceWith(extensions: Seq[IgnifyrServerExtension]): MetadataService =
    new MetadataService(
      new IgnifyrEngineConfig(rootConfig.getConfig("ignifyr")),
      new WebServerConfig(rootConfig.getConfig("webserver")),
      new FhirDefinitionsConfig(rootConfig.getConfig("fhir")),
      extensions
    )

  /** An extension answering `externalComponentVersion` however the test needs it to. */
  private class StubExtension(val id: String, answer: ExecutionContext => Future[Option[String]])
      extends IgnifyrServerExtension {
    override def externalComponentVersion()(implicit ec: ExecutionContext): Future[Option[String]] = answer(ec)
  }

  private def responding(id: String, version: Option[String]) =
    new StubExtension(id, _ => Future.successful(version))

  "getMetadata" should "report the version of the redcap extension" in {
    serviceWith(Seq(responding("redcap", Some("1.2.3")))).getMetadata.ignifyrRedcapVersion shouldBe Some("1.2.3")
  }

  it should "report no redcap version when no extension is installed" in {
    serviceWith(Seq.empty).getMetadata.ignifyrRedcapVersion shouldBe None
  }

  // The seam is documented as generic but is not: MetadataService matches on the literal id "redcap",
  // so implementing the hook in any other module has no effect on /metadata today.
  it should "ignore the version reported by an extension other than redcap" in {
    serviceWith(Seq(responding("observability", Some("9.9.9")))).getMetadata.ignifyrRedcapVersion shouldBe None
  }

  it should "swallow a failing redcap lookup instead of failing the whole metadata response" in {
    val failing = new StubExtension("redcap", _ => Future.failed(new RuntimeException("connection refused")))
    val metadata = serviceWith(Seq(failing)).getMetadata
    metadata.ignifyrRedcapVersion shouldBe None
    metadata.name shouldBe "Ignifyr"
  }

  it should "give up on a redcap lookup that outlasts the one-second bound" in {
    val slow = new StubExtension("redcap", ec => Future { Thread.sleep(3000); Some("too-late") }(ec))
    val startedAt = System.currentTimeMillis()
    serviceWith(Seq(slow)).getMetadata.ignifyrRedcapVersion shouldBe None
    (System.currentTimeMillis() - startedAt) should be < 3000L
  }

  it should "report the repository folders and the archiving settings from the engine config" in {
    val metadata = serviceWith(Seq.empty).getMetadata
    metadata.repositoryNames.mappings should not be empty
    metadata.repositoryNames.schemas should not be empty
    metadata.repositoryNames.jobs should not be empty
    metadata.archiving.archiveFolder should not be empty
  }

  it should "publish the mapping execution configurations the UI displays" in {
    val names = serviceWith(Seq.empty).getMetadata.executionConfigurations.map(_.name)
    names should contain allOf ("Mapping Timeout", "Maximum Chunk Size", "Batch Group Size")
  }
}
