package io.ignifyr.engine.mapping.service

import io.onfhir.api.Resource
import io.onfhir.api.service.IFhirTerminologyService
import io.onfhir.api.util.FHIRUtil
import io.ignifyr.engine.spi.{LookupEvent, LookupRecorder, LookupTypes}
import org.json4s.{JArray, JBool, JObject, JString, JValue}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

/**
 * Terminology service decorator reporting every translate and lookup call, together with whether it found a
 * match, to a [[LookupRecorder]] (for mapping coverage monitoring). All calls are delegated unchanged;
 * expand and validate-code calls are not recorded.
 *
 * A translation counts as successful with the same rule the `trms` FHIRPath functions apply: the result is
 * `true` and at least one match has an accepted equivalence. A lookup is successful when the code is found.
 *
 * @param delegate The terminology service performing the calls
 * @param recorder Recorder receiving the lookup events
 */
class ObservingTerminologyService(delegate: IFhirTerminologyService, recorder: LookupRecorder)
    extends IFhirTerminologyService {

  import ObservingTerminologyService._

  override def getTimeout: Duration = delegate.getTimeout

  override def translate(
      code: String,
      system: String,
      conceptMapUrl: String,
      version: Option[String],
      conceptMapVersion: Option[String],
      reverse: Boolean
  ): Future[JObject] =
    recordTranslation(Some(conceptMapUrl), Some(system), Some(code))(
      delegate.translate(code, system, conceptMapUrl, version, conceptMapVersion, reverse)
    )

  override def translate(code: String, system: String, conceptMapUrl: String): Future[JObject] =
    recordTranslation(Some(conceptMapUrl), Some(system), Some(code))(delegate.translate(code, system, conceptMapUrl))

  override def translate(
      codingOrCodeableConcept: JObject,
      conceptMapUrl: String,
      conceptMapVersion: Option[String],
      reverse: Boolean
  ): Future[JObject] =
    recordTranslation(Some(conceptMapUrl), codingOrCodeableConcept)(
      delegate.translate(codingOrCodeableConcept, conceptMapUrl, conceptMapVersion, reverse)
    )

  override def translate(codingOrCodeableConcept: JObject, conceptMapUrl: String): Future[JObject] =
    recordTranslation(Some(conceptMapUrl), codingOrCodeableConcept)(
      delegate.translate(codingOrCodeableConcept, conceptMapUrl)
    )

  override def translate(
      code: String,
      system: String,
      source: Option[String],
      target: Option[String],
      version: Option[String],
      reverse: Boolean
  ): Future[JObject] =
    recordTranslation(None, Some(system), Some(code))(
      delegate.translate(code, system, source, target, version, reverse)
    )

  override def translate(
      code: String,
      system: String,
      source: Option[String],
      target: Option[String]
  ): Future[JObject] =
    recordTranslation(None, Some(system), Some(code))(delegate.translate(code, system, source, target))

  override def translate(
      codingOrCodeableConcept: JObject,
      source: Option[String],
      target: Option[String],
      reverse: Boolean
  ): Future[JObject] =
    recordTranslation(None, codingOrCodeableConcept)(
      delegate.translate(codingOrCodeableConcept, source, target, reverse)
    )

  override def translate(
      codingOrCodeableConcept: JObject,
      source: Option[String],
      target: Option[String]
  ): Future[JObject] =
    recordTranslation(None, codingOrCodeableConcept)(delegate.translate(codingOrCodeableConcept, source, target))

  override def lookup(
      code: String,
      system: String,
      version: Option[String],
      date: Option[String],
      displayLanguage: Option[String],
      properties: Seq[String]
  ): Future[Option[JObject]] =
    recordLookup(Some(system), Some(code))(delegate.lookup(code, system, version, date, displayLanguage, properties))

  override def lookup(code: String, system: String): Future[Option[JObject]] =
    recordLookup(Some(system), Some(code))(delegate.lookup(code, system))

  override def lookup(
      coding: JObject,
      date: Option[String],
      displayLanguage: Option[String],
      properties: Seq[String]
  ): Future[Option[JObject]] = {
    val (system, code) = sourceCoding(coding)
    recordLookup(system, code)(delegate.lookup(coding, date, displayLanguage, properties))
  }

  override def lookup(coding: JObject): Future[Option[JObject]] = {
    val (system, code) = sourceCoding(coding)
    recordLookup(system, code)(delegate.lookup(coding))
  }

  override def expandWithId(
      id: String,
      filter: Option[String],
      offset: Option[Long],
      count: Option[Long]
  ): Future[JObject] = delegate.expandWithId(id, filter, offset, count)

  override def expand(
      url: String,
      version: Option[String],
      filter: Option[String],
      offset: Option[Long],
      count: Option[Long]
  ): Future[JObject] = delegate.expand(url, version, filter, offset, count)

  override def expandWithValueSet(valueSet: Resource, offset: Option[Long], count: Option[Long]): Future[JObject] =
    delegate.expandWithValueSet(valueSet, offset, count)

  override def validateCode(
      url: String,
      valueSetVersion: Option[String],
      code: String,
      system: Option[String],
      systemVersion: Option[String],
      display: Option[String]
  ): Future[JObject] = delegate.validateCode(url, valueSetVersion, code, system, systemVersion, display)

  private def recordTranslation(conceptMap: Option[String], codingOrCodeableConcept: JObject)(
      call: => Future[JObject]
  ): Future[JObject] = {
    val (system, code) = sourceCoding(codingOrCodeableConcept)
    recordTranslation(conceptMap, system, code)(call)
  }

  /**
   * Record the outcome of a translation once it completes. A failed call (e.g. an unreachable terminology
   * server) is not a coverage result; it surfaces as a mapping error instead.
   */
  private def recordTranslation(conceptMap: Option[String], sourceSystem: Option[String], sourceCode: Option[String])(
      call: => Future[JObject]
  ): Future[JObject] =
    call.andThen { case Success(parameters) =>
      val accepted = acceptedMatch(parameters)
      safeRecord(
        LookupEvent(
          lookupType = LookupTypes.TERMINOLOGY,
          operation = "translate",
          conceptMap = conceptMap,
          sourceSystem = sourceSystem,
          sourceCode = sourceCode,
          targetSystem = accepted.flatMap(_._1),
          targetCode = accepted.flatMap(_._2),
          success = accepted.isDefined
        )
      )
    }(ExecutionContext.parasitic)

  private def recordLookup(sourceSystem: Option[String], sourceCode: Option[String])(
      call: => Future[Option[JObject]]
  ): Future[Option[JObject]] =
    call.andThen { case Success(result) =>
      safeRecord(
        LookupEvent(
          lookupType = LookupTypes.TERMINOLOGY,
          operation = "lookup",
          sourceSystem = sourceSystem,
          sourceCode = sourceCode,
          success = result.isDefined
        )
      )
    }(ExecutionContext.parasitic)

  // Monitoring must never break the mapping
  private def safeRecord(event: LookupEvent): Unit = Try(recorder.record(event))
}

object ObservingTerminologyService {

  /** Equivalence / relationship codes of a translation match accepted as a mapping (same set as the `trms` functions). */
  val acceptedEquivalenceCodes: Set[String] = Set("relatedto", "equivalent", "equal", "wider", "subsumes")

  /**
   * The (system, code) of the first match with an accepted equivalence in a translate result, if the result is `true`.
   *
   * @param parameters Parameters resource returned by the translate operation
   * @return The target (system, code) of the accepted match, or None if nothing was matched
   */
  def acceptedMatch(parameters: JObject): Option[(Option[String], Option[String])] =
    Try {
      FHIRUtil.getParameterValueByName(parameters, "result") match {
        case Some(JBool(true)) =>
          val matches: Seq[Seq[(String, JValue)]] =
            FHIRUtil.getParameterValueByName(parameters, "match") match {
              // Multiple matches: an array of part arrays
              case Some(JArray(mtchs)) if mtchs.forall(_.isInstanceOf[JArray]) =>
                mtchs.map(_.asInstanceOf[JArray].arr.collect { case p: JObject => FHIRUtil.parseParameter(p) })
              // Single match: an array of parts
              case Some(JArray(parts)) if parts.forall(_.isInstanceOf[JObject]) =>
                Seq(parts.map(p => FHIRUtil.parseParameter(p.asInstanceOf[JObject])))
              case _ => Nil
            }
          matches
            .find(parts =>
              parts.exists {
                case ("relationship" | "equivalence", JString(eq)) => acceptedEquivalenceCodes.contains(eq)
                case _ => false
              }
            )
            .map { parts =>
              val concept = parts.collectFirst { case ("concept", c: JObject) => c }
              concept.map(stringField(_, "system")).flatten -> concept.map(stringField(_, "code")).flatten
            }
        case _ => None
      }
    }.toOption.flatten

  /**
   * The (system, code) of a Coding, or of the first coding of a CodeableConcept.
   */
  def sourceCoding(codingOrCodeableConcept: JObject): (Option[String], Option[String]) = {
    val coding = codingOrCodeableConcept \ "coding" match {
      case JArray((first: JObject) :: _) => first
      case _ => codingOrCodeableConcept
    }
    stringField(coding, "system") -> stringField(coding, "code")
  }

  private def stringField(obj: JObject, field: String): Option[String] =
    obj \ field match {
      case JString(s) => Some(s)
      case _ => None
    }
}
