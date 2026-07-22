package io.ignifyr

import io.ignifyr.engine.model.{FhirMappingResult, MappedFhirResource}
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Timestamp

/**
 * Shared FHIR-mapping-result test fixtures: a representative dataset of mapped resources (10 Patients
 * and 5 Conditions) used by the file sink-format suites across modules (connector-file's writer tests
 * and the enterprise Delta format tests), so the sizeable builder is defined once here rather than
 * duplicated per module.
 */
object FhirMappingResultFixtures {

  /** A dataset with 10 Patient and 5 Condition FHIR mapping results. */
  def sampleFhirMappingResults(spark: SparkSession): Dataset[FhirMappingResult] = {
    import spark.implicits._
    Seq(
      // Patients
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Patient\",\"id\":\"34dc88d5972fd5472a942fc80f69f35c\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p1\"}],\"gender\":\"male\",\"birthDate\":\"2000-05-10\"}"
            ),
            mappingExpr = Some("expression1"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Patient")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Patient\",\"id\":\"0b3a0b23a0c6e223b941e63787f15a6a\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p2\"}],\"gender\":\"male\",\"birthDate\":\"1985-05-08\",\"deceasedDateTime\":\"2017-03-10\",\"address\":[{\"use\":\"home\",\"type\":\"both\",\"postalCode\":\"G02547\"}]}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Patient")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Patient\",\"id\":\"49d3c335681ab7fb2d4cdf19769655db\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p3\"}],\"gender\":\"male\",\"birthDate\":\"1997-02\"}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Patient")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Patient\",\"id\":\"0bbad2343eb86d5cdc16a1b292537576\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p4\"}],\"gender\":\"male\",\"birthDate\":\"1999-06-05\",\"address\":[{\"use\":\"home\",\"type\":\"both\",\"postalCode\":\"H10564\"}]}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Patient")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Patient\",\"id\":\"7b650be0176d6d29351f84314a5efbe3\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p5\"}],\"gender\":\"male\",\"birthDate\":\"1965-10-01\",\"deceasedDateTime\":\"2019-04-21\",\"address\":[{\"use\":\"home\",\"type\":\"both\",\"postalCode\":\"G02547\"}]}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Patient")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Patient\",\"id\":\"17c7c9664ac82f384de0ad4625f2ae4c\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p6\"}],\"gender\":\"female\",\"birthDate\":\"1991-03\"}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Patient")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Patient\",\"id\":\"e1ea114dcfcea572982f224e43deb7a6\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p7\"}],\"gender\":\"female\",\"birthDate\":\"1972-10-25\",\"address\":[{\"use\":\"home\",\"type\":\"both\",\"postalCode\":\"V13135\"}]}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Patient")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Patient\",\"id\":\"f6bf84d63799f65dcdd4f5027021adf3\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p8\"}],\"gender\":\"female\",\"birthDate\":\"2010-01-10\",\"address\":[{\"use\":\"home\",\"type\":\"both\",\"postalCode\":\"Z54564\"}]}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Patient")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Patient\",\"id\":\"a06f7d449f8a655d9163204f0de8237f\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p9\"}],\"gender\":\"female\",\"birthDate\":\"1999-05-12\"}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Patient")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Patient\",\"id\":\"7bd4fad75b1efbdc50859a736b839e24\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"active\":true,\"identifier\":[{\"use\":\"official\",\"system\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\",\"value\":\"p10\"}],\"gender\":\"female\",\"birthDate\":\"2003-11\"}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Patient")
      ),
      // Conditions
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Condition\",\"id\":\"2faab6373e7c3bba4c1971d089fc6407\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Condition\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"active\"}]},\"verificationStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-ver-status\",\"code\":\"confirmed\"}]},\"category\":[{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-category\",\"code\":\"problem-list-item\"}]}],\"code\":{\"coding\":[{\"system\":\"http://hl7.org/fhir/sid/icd-10\",\"code\":\"J13\",\"display\":\"Pneumonia due to Streptococcus pneumoniae\"}]},\"subject\":{\"reference\":\"Patient/34dc88d5972fd5472a942fc80f69f35c\"},\"onsetDateTime\":\"2012-10-15\"}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Condition")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Condition\",\"id\":\"63058b87a718e66d4198703675b0204a\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Condition\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"inactive\"}]},\"category\":[{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-category\",\"code\":\"encounter-diagnosis\"}]}],\"code\":{\"coding\":[{\"system\":\"http://hl7.org/fhir/sid/icd-10\",\"code\":\"G40\",\"display\":\"Parkinson\"}]},\"subject\":{\"reference\":\"Patient/0b3a0b23a0c6e223b941e63787f15a6a\"},\"encounter\":{\"reference\":\"Encounter/bb7134de6cdbf64352b074e9d2555adb\"},\"onsetDateTime\":\"2013-05-07\",\"abatementDateTime\":\"2013-05-22\"}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Condition")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Condition\",\"id\":\"ec4aed2cb844c70104e467fad58f6a44\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Condition\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"inactive\"}]},\"verificationStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-ver-status\",\"code\":\"unconfirmed\"}]},\"category\":[{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-category\",\"code\":\"encounter-diagnosis\"}]}],\"code\":{\"coding\":[{\"system\":\"http://hl7.org/fhir/sid/icd-10\",\"code\":\"J85\",\"display\":\"Abscess of lung and mediastinum\"}]},\"subject\":{\"reference\":\"Patient/49d3c335681ab7fb2d4cdf19769655db\"},\"onsetDateTime\":\"2016-02-11\",\"asserter\":{\"reference\":\"Practitioner/09361569c5dee906d244968c680cf2b4\"}}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Condition")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Condition\",\"id\":\"6e0337f749b68a5450efb3fe6f918362\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Condition\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"inactive\"}]},\"category\":[{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-category\",\"code\":\"encounter-diagnosis\"}]}],\"code\":{\"coding\":[{\"system\":\"http://hl7.org/fhir/sid/icd-10\",\"code\":\"M89.9\",\"display\":\"Disorder of bone, unspecified\"}]},\"subject\":{\"reference\":\"Patient/0bbad2343eb86d5cdc16a1b292537576\"},\"onsetDateTime\":\"2014-01-05T10:00:00Z\"}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Condition")
      ),
      FhirMappingResult(
        jobId = "job",
        mappingTaskName = "example-mappingTask-name",
        timestamp = new Timestamp(System.currentTimeMillis()),
        mappedFhirResource = Some(
          MappedFhirResource(
            mappedResource = Some(
              "{\"resourceType\":\"Condition\",\"id\":\"14ce4f8a1b8161ad59e1a8d67ce8d06d\",\"meta\":{\"profile\":[\"https://aiccelerate.eu/fhir/StructureDefinition/AIC-Condition\"],\"source\":\"https://aiccelerate.eu/data-integration-suite/pilot1-data\"},\"clinicalStatus\":{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-clinical\",\"code\":\"inactive\"}]},\"category\":[{\"coding\":[{\"system\":\"http://terminology.hl7.org/CodeSystem/condition-category\",\"code\":\"encounter-diagnosis\"}]}],\"code\":{\"coding\":[{\"system\":\"http://hl7.org/fhir/sid/icd-10\",\"code\":\"G40.419\",\"display\":\"Other generalized epilepsy and epileptic syndromes, intractable, without status epilepticus\"}]},\"subject\":{\"reference\":\"Patient/7b650be0176d6d29351f84314a5efbe3\"},\"onsetDateTime\":\"2009-04-07\",\"asserter\":{\"reference\":\"Practitioner/b2e43c8d7dae698f539b1924679a7814\"}}"
            ),
            mappingExpr = Some("expression"),
            fhirInteraction = None
          )
        ),
        source = "Source",
        error = None,
        executionId = Some("exec"),
        projectId = Some("project"),
        resourceType = Some("Condition")
      )
    ).toDS()
  }
}
