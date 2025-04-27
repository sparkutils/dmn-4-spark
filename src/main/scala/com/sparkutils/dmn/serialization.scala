package com.sparkutils.dmn

import org.apache.spark.sql.functions.{col, collect_set, first, struct}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, functions}

case class Versioned[T](id: Int, version: Int, what: T)

object serialization {

  // TODO are providers ever needing to be ordered?

  type VersionedConfiguration = Versioned[DMNConfiguration]
  type VersionedExecution = Versioned[DMNExecution]
  type VersionedModelService = Versioned[DMNModelService]
  type VersionedFiles = Versioned[DMNFile]
  type VersionedProviders = Versioned[DMNInputField]

  def readVersionedProvidersFromDF(df: DataFrame,
                               ruleSuiteId: Column,
                               ruleSuiteVersion: Column,
                               fieldExpression: Column,
                               providerType: Column,
                               contextPath: Column
                              )(implicit enc: Encoder[VersionedProviders]):
    Dataset[VersionedProviders] =
      df.select(ruleSuiteId.as("id"),
        ruleSuiteVersion.as("version"),
        functions.struct(
          fieldExpression.as("fieldExpression"),
          providerType.as("providerType"),
          contextPath.as("contextPath")
        ).as("what")
      ).as[VersionedProviders]

  def readVersionedFilesFromDF(df: DataFrame,
                               ruleSuiteId: Column,
                               ruleSuiteVersion: Column,
                               locationURI: Column,
                               bytes: Column
                              )(implicit enc: Encoder[VersionedFiles]):
    Dataset[VersionedFiles] =
      df.select(ruleSuiteId.as("id"),
        ruleSuiteVersion.as("version"),
        functions.struct(
          locationURI.as("locationURI"),
          bytes.as("bytes")
        ).as("what")
      ).as[VersionedFiles]

  def readVersionedConfigurationDF(df: DataFrame,
                               ruleSuiteId: Column,
                               ruleSuiteVersion: Column,
                               options: Column
                              )(implicit enc: Encoder[VersionedConfiguration]):
  Dataset[VersionedConfiguration] =
    df.select(ruleSuiteId.as("id"),
      ruleSuiteVersion.as("version"),
      functions.struct(
        options.as("options")
      ).as("what")
    ).as[VersionedConfiguration]

  def readVersionedModelServicesFromDF(df: DataFrame,
                               ruleSuiteId: Column,
                               ruleSuiteVersion: Column,
                               name: Column,
                               namespace: Column,
                               service: Column,
                               resultProvider: Column
                              )(implicit enc: Encoder[VersionedModelService]):
    Dataset[VersionedModelService] =
      df.select(ruleSuiteId.as("id"),
        ruleSuiteVersion.as("version"),
        functions.struct(
          name.as("name"),
          namespace.as("namespace"),
          service.as("service"),
          resultProvider.as("resultProvider")
        ).as("what")
      ).as[VersionedModelService]

  def readVersionedExecutionsFromDF(
                                  files: Dataset[VersionedFiles],
                                  models: Dataset[VersionedModelService],
                                  inputs: Dataset[VersionedProviders],
                                  configuration: Dataset[VersionedConfiguration]
                                 )(implicit enc: Encoder[VersionedExecution]):
    Dataset[VersionedExecution] =
      files.select("id", "version").distinct().join(
        files.groupBy(col("id").as("fid"), col("version").as("fv")).agg(collect_set("what").as("dmnFiles")),
        col("id") === col("fid") && col("version") === col("fv")
      ).join(
        models.groupBy(col("id").as("mid"), col("version").as("mv")).agg(first("what").as("model")),
        col("id") === col("mid") && col("version") === col("mv")
      ).join(
        inputs.groupBy(col("id").as("pid"), col("version").as("pv")).agg(collect_set("what").as("contextProviders")),
        col("id") === col("pid") && col("version") === col("pv")
      ).join(
        configuration.selectExpr("what configuration", "id cid", "version cv"),
        col("id") === col("cid") && col("version") === col("cv")
      ).select(col("id"), col("version"), struct(col("dmnFiles"), col("model"), col("contextProviders"), col("configuration")).as("what"))
        .as[VersionedExecution]

}
