package com.sparkutils.dmn

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{ShimUtils, functions}

/**
 * Represents a DMN file, could have been on disk or from a database etc.
 *
 * @param locationURI locationURI used for imports, probably just the file name
 * @param bytes the raw xml file, boms and all
 */
case class DMNFile(locationURI: String, bytes: Array[Byte]) extends Serializable

/**
 * An individual input field necessary for constructing the DMNContext
 * @param fieldExpression an sql expression (could be the input field name or more complex expressions, or implementation specific) producing the input value for this provider
 * @param providerType the type of the input provider (JSON, DDL, or an implementation specific classname)
 * @param contextPath a string representation of the DMNContextPath to store the results in (implementation specific)
 * @param stillSetWhenNull specifies if, when the fieldExpression is null what should happen to the contextPath.
 *                         The default value of true specifies that a context entry should be made with a null value
 */
case class DMNInputField(fieldExpression: String, providerType: String, contextPath: String, stillSetWhenNull: Boolean = true) extends Serializable {
  /**
   * Implementations are free to choose a different parsing approach for the fieldExpression
   * @return
   */
  def defaultExpr: Expression = ShimUtils.expression( functions.expr(fieldExpression) )

}

/**
 * Model service definitions
 * @param name
 * @param namespace
 * @param service optional, when not provided or supported by the engine executeAll will be used
 * @param resultProvider a string representation of the provider (typically DDL, but implementations may process this differently)
 */
case class DMNModelService(name: String, namespace: String, service: Option[String], resultProvider: String) extends Serializable

/**
 * Represents any execution specific configuration
 * @param options an implementation specific encoding of options, provided to all repository functions and execution.
 *                A default name=value;flag1;name2=value2 encoding scheme can be used if the runtime supports it.
 * @param runtime when provided the dmn-4-spark api will attempt to load this runtime (if not already the default)
 */
case class DMNConfiguration(options: String = "", runtime: Option[String] = None) extends Serializable

object DMNConfiguration {
  val empty: DMNConfiguration = DMNConfiguration()
}

/**
 * Represents a complete set of information necessary for DMN execution
 * @param dmnFiles the dmn modules to be loaded
 * @param model the model to execute (with or without DecisionService) and the return processing
 * @param contextProviders the fields to inject into the DMN Context
 * @param configuration an implementation specific encoding of options, provided to all repository functions and execution
 */
case class DMNExecution(dmnFiles: Seq[DMNFile], model: DMNModelService,
                        contextProviders: Seq[DMNInputField],
                        configuration: DMNConfiguration = DMNConfiguration.empty) extends Serializable
