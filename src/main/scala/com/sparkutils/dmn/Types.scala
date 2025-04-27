package com.sparkutils.dmn

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.apache.spark.sql.{Column, ShimUtils, functions}

import java.util.ServiceLoader
import scala.collection.immutable.{IndexedSeq, Seq}


/**
 * Represents a DMN file, could have been on disk or from a database etc.
 *
 * @param locationURI locationURI used for imports, probably just the file name
 * @param bytes the raw xml file, boms and all
 */
case class DMNFile(locationURI: String, bytes: Array[Byte]) extends Serializable

/**
 * Model service definitions
 * @param name
 * @param namespace
 * @param service optional, when not provided or supported by the engine executeAll will be used
 * @param resultProvider a string representation of the provider (typically DDL, but implementations may process this differently)
 */
case class DMNModelService(name: String, namespace: String, service: Option[String], resultProvider: String) extends Serializable

/**
 * An individual input field necessary for constructing the DMNContext
 * @param fieldExpression an sql expression (could be the input field name or more complex expressions, or implementation specific) producing the input value for this provider
 * @param providerType the type of the input provider (JSON, DDL, or an implementation specific classname)
 * @param contextPath a string representation of the DMNContextPath to store the results in (implementation specific)
 */
case class DMNInputField(fieldExpression: String, providerType: String, contextPath: String) extends Serializable {
  /**
   * Implementations are free to chose a different parsing approach for the fieldExpression
   * @return
   */
  def defaultExpr: Expression = ShimUtils.expression( functions.expr(fieldExpression) )
}

/**
 * A provider for DMN Context injection
 */
trait DMNContextProvider[R] extends Expression {
  val contextPath: DMNContextPath

  override def dataType: DataType = ObjectType(classOf[(DMNContextPath, R)])
}

case class DMNException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
}

/**
 * Represents a DMN Result from an engine
 */
trait DMNResult

/**
 * A path along a DMN Context (e.g. an input variable location)
 */
trait DMNContextPath {

}

/**
 * A processor of a DMNResult.  They must be Expressions so any children (e.g. serializers) may be resolved.
 * The expressions cannot implement CodgenFallback as eval will not be called.
 */
trait DMNResultProvider extends Expression {
  def process(dmnResult: DMNResult): Any

}

/**
 * Represents an executable DMN Model
 */
trait DMNModel {

  def evaluateAll(ctx: DMNContext): DMNResult

  def evaluateDecisionService(ctx: DMNContext, service: String): DMNResult

}

/**
 * Represents a repository of DMN, implementations must provide the SPI
 */
trait DMNRepository extends Serializable {
  /**
   * Throws DMNException if it can't be constructed
   * @param dmnFiles the complete set of DMNFiles to load
   * @param configuration options passed from the DMNExecution
   * @return
   */
  def dmnRuntimeFor(dmnFiles: Seq[DMNFile], configuration: DMNConfiguration): DMNRuntime

  /**
   * The engine may not support calling decision services, evaluation will fall back to "evaluateAll" on the model
   * @return
   */
  def supportsDecisionService: Boolean

  /**
   * Implementation specific providers, usually managed by the dmnEval function.
   * Note at time of calling the source Expression will not be resolved.
   *
   * @param inputField the configured input field from the DMNExecution
   * @param debug enable an implementation specific debug mode
   * @param configuration options passed from the DMNExecution
   * @return Either the provider type or throws for an unknown type
   */
  def providerForType(inputField: DMNInputField, debug: Boolean, configuration: DMNConfiguration): DMNContextProvider[_]

  /**
   * Implementation specific result provider
   * @param resultProviderType typically DDL of the result type.  This must be a struct with each of the possible decision names
   *                           entered against their types.
   * @param debug enable an implementation specific debug mode
   * @param configuration options passed from the DMNExecution
   * @return
   */
  def resultProviderForType(resultProviderType: String, debug: Boolean, configuration: DMNConfiguration): DMNResultProvider
}

/**
 * Represents an execution context for the DMN engine, the "input" for decisions
 */
trait DMNContext {
  /**
   * Implementation specific management of context
   *
   * @param path
   * @param data
   */
  def set(path: DMNContextPath, data: Any): Unit
}

/**
 * Represents a configured DMNRuntime
 */
trait DMNRuntime {

  /**
   * Throws DMNException if it cannot be constructed
   *
   * @param name
   * @param namespace
   * @return
   */
  def getModel(name: String, namespace: String): DMNModel

  def context(): DMNContext
}

trait DMNExpression extends Expression with CodegenFallback {

  def dmnRepository: DMNRepository

  def dmnFiles: Seq[DMNFile]
  def model: DMNModelService

  def configuration: DMNConfiguration

  def debug: Boolean

  lazy val resultProvider: DMNResultProvider = children.last.asInstanceOf[DMNResultProvider]

  @transient
  lazy val dmnRuntime: DMNRuntime = dmnRepository.dmnRuntimeFor(dmnFiles, configuration)

  @transient
  lazy val contextProviders: Seq[Expression] = children.dropRight(1).toVector

  @transient
  lazy val dmnModel = dmnRuntime.getModel(model.name, model.namespace) // the example pages show context outside of loops, we can re share it for a partition

  override def dataType: DataType = resultProvider.dataType

  def evaluate(ctx: DMNContext): DMNResult

  override def eval(input: InternalRow): Any = {
    val ctx = dmnRuntime.context()
    contextProviders.foreach { child =>
      val res = child.eval(input)
      if (res != null) {
        val (contextPath: DMNContextPath, testData: Any) = res
        ctx.set(contextPath, testData)
      }
    }

    val dmnRes = evaluate(ctx)
    val res = resultProvider.process(dmnRes)
    res
  }

  override def nullable: Boolean = resultProvider.nullable

}

/**
 * Represents any execution specific configuration
 * @param options an implementation specific encoding of options, provided to all repository functions and execution
 */
case class DMNConfiguration(options: String) extends Serializable

/**
 * Represents a complete set of information necessary for DMN execution
 * @param dmnFiles the dmn modules to be loaded
 * @param model the model to execute (with or without DecisionService) and the return processing
 * @param contextProviders the fields to inject into the DMN Context
 * @param options an implementation specific encoding of options, provided to all repository functions and execution
 */
case class DMNExecution(dmnFiles: Seq[DMNFile], model: DMNModelService,
                        contextProviders: Seq[DMNInputField], configuration: DMNConfiguration) extends Serializable

object DMN {

  lazy val dmnRepository: DMNRepository = {
    val serviceLoader = ServiceLoader.load(classOf[DMNRepository])
    val itr = serviceLoader.iterator()
    if (!itr.hasNext) {
      throw new DMNException("No ServiceProvider found for DMNRepository")
    }

    val repo = itr.next()
    repo
  }

  /**
   * Runs the dmnExecution with an optional implementation specific debug mode
   * @param dmnExecution The collection of dmn files, providers, model and options
   * @param debug An implementation specific debug flag passed to the DMNResultProvider
   * @return
   */
  def dmnEval(dmnExecution: DMNExecution, debug: Boolean = false): Column = {
    import dmnExecution._

    val children = contextProviders.map(p => dmnRepository.providerForType(p, debug, configuration))
    val resultProvider = dmnRepository.resultProviderForType(model.resultProvider, debug, configuration)

    if (model.service.isDefined && dmnRepository.supportsDecisionService)
      ShimUtils.column(DMNDecisionService(dmnRepository, dmnFiles, model, configuration, debug, children :+ resultProvider))
    else
      ShimUtils.column(DMNEvaluateAll(dmnRepository, dmnFiles, model, configuration, debug, children :+ resultProvider))
  }
}

private case class DMNDecisionService(dmnRepository: DMNRepository, dmnFiles: Seq[DMNFile], model: DMNModelService, configuration: DMNConfiguration, debug: Boolean, children: Seq[Expression]) extends DMNExpression {
  assert(children.dropRight(1).forall(_.isInstanceOf[DMNContextProvider[_]]), "Input children must be DMNContextProvider's")
  assert(children.last.isInstanceOf[DMNResultProvider], "Last child must be a DMNResultProvider")

  protected def withNewChildrenInternal(newChildren: scala.IndexedSeq[Expression]): Expression = copy(children = newChildren.toVector)

  override def evaluate(ctx: DMNContext): DMNResult = dmnModel.evaluateDecisionService(ctx, model.service.get)

}

private case class DMNEvaluateAll(dmnRepository: DMNRepository, dmnFiles: Seq[DMNFile], model: DMNModelService, configuration: DMNConfiguration, debug: Boolean, children: Seq[Expression]) extends DMNExpression {
  assert(children.dropRight(1).forall(_.isInstanceOf[DMNContextProvider[_]]), "Input children must be DMNContextProvider's")
  assert(children.last.isInstanceOf[DMNResultProvider], "Last child must be a DMNResultProvider")

  protected def withNewChildrenInternal(newChildren: scala.IndexedSeq[Expression]): Expression = copy(children = newChildren.toVector)

  override def evaluate(ctx: DMNContext): DMNResult = dmnModel.evaluateAll(ctx)
}
