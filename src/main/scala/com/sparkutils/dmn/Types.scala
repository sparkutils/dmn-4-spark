package com.sparkutils.dmn

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.apache.spark.sql.{Column, ShimUtils, functions}

import java.util.ServiceLoader
import scala.collection.immutable.Seq
import impl.{DMNDecisionService, DMNEvaluateAll}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

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
 * @param stillSetWhenNull specifies if, when the fieldExpression is null what should happen to the contextPath.
 *                         The default value of true specifies that a context entry should be made with a null value
 */
case class DMNInputField(fieldExpression: String, providerType: String, contextPath: String, stillSetWhenNull: Boolean = true) extends Serializable {
  /**
   * Implementations are free to chose a different parsing approach for the fieldExpression
   * @return
   */
  def defaultExpr: Expression = ShimUtils.expression( functions.expr(fieldExpression) )
}

trait UnaryDMNContextProvider[R] extends UnaryExpression with DMNContextProvider[R] {
  val providedType: Option[DataType]

  override def eval(input: InternalRow): Any = {
    val res = child.eval(input)
    nullSafeContextEval(child, res)
  }

  def verifyDataTypes(child: Expression): Unit = {
    if (child.resolved) {
      providedType.foreach{
        provided =>
          if (provided != child.dataType) {
            throw new DMNException(s"Provided type '${provided.sql}' for context '$contextPath' does not match the child expression type '${child.dataType.sql}'")
          }
      }
    }
  }
}

/**
 * A provider for DMN Context injection.  The resulting value from codegen must be an Object[2] array, ideally as mutable state.
 */
trait DMNContextProvider[R] extends Expression {
  val contextPath: DMNContextPath
  val stillSetWhenNull: Boolean

  /**
   * Result class type
   */
  val resultType: Class[R]


  override def dataType: DataType = ObjectType(classOf[Array[Object]])

  /**
   * When stillSetWhenNull is true we cannot allow folding to null
   * @return
   */
  override def foldable: Boolean = super.foldable && !stillSetWhenNull

  /**
   * Typical implementation function for the provider logic, only the result need be provided
   * @param input
   * @return the result of the input processing without context
   */
  protected def nullSafeContextEval(input: Any): Any

  /**
   * Processes child expression eval results, provided by UnaryDMNContextProvider
   * @param child not used by the base implementation, provided for possible overriding implementations
   * @param input the result of child eval processing
   * @return
   */
  protected def nullSafeContextEval(child: Expression, input: Any): Any =
    (stillSetWhenNull, input) match {
      case (true, null) => Array(contextPath, null)
      case (false, null) => null
      case (_, i) => Array(contextPath, nullSafeContextEval(i))
    }

  /**
   * Utility function for single children codegen, pre-prepares the result array as mutable state taking stillSetWhenNull
   * behaviour into account.
   *
   * @param f function that accepts the non-null evaluation result name of child and returns Java
   *          code to compute the output.
   */
  protected def nullSafeContextCodeGen(child: Expression,
                                 ctx: CodegenContext,
                                 ev: ExprCode,
                                 contextPath: String,
                                 f: String => String): ExprCode = {

    val childGen = child.genCode(ctx)
    val resultCode = f(childGen.value)

    val cRes = ctx.freshName("contextResult")
    val res = ctx.addMutableState("Object[]", cRes, v => s" $v = new Object[2];", useFreshName = false)

    // only in this combo should null be returned
    if (nullable && !stillSetWhenNull) {
      val nullSafeEval = ctx.nullSafeExec(child.nullable, childGen.isNull)(resultCode)
      ev.copy(code = code"""
        Object[] ${ev.value} = $res;
        ${childGen.code}
        boolean ${ev.isNull} = ${childGen.isNull};
        $nullSafeEval
      """)
    } else {
      ev.copy(code = code"""
        Object[] ${ev.value} = $res;
        boolean ${ev.isNull} = false;
        ${childGen.code}
        ${
          if (stillSetWhenNull)
            code"""
              if (${childGen.isNull}) {
                ${ev.value}[0] = $contextPath;
                ${ev.value}[1] = null;
              } else {
                $resultCode
              }
                """
          else
            code"""
              $resultCode
                """
        }
        """)
    }
  }

  /**
   * Returns (DMNContext class name, contextPath Variable)
   */
  def genContext(ctx: CodegenContext): (String, String) = {
    ctx.references += this
    val dmnProviderClassName = classOf[DMNContextProvider[_]].getName
    val dmnContextClassName = classOf[DMNContextPath].getName

    val dmnExprIdx = ctx.references.size - 1
    val contextPath = ctx.addMutableState(dmnContextClassName, ctx.freshName("contextPath"),
      v => s"$v = ($dmnContextClassName)((($dmnProviderClassName)references" +
        s"[$dmnExprIdx]).contextPath());")
    (dmnContextClassName, contextPath)
  }
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
 * The expressions should implement CodgenFallback if they cannot perform codegen (although only process will be called).
 * If codegen is possible they must accept a local variable 'dmnResult'
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
   * Runs the dmnExecution with an optional implementation specific debug mode.  If a specific runtime is provided in the DMNConfiguration
   * an attempt to load it will be made, reverting to the first found DMNRepository SPI implementation.
   * @param dmnExecution The collection of dmn files, providers, model and options
   * @param debug An implementation specific debug flag passed to the DMNResultProvider
   * @return
   */
  def dmnEval(dmnExecution: DMNExecution, debug: Boolean = false): Column = {
    import dmnExecution._

    val repo = configuration.runtime.flatMap{r =>
      if (r == dmnRepository.getClass.getName)
        // we already have it
        Some(dmnRepository)
      else
        ServiceLoader.load(classOf[DMNRepository]).asScala.find(_.getClass.getName == r)
    }.getOrElse(dmnRepository)

    val children = contextProviders.map(p => repo.providerForType(p, debug, configuration))
    val resultProvider = repo.resultProviderForType(model.resultProvider, debug, configuration)

    if (model.service.isDefined && repo.supportsDecisionService)
      ShimUtils.column(DMNDecisionService(repo, dmnFiles, model, configuration, debug, children :+ resultProvider))
    else
      ShimUtils.column(DMNEvaluateAll(repo, dmnFiles, model, configuration, debug, children :+ resultProvider))
  }
}
