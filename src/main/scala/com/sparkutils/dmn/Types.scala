package com.sparkutils.dmn

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import org.apache.spark.sql.{Column, ShimUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.{ByteArrayInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
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
 */
case class DMNModelService(name: String, namespace: String, service: Option[String]) extends Serializable

trait DMNContextProvider

case class DMNException(message: String, cause: Throwable) extends RuntimeException(message, cause)

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
 * A processor of a DMNResult
 */
trait DMNResultProvider {
  def process(dmnResult: DMNResult): Any

  def dataType: DataType

  def nullable: Boolean
}

/**
 * Represents an executable DMN Model
 */
trait DMNModel {

  def evaluateAll(ctx: DMNContext): DMNResult

  def evaluateDecisionService(ctx: DMNContext, service: String): DMNResult

}

/**
 * Represents a repository of DMN, this is the actual root provider
 */
trait DMNRepository extends Serializable {
  /**
   * Throws DMNException if it can't be constructed
   * @param dmnFiles
   * @return
   */
  def dmnRuntimeFor(dmnFiles: Seq[DMNFile]): DMNRuntime

  /**
   * The engine may not support calling decision services, evaluation will fall back to "evaluateAll" on the model
   * @return
   */
  def supportsDecisionService: Boolean
}

trait DMNContext {
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

  def resultProvider: DMNResultProvider

  @transient
  lazy val dmnRuntime: DMNRuntime = dmnRepository.dmnRuntimeFor(dmnFiles)

  //@transient
  //lazy val ctx = dmnRuntime.newContext() // the example pages show context outside of loops, we can re share it for a partition

  @transient
  lazy val dmnModel = dmnRuntime.getModel(model.name, model.namespace) // the example pages show context outside of loops, we can re share it for a partition

  override def dataType: DataType = resultProvider.dataType

  def evaluate(ctx: DMNContext): DMNResult

  override def eval(input: InternalRow): Any = {
    val ctx = dmnRuntime.context()
    children.foreach { child =>
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

object DMN {
  def dmn(dmnRepository: DMNRepository, dmnFiles: Seq[DMNFile], model: DMNModelService, children: Seq[Column], resultProvider: DMNResultProvider): Column =
    if (model.service.isDefined && dmnRepository.supportsDecisionService)
      ShimUtils.column(DMNDecisionService(dmnRepository, dmnFiles, model, children.map(ShimUtils.expression), resultProvider))
    else
      ShimUtils.column(DMNEvaluateAll(dmnRepository, dmnFiles, model, children.map(ShimUtils.expression), resultProvider))
}

private case class DMNDecisionService(dmnRepository: DMNRepository, dmnFiles: Seq[DMNFile], model: DMNModelService, children: Seq[Expression], resultProvider: DMNResultProvider) extends DMNExpression {
  assert(children.forall(_.isInstanceOf[DMNContextProvider]), "Input children must be DMNContextProvider's")

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

  override def evaluate(ctx: DMNContext): DMNResult = dmnModel.evaluateDecisionService(ctx, model.service.get)
}

private case class DMNEvaluateAll(dmnRepository: DMNRepository, dmnFiles: Seq[DMNFile], model: DMNModelService, children: Seq[Expression], resultProvider: DMNResultProvider) extends DMNExpression {
  assert(children.forall(_.isInstanceOf[DMNContextProvider]), "Input children must be DMNContextProvider's")

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

  override def evaluate(ctx: DMNContext): DMNResult = dmnModel.evaluateAll(ctx)
}
