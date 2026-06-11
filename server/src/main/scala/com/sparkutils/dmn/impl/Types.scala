package com.sparkutils.dmn.impl


import com.sparkutils.dmn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types._

case class DMNException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
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


trait UnaryDMNContextProvider[R] extends UnaryExpression with DMNContextProvider[R] {
  val providedType: Option[DataType]

  override def eval(input: InternalRow): Any = {
    val res = child.eval(input)
    nullSafeContextEval(child, res)
  }

  def equalsIgnoreNull(left: DataType, right: DataType): Boolean =
    (left, right) match {
      case (l: StructType, r: StructType) =>
        if (l.fields.length == r.fields.length)
          l.fields.zip(r.fields).forall {
            case (l, r) =>
              l.name == r.name && equalsIgnoreNull(l.dataType, r.dataType)
          }
        else
          false
      case (ArrayType(l, _), ArrayType(r, _)) => equalsIgnoreNull(l,r)
      case (MapType(lk, lv, _), MapType(rk, rv, _)) =>
        equalsIgnoreNull(lk,rk) && equalsIgnoreNull(lv,rv)
      case (l: DecimalType, r: DecimalType) => true // precision is hard and input isn't the same as Spark uses
      case _ => left == right
    }

  def verifyDataTypes(child: Expression): Unit = {
    if (child.resolved) {
      providedType.foreach{
        provided =>

          if (!equalsIgnoreNull(provided, child.dataType)) {
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


/**
 * Represents a DMN Result from an engine
 */
trait DMNResult

/**
 * A path along a DMN Context (e.g. an input variable location)
 */
trait DMNContextPath

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
