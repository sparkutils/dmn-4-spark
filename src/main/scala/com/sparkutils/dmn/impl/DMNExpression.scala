package com.sparkutils.dmn.impl

import com.sparkutils.dmn.{DMNConfiguration, DMNContext, DMNContextPath, DMNContextProvider, DMNFile, DMNModel, DMNModelService, DMNRepository, DMNResult, DMNResultProvider, DMNRuntime}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodeGenerator, CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.types.DataType

import scala.collection.immutable.Seq

/**
 * Injects the context from input providers, calls the engine, and invokes result providers
 */
private[dmn] trait DMNExpression extends Expression {

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

  def evaluateCodeGen(dmnModelName: String, ctxName: String, modelService: String): String

  override def eval(input: InternalRow): Any = {
    val ctx = dmnRuntime.context()
    contextProviders.foreach { child =>
      val res = child.eval(input)
      if (res != null) {
        val p = res.asInstanceOf[(DMNContextPath, AnyRef)]
        ctx.set(p._1, p._2)
      }
    }

    val dmnRes = evaluate(ctx)
    val res = resultProvider.process(dmnRes)
    res
  }

  override def nullable: Boolean = resultProvider.nullable

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.references += this
    val dmnExprClassName = classOf[DMNExpression].getName
    val dmnModelClassName = classOf[DMNModel].getName
    val dmnModelServiceClassName = classOf[DMNModelService].getName
    val dmnRuntimeClassName = classOf[DMNRuntime].getName
    val providerClassName = classOf[DMNResultProvider].getName
    val dmnContextPathClassName = classOf[DMNContextPath].getName

    val dmnExprIdx = ctx.references.size - 1
    val dmnModel = ctx.addMutableState(dmnModelClassName, ctx.freshName("dmnModel"),
      v => s"$v = ($dmnModelClassName)((($dmnExprClassName)references" +
        s"[$dmnExprIdx]).dmnModel());")
    val dmnModelService = ctx.addMutableState(dmnModelServiceClassName, ctx.freshName("modelService"),
      v => s"$v = ($dmnModelServiceClassName)((($dmnExprClassName)references" +
        s"[$dmnExprIdx]).model());")
    val dmnRuntime = ctx.addMutableState(dmnRuntimeClassName, ctx.freshName("runtime"),
      v => s"$v = ($dmnRuntimeClassName)((($dmnExprClassName)references" +
        s"[$dmnExprIdx]).dmnRuntime());")
    val resultProvider =
      if (this.resultProvider.isInstanceOf[CodegenFallback])
        ctx.addMutableState(providerClassName, ctx.freshName("runtime"),
        v => s"$v = ($providerClassName)((($dmnExprClassName)references" +
          s"[$dmnExprIdx]).resultProvider());")
      else
        ""

    val ctxv = ctx.addMutableState(classOf[DMNContext].getName, "ctx")

    def gen(i: Int): Block = {
      val child = contextProviders(i).genCode(ctx)
      val boxed = CodeGenerator.boxedType(contextProviders(i).asInstanceOf[DMNContextProvider[_]].resultType.getName)
      val code = s"$ctxv.set(($dmnContextPathClassName)${child.value}[0], ($boxed)${child.value}[1]);"
      if (contextProviders(i).nullable)
        code"""
          ${child.code}
          if (!${child.isNull}) {
            $code
          }
        """
      else
        code"""
          ${child.code}
          $code
        """
    }

    val allContexts = contextProviders.indices.foldLeft(code""){case (b, i) =>
      code"\n$b\n${gen(i)}"
    }
    val javaType = CodeGenerator.javaType(dataType)
    val boxed = CodeGenerator.boxedType(dataType)

    val dmnResult = ctx.addMutableState(classOf[DMNResult].getName, "dmnResult")

    DMNExpression.runtimeVar.set(dmnResult)

    // if the result provider can gen code, use it, otherwise "eval" via reference
    val (resultProviderInit: Block, resultProviderCode) =
      if (this.resultProvider.isInstanceOf[CodegenFallback])
        (code"", s"$resultProvider.process($dmnResult);")
      else {
        val c = this.resultProvider.genCode(ctx)
        (c.code, s"${c.value};")
      }

    DMNExpression.runtimeVar.remove()

    ev.copy(code =
      code"""
        $ctxv = $dmnRuntime.context();
        $allContexts

        $dmnResult = ${evaluateCodeGen(dmnModel, ctxv, dmnModelService)}
        $resultProviderInit
        $javaType ${ev.value} = ($boxed) $resultProviderCode
        boolean ${ev.isNull} = ${ev.value} == null;
          """)
  }
}

object DMNExpression {
  val runtimeVar = new ThreadLocal[String] {
    override def initialValue(): String = ""
  }
}

private[dmn] case class DMNDecisionService(dmnRepository: DMNRepository, dmnFiles: Seq[DMNFile], model: DMNModelService, configuration: DMNConfiguration, debug: Boolean, children: Seq[Expression]) extends DMNExpression {
  assert(children.dropRight(1).forall(c => c.isInstanceOf[DMNContextProvider[_]] || (c.isInstanceOf[Literal] && c.asInstanceOf[Literal].value == null) ), "Input children must be DMNContextProvider's")
  assert(children.last.isInstanceOf[DMNResultProvider], "Last child must be a DMNResultProvider")

  protected def withNewChildrenInternal(newChildren: scala.IndexedSeq[Expression]): Expression = copy(children = newChildren.toVector)

  override def evaluate(ctx: DMNContext): DMNResult = dmnModel.evaluateDecisionService(ctx, model.service.get)

  override def evaluateCodeGen(dmnModelName: String, ctxName: String, model: String): String = s"$dmnModelName.evaluateDecisionService($ctxName, (String)$model.service().get());"
}

private[dmn] case class DMNEvaluateAll(dmnRepository: DMNRepository, dmnFiles: Seq[DMNFile], model: DMNModelService, configuration: DMNConfiguration, debug: Boolean, children: Seq[Expression]) extends DMNExpression {
  assert(children.dropRight(1).forall(c => c.isInstanceOf[DMNContextProvider[_]] || (c.isInstanceOf[Literal] && c.asInstanceOf[Literal].value == null) ), "Input children must be DMNContextProvider's")
  assert(children.last.isInstanceOf[DMNResultProvider], "Last child must be a DMNResultProvider")

  protected def withNewChildrenInternal(newChildren: scala.IndexedSeq[Expression]): Expression = copy(children = newChildren.toVector)

  override def evaluate(ctx: DMNContext): DMNResult = dmnModel.evaluateAll(ctx)

  override def evaluateCodeGen(dmnModelName: String, ctxName: String, model: String): String = s"$dmnModelName.evaluateAll($ctxName);"
}
