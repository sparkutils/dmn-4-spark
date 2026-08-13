package com.sparkutils.dmn.impl.mock

import com.sparkutils.dmn._
import com.sparkutils.dmn.impl._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class DMNContextMock extends DMNContext {

  override def set(path: DMNContextPath, data: Any): Unit = {}

}

case class DMNResultMock(dmnModelName: String, ctxName: String, model: String) extends DMNResult {

  def this() = {
    this("empty", "empty", "empty")
  }

}

class DMNModelMock extends DMNModel {

  override def evaluateAll(ctx: DMNContext): DMNResult = new DMNResultMock("empty", "empty", ctx.getClass.getName)

  override def evaluateDecisionService(ctx: DMNContext, service: String): DMNResult = new DMNResultMock("empty", service, ctx.getClass.getName)
}

class DMNRuntimeMock extends DMNRuntime {

  /**
   * Throws DMNException if it cannot be constructed
   *
   * @param name
   * @param namespace
   * @return
   */
  override def getModel(name: String, namespace: String): DMNModel = new DMNModelMock

  override def context(): DMNContext = new DMNContextMock
}

case class DMNResultProviderMock(children: Seq[Expression]) extends DMNResultProvider {

  override def process(r: DMNResult): Any = {
    r match {
      case r:DMNResultMock =>
//        new GenericInternalRow(
//          Array[Any](
//            UTF8String.fromString(r.dmnModelName),
//            UTF8String.fromString(r.ctxName),
//            UTF8String.fromString(r.model)
//          )
//        )
        UTF8String.fromString(s"""
          |${r.dmnModelName},
          |${r.ctxName},
          |${r.model}""".stripMargin
        )
      case other =>
//        new GenericInternalRow(
//        Array[Any](
//          UTF8String.fromString(other.getClass.getName),
//          UTF8String.fromString("unable to parse"),
//          UTF8String.fromString(other.toString)
//        )
//      )
        UTF8String.fromString(s"""
           |${other.getClass.getName},
           |${"unable to parse"},
           |${other.toString}""".stripMargin
        )
    }

  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = ???

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ev

  override def dataType: DataType = StringType

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }
}
