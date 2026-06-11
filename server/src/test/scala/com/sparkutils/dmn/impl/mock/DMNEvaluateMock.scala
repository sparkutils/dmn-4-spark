package com.sparkutils.dmn.impl.mock

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

case class DMNEvaluateMock(children: Seq[Expression]) extends Expression {

  protected def withNewChildrenInternal(newChildren: scala.IndexedSeq[Expression]): Expression = copy(children = newChildren.toVector)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {

    val r = new DMNResultMock("evalModelName", "evalContext", "evalModel")

    new GenericInternalRow(
      Array[Any](
        UTF8String.fromString(r.dmnModelName),
        UTF8String.fromString(r.ctxName),
        UTF8String.fromString(r.model)
      )
    )
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.copy(code = code"""new DMNResultMock("genModelName", "genContext", "genModel")""")
  }

  override def dataType: DataType = StructType(Seq(
    StructField("dmnModelName", StringType),
    StructField("ctxName", StringType),
    StructField("model", StringType)
  ))
}