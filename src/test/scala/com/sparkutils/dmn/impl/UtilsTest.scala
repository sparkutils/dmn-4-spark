package com.sparkutils.dmn.impl

import com.sparkutils.dmn._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{LeafExpression, Literal}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType
import org.scalatest.{FunSuite, Matchers}

case class ContextPath() extends DMNContextPath {

}

case class SeqB(debug: Boolean) extends LeafExpression with DMNResultProvider {

  override def process(dmnResult: DMNResult): Any = ???

  override def eval(input: InternalRow): Any = ???

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ???

  override def nullable: Boolean = ???

  override def dataType: DataType = ???
}

class UtilsTest extends FunSuite with Matchers {

  test("Instantiating contextproviders") {
    val c = ContextPath()
    val l = Literal(1L)
    val exp = StringContextProvider(c, l)

    val r = utils.loadUnaryContextProvider(classOf[StringContextProvider].getName, c, l)
    r shouldBe exp
  }

  test("Instantiating bad classnames should fail") {
    val claz = "I.don.t.exist"
    try {
      utils.loadUnaryContextProvider(claz, null, null)
    } catch {
      case e: DMNException => e.message shouldBe s"Could not loadUnaryContextProvider $claz"
      case t: Throwable => throw t
    }
  }

  test("Instantiating resultproviders") {
    val exp = SeqB(true)

    val r = utils.loadResultProvider(classOf[SeqB].getName, true)
    r shouldBe exp
  }

  test("Instantiating bad resultproviders should fail") {
    val claz = "I.don.t.exist"
    try {
      utils.loadResultProvider(claz, false)
    } catch {
      case e: DMNException => e.message shouldBe s"Could not loadResultProvider $claz"
      case t: Throwable => throw t
    }
  }
}
