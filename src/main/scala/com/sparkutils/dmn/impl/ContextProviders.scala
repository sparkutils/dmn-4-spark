package com.sparkutils.dmn.impl

import com.sparkutils.dmn.{DMNContextPath, UnaryDMNContextProvider}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.{ClassTag, classTag}

/**
 * provides UTF8 to IO operation on string conversion.  IO Exceptions are treated as null
 * @tparam R
 */
trait StringWithIOProcessorContextProvider[R] extends UnaryDMNContextProvider[R] {
  /**
   * Eval path to process the input stream
   */
  def readValue(str: String): R

  /**
   * Codegen path, should be a one liner without semi-colon, if a more complex processing is required override doGenCode instead.
   * @param strVal the variable name of the inputStreamReader
   * @param ctx
   * @return
   */
  def codeGen(strVal: String, ctx: CodegenContext): String

  override def nullSafeContextEval(input: Any): Any = {
    val i = input.asInstanceOf[UTF8String]
    val istr = i.toString

    // assuming it's quicker than using classes
    val r = // bytes is a couple of percents slower mapper.readValue(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining(), classOf[java.util.Map[String, Object]])
      try {
        readValue(istr)
      } catch {
        case _: java.io.IOException => null
      }

    r
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val (_, contextPath) = genContext(ctx)

    val istr = ctx.freshName("istr")

    nullSafeContextCodeGen(child, ctx, ev, contextPath, childName =>
    s"""
      String $istr = $childName.toString();
      try {
        ${ev.value}[0] = $contextPath;
        ${ev.value}[1] = ${codeGen(istr, ctx)};
      } catch(java.io.IOException e) {
        ${ev.isNull} = true;
      }
    """
    )
  }
}

case class StringContextProvider(contextPath: DMNContextPath, stillSetWhenNull: Boolean, child: Expression) extends UnaryDMNContextProvider[String] {

  def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)


  override def nullSafeContextEval(input: Any): Any =
    input.toString

  /**
   * Result class type
   */
  override val resultType: Class[String] = classOf[String]

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val (_, contextPath) = genContext(ctx)

    nullSafeContextCodeGen(child, ctx, ev, contextPath, input => s"""
      ${ev.value}[0] = $contextPath;
      ${ev.value}[1] = $input.toString();
      """)
  }
}

case class SimpleContextProvider[T: ClassTag](contextPath: DMNContextPath, stillSetWhenNull: Boolean, child: Expression, converter: Option[(Any => T, (CodegenContext, String) => String)] = None) extends UnaryDMNContextProvider[T] {

  def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  override def nullSafeContextEval(input: Any): Any =
    converter.map(f => f._1(input)).getOrElse(input)

  /**
   * Result class type
   */
  override val resultType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val (_, contextPath) = genContext(ctx)
    val rClassName = resultType.getName

    val boxed = CodeGenerator.boxedType(rClassName)

    val res = ctx.freshName("res")

    nullSafeContextCodeGen(child, ctx, ev, contextPath, input => s"""
      $rClassName $res = ${
      converter.fold(input)( p =>
        p._2(ctx, input)
      )
    };
      ${ev.value}[0] = $contextPath;
      ${ev.value}[1] = ($boxed) $res;
    """)
  }
}
