package com.sparkutils.dmn.impl

import com.sparkutils.dmn.{DMNContextPath, DMNContextProvider}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.unsafe.types.UTF8String

import java.io.{ByteArrayInputStream, InputStreamReader}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.reflect.{ClassTag, classTag}

/**
 * provides UTF8 to inputstream conversion
 * @tparam R
 */
trait UTF8StringInputStreamContextProvider[R] extends UnaryExpression with DMNContextProvider[R] {
  /**
   * Eval path to process the input stream
   */
  def readValue(str: InputStreamReader): R

  /**
   * Codegen gen path
   * @param inputStreamReaderVal the variable name of the inputStreamReader
   * @param ctx
   * @param exprCode pre-prepared variable for response
   * @return
   */
  def codeGen(inputStreamReaderVal: String, ctx: CodegenContext): String

  override def nullSafeEval(input: Any): Any = {
    val i = input.asInstanceOf[UTF8String]
    val bb = i.getByteBuffer // handles the size of issues
    assert(bb.hasArray)

    val bain = new ByteArrayInputStream(
      bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())

    val istr = new InputStreamReader(bain, StandardCharsets.UTF_8)

    // assuming it's quicker than using classes
    val r = // bytes is a couple of percents slower mapper.readValue(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining(), classOf[java.util.Map[String, Object]])
      readValue(istr)

    (contextPath, r)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val (ctxClassName, contextPath) = genContext(ctx)
    val rClassName = resultType.getName

    nullSafeCodeGen(ctx, ev, childName =>
    s"""
      ${classOf[ByteBuffer].getName} bb = ${childName}.getByteBuffer();
      ${classOf[ByteArrayInputStream].getName} bain = new ${classOf[ByteArrayInputStream].getName}(
        bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
      ${classOf[InputStreamReader].getName} istr = new ${classOf[InputStreamReader].getName}(bain, ${classOf[StandardCharsets].getName}.UTF_8);

      try {
        ${ev.value} = new scala.Tuple2<$ctxClassName, $rClassName>( $contextPath,
          ${codeGen("istr", ctx)});
      } catch(java.io.IOException e) {
        ${ev.isNull} = true;
      }
    """
    )
  }
}

case class StringContextProvider(contextPath: DMNContextPath, child: Expression) extends UnaryExpression with DMNContextProvider[String] {

  def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)


  override def nullSafeEval(input: Any): Any = {
    (contextPath, input.toString)
  }

  /**
   * Result class type
   */
  override val resultType: Class[String] = classOf[String]

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val (contextClassName, contextPath) = genContext(ctx)

    defineCodeGen(ctx, ev, input => s"new scala.Tuple2<$contextClassName, String>($contextPath, $input.toString())")
  }
}

case class SimpleContextProvider[T: ClassTag](contextPath: DMNContextPath, child: Expression, converter: Option[(Any => T, (CodegenContext, String) => String)] = None) extends UnaryExpression with DMNContextProvider[T] {

  def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  override def nullSafeEval(input: Any): Any =
    (contextPath, converter.map(f => f._1(input)).getOrElse(input))

  /**
   * Result class type
   */
  override val resultType: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val (contextClassName, contextPath) = genContext(ctx)
    val rClassName = resultType.getName

    val boxed = CodeGenerator.boxedType(rClassName)

    nullSafeCodeGen(ctx, ev, input => s"""
      $rClassName res = ${
      converter.fold(input)( p =>
        p._2(ctx, input)
      )
    };
      ${ev.value} = new scala.Tuple2<$contextClassName, String>($contextPath, ($boxed) res);
    """)
  }
}
