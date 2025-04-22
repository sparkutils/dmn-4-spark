package com.sparkutils.dmn.impl

import com.sparkutils.dmn.{DMNContextPath, DMNContextProvider}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.unsafe.types.UTF8String

import java.io.{ByteArrayInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

/**
 * provides UTF8 to inputstream conversion
 * @tparam R
 */
trait UTF8StringInputStreamContextProvider[R] extends UnaryExpression with CodegenFallback with DMNContextProvider[R] {

  def readValue(str: InputStreamReader): R

  override def nullSafeEval(input: Any): Any = {
    val i = input.asInstanceOf[UTF8String]
    val bb = i.getByteBuffer // handles the size of issues
    assert(bb.hasArray)

    val bain = new ByteArrayInputStream(
      bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())

    val str = new InputStreamReader(bain, StandardCharsets.UTF_8)

    // assuming it's quicker than using classes
    val testData = // bytes is a couple of percents slower mapper.readValue(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining(), classOf[java.util.Map[String, Object]])
      readValue(str)

    (contextPath, testData)
  }

}

case class StringContextProvider(contextPath: DMNContextPath, child: Expression) extends UnaryExpression with DMNContextProvider[String] with CodegenFallback {

  def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)


  override def nullSafeEval(input: Any): Any = {
    (contextPath, input.toString)
  }

}

case class SimpleContextProvider[T](contextPath: DMNContextPath, child: Expression, converter: Option[Any => T] = None) extends UnaryExpression with DMNContextProvider[T] with CodegenFallback {

  def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

  override def nullSafeEval(input: Any): Any = {
    // TODO - compile the option out
    (contextPath, converter.map(f => f(input)).getOrElse(input))
  }

}
