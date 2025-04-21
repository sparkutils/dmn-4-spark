package com.sparkutils.dmn

import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{DataType, ObjectType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.{ByteArrayInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets


trait JSONContext extends UnaryExpression with CodegenFallback with DMNContextProvider {
  val contextPath: DMNContextPath

  val resultClass: Class[_]

  def readValue(str: InputStreamReader): Any

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

  override def dataType: DataType = ObjectType(resultClass)
}
