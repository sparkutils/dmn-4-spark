package com.sparkutils.dmn.impl

import com.sparkutils.dmn._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType}

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectStreamClass}
import scala.util.Try

object utils {

  /**
   * Called by implementations as a fallback extension method.  Expects a case class constructor with (contextPath: DMNContextPath, child: Expression)
   *
   * @param className fully formed class name
   * @param context   the implementation specific ContextPath
   * @param child     the input fieldExpression
   * @return
   */
  def loadUnaryContextProvider(className: String, context: DMNContextPath, child: Expression): DMNContextProvider[_] =
    Try(this.getClass.getClassLoader.loadClass(className)).flatMap[DMNContextProvider[_]] { (clazz: Class[_]) =>
      Try(clazz.getConstructor(classOf[DMNContextPath], classOf[Boolean], classOf[Expression], classOf[Option[DataType]]).newInstance(context, Boolean.box(true), child, None).asInstanceOf[DMNContextProvider[_]])
    }.fold(t => throw DMNException(s"Could not loadUnaryContextProvider $className", t), t => t)

  /**
   * Called by implementations as a fallback extension method.  Expects a single-arg class (debug: Boolean)
   *
   * @param className fully qualified className
   * @param debug     the single arg passed to the constructor of className
   * @return
   */
  def loadResultProvider(className: String, debug: Boolean): DMNResultProvider = (
    for {
      clazz <- Try(this.getClass.getClassLoader.loadClass(className))
      constructor <- Try(clazz.getConstructor(classOf[Boolean]))
      constructed <- Try(constructor.newInstance(debug.asInstanceOf[java.lang.Boolean]).asInstanceOf[DMNResultProvider])
      provider = constructed
    } yield
      provider
    ).fold(t => throw DMNException(s"Could not loadResultProvider $className", t), identity)


  /**
   * Provides support for ; configuration of name=value pairs.  No =value implies a flag and will be saved against empty string.
   * More than one = will simply ignore all further entries, no name will similarly be ignored.
   *
   * @param dmnConfiguration
   * @return
   */
  def configMap(dmnConfiguration: DMNConfiguration): Map[String, String] =
    dmnConfiguration.options.split(";").flatMap { e =>
      val p = e.split("=")

      p.length match {
        case 0 =>
          None
        case 1 =>
          Some(p(0), "")
        case _ =>
          Some(p(0), p(1))
      }
    }.toMap

  def getSparkClassLoader: ClassLoader = classOf[SparkSession].getClassLoader

  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  def deserializeImpl[T](in: Array[Byte]): T = {
    val os = new ObjectInputStream(new ByteArrayInputStream(in)) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] =
        Class.forName(desc.getName, false, getContextOrSparkClassLoader)
    }
    val suite = os.readObject()
    os.close()
    suite.asInstanceOf[T]
  }

  def deserialize(in: Array[Byte]): DMNExecution = deserializeImpl[DMNExecution](in)

  def getDMNExecutionFromExpression(expr: Expression): DMNExecution = {
    require(expr.isInstanceOf[Literal], "DMNExecution expression must be literal")
    val l = expr.asInstanceOf[Literal]
    require(l.dataType == BinaryType, "DMNExecution literal supports only BinaryType")
    deserialize(l.value.asInstanceOf[Array[Byte]])
  }

  def getDebugFlagFromExpression(expr: Expression): Boolean = {
    require(expr.isInstanceOf[Literal], "Debug Flag expression must be literal")
    val l = expr.asInstanceOf[Literal]
    require(l.dataType == BooleanType, "Debug Flag literal supports only BooleanType")
    l.value.asInstanceOf[Boolean]
  }
}
