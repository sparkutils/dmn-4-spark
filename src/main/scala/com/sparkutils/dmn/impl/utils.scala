package com.sparkutils.dmn.impl

import com.sparkutils.dmn.{DMNConfiguration, DMNContextPath, DMNContextProvider, DMNException, DMNResultProvider}
import org.apache.spark.sql.catalyst.expressions.Expression

import scala.util.Try

object utils {

  /**
   * Called by implementations as a fallback extension method.  Expects a case class constructor with (contextPath: DMNContextPath, child: Expression)
   * @param className fully formed class name
   * @param context the implementation specific ContextPath
   * @param child the input fieldExpression
   * @return
   */
  def loadUnaryContextProvider(className: String, context: DMNContextPath, child: Expression): DMNContextProvider[_] =
    Try(this.getClass.getClassLoader.loadClass(className)).flatMap[DMNContextProvider[_]]{ (clazz: Class[_]) =>
      Try(clazz.getConstructor(classOf[DMNContextPath], classOf[Expression]).newInstance(context, child).asInstanceOf[DMNContextProvider[_]])
    }.fold(t => throw DMNException(s"Could not loadUnaryContextProvider $className", t), t => t)

  /**
   * Called by implementations as a fallback extension method.  Expects a single-arg class (debug: Boolean)
   * @param className fully qualified className
   * @param debug the single arg passed to the constructor of className
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
   * @param dmnConfiguration
   * @return
   */
  def configMap(dmnConfiguration: DMNConfiguration): Map[String,String] =
    dmnConfiguration.options.split(";").flatMap{e =>
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

}
