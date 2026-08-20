package com.sparkutils.dmn.impl.test

import com.sparkutils.dmn.DMNExecution
import com.sparkutils.testing.TestRunner
import com.sparkutils.testing.TestUtilsEnvironment.setupDefaultsViaCurrentSession

object DMNTestRunner extends TestRunner {

  val packageName: String = "com.sparkutils.dmn"

  val projectName: String = "DMN4Spark"

  override val classLoader: ClassLoader = classOf[DMNExecution].getClassLoader

  // when on Fabric or Databricks disables cluster tests
  setupDefaultsViaCurrentSession()

  def main(args: Array[String]): Unit = test(args)
}