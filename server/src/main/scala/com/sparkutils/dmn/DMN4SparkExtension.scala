package com.sparkutils.dmn

import com.sparkutils.dmn.impl.{DMNExpressionImpl, utils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ShimUtils, SparkSessionExtensions}

class DMN4SparkExtension extends ((SparkSessionExtensions) => Unit) with Logging {

  override def apply(v1: SparkSessionExtensions): Unit = {

    ShimUtils.registerFunctionViaExtension(v1)("dmnEval", args => {
      require(args.nonEmpty, "not enough arguments for dmnEval")

      val (dmnExecution, debug) = args match {
        case Seq(dmnExecutionExpr, debugExpr, _*) =>
          utils.getDMNExecutionFromExpression(dmnExecutionExpr) -> utils.getDebugFlagFromExpression(debugExpr)
        case Seq(dmnExecutionExpr, _*) =>
          utils.getDMNExecutionFromExpression(dmnExecutionExpr) -> false
      }
      DMNExpressionImpl.dmnEvalExpr(dmnExecution, debug)(identity)
    })
  }
}