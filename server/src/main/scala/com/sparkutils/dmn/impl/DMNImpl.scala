package com.sparkutils.dmn.impl

import com.sparkutils.dmn.DMNExecution
import org.apache.spark.sql.{Column, ShimUtils}

object DMNImpl {

  def eval(dmnExecution: DMNExecution, debug: Boolean = false): Option[Column] = {
    Some(DMNExpressionImpl.dmnEvalExpr(dmnExecution, debug)(ShimUtils.column))
  }

}