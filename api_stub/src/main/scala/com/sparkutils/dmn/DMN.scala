package com.sparkutils.dmn

import org.apache.spark.sql.Column

object DMN {

  def dmnEval(dmnExecution: DMNExecution, debug: Boolean = false): Column = dmnEvalOpt(dmnExecution, debug).orNull

  def dmnEvalOpt(dmnExecution: DMNExecution, debug: Boolean = false): Option[Column] = None

}