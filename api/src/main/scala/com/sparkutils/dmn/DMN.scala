package com.sparkutils.dmn

import org.apache.spark.sql.Column

import javax.annotation.Nullable

object DMN {

  /**
   * @deprecated
   * migrate to [[DMN.dmnEvalOpt]]
   */
  @Nullable
  @deprecated("dmnEval may return null, use dmnEvalOpt", "06-14-2026")
  def dmnEval(dmnExecution: DMNExecution, debug: Boolean = false): Column = ???

  /**
   * Runs the dmnExecution with an optional implementation specific debug mode.  If a specific runtime is provided in the [[DMNConfiguration]]
   * an attempt to load it will be made, reverting to the first found DMNRepository SPI implementation.
   *
   * @param dmnExecution The collection of dmn files, providers, model and options
   * @param debug        An implementation specific debug flag passed to execution flow
   * @return
   */
  def dmnEvalOpt(dmnExecution: DMNExecution, debug: Boolean = false): Option[Column] = ???

}