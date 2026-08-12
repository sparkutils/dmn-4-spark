package com.sparkutils.dmn

import com.sparkutils.dmn.impl.DMNImpl
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, ShimUtils}

object DMN {

  /**
   * Runs the dmnExecution with an optional implementation specific debug mode.  If a specific runtime is provided in the [[DMNConfiguration]]
   * an attempt to load it will be made, reverting to the first found DMNRepository SPI implementation.
   *
   * @param dmnExecution The collection of dmn files, providers, model and options
   * @param debug        An implementation specific debug flag passed to execution flow
   * @return
   */
  def dmnEval(dmnExecution: DMNExecution, debug: Boolean = false): Column = {
    DMNImpl
      .eval(dmnExecution, debug)
      .getOrElse(ShimUtils.callFunction("dmnEval", lit(DMNExecution.serialize(dmnExecution)), lit(debug)))
  }

}