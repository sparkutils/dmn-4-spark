package com.sparkutils.dmn.impl

import com.sparkutils.dmn.DMNExecution
import org.apache.spark.sql.Column

object DMNImpl {

  def eval(dmnExecution: DMNExecution, debug: Boolean = false): Option[Column] = None

}