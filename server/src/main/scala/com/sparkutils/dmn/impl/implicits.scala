package com.sparkutils.dmn.impl

import com.sparkutils.dmn.DMNInputField
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{ShimUtils, functions}

object implicits {

  implicit class DMNInputFieldExt(val underlying: DMNInputField) {
    /**
     * Implementations are free to choose a different parsing approach for the fieldExpression
     * @return
     */
    def defaultExpr: Expression = ShimUtils.expression( functions.expr(underlying.fieldExpression) )

  }

}
