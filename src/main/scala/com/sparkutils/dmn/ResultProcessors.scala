package com.sparkutils.dmn

import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType}

trait SeqOfBools extends DMNResultProvider {

  override def dataType: DataType = ArrayType(BooleanType)

  override def nullable: Boolean = true
}
