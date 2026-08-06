package com.sparkutils.dmn.impl.test

import com.sparkutils.dmn.impl.mock.DMNContextMock
import com.sparkutils.dmn.{DMN, DMNConfiguration, DMNExecution, DMNModelService}
import com.sparkutils.testing.ConnectWhenForced.someOrForcedConnect
import org.apache.spark.sql.ShimUtils
import org.apache.spark.sql.functions.{column, lit}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.Matchers

class DMN4SparkExtensionTest extends SharedPureConnectTests with Matchers {

  test("call function via implementation") { evalCodeGens {
    val s = sparkSession
    import s.implicits._

    val dmnExecution = DMNExecution(
      Seq.empty,
      DMNModelService("name", "", None, ""),
      Seq.empty,
      DMNConfiguration.empty,
    )

    val debug = false

    val dcol = someOrForcedConnect(DMN.dmnEval(dmnExecution, debug)).
        getOrElse(ShimUtils.callFunction("dmnEval", lit(DMNExecution.serialize(dmnExecution)), lit(debug)))

    val df = Seq(
        ("foo", "bar"),
        ("foo", "baz")
      ).toDF

    val rows = df
        .withColumn("result", dcol)
        .select(column("result").as[String])
        .collect

    rows shouldBe Seq.fill(2)(
      s"""
         |empty,
         |empty,
         |${classOf[DMNContextMock].getName}""".stripMargin
    )
  } }

}
