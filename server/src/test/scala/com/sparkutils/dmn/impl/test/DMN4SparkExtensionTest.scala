package com.sparkutils.dmn.impl.test

import com.sparkutils.dmn.{DMN, DMNConfiguration, DMNExecution, DMNModelService}
import com.sparkutils.testing.ConnectWhenForced.someOrForcedConnect
import org.apache.spark.sql.functions.column
import org.scalatest.Matchers

import scala.collection.JavaConverters.asScalaIteratorConverter

class DMN4SparkExtensionTest extends SharedPureConnectTests with Matchers {

  test("call function via implementation") {
    val s = sparkSession
    import s.implicits._

    val dmnExecution = DMNExecution(
      Seq.empty,
      DMNModelService("", "", None, ""),
      Seq.empty,
      DMNConfiguration.empty,
    )

    val debug = false

    for {
      rcol <- someOrForcedConnect(DMN.dmnEval(dmnExecution, debug))
      df = Seq(
        ("foo", "bar"),
        ("foo", "baz")
      ).toDF
      rows = df
        .withColumn("result", rcol)
        .select(column("result").as[String])
        .toLocalIterator()
        .asScala
        .toSeq
    } {
      assert(rows.size === 2)
    }
  }

}
