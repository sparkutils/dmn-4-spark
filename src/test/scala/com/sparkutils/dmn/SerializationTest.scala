package com.sparkutils.dmn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}
import org.scalatest.{FunSuite, Matchers}

class SerializationTest extends FunSuite with Matchers {

  lazy val sparkSession = {
    val s = SparkSession.builder().config("spark.master", "local[*]").config("spark.ui.enabled", false).getOrCreate()
    s.sparkContext.setLogLevel("ERROR") // set to debug to get actual code lines etc.
    s
  }

  val pseq1 = Seq(
    DMNInputField("a", "b", "c"),
    DMNInputField("d", "e", "f"),
    DMNInputField("g", "h", "i"),
    DMNInputField("j", "k", "l")
  )

  val pseq2 = Seq(
    DMNInputField("a1", "b1", "c1"),
    DMNInputField("d1", "e1", "f1"),
    DMNInputField("g1", "h1", "i1"),
    DMNInputField("j1", "k1", "l1")
  )

  val providers =
    pseq1.map(Versioned(1, 1, _)) ++
      pseq2.map(Versioned(3, 1, _))

  val m1 = DMNModelService("n", "ns", Some("DS"), "r")
  val m2 = DMNModelService("n2", "ns2", Some("DS2"), "r2")
  val models = Seq(
    Versioned(1, 1, m1),
    Versioned(3, 1, m2)
  )

  val f1 = Seq(
    DMNFile("loc", Array(1.toByte, 2.toByte)),
    DMNFile("loc2", Array(3.toByte, 3.toByte))
  )
  val f2 = Seq(DMNFile("loc3", Array(5.toByte, 5.toByte)))

  val files =
    f1.map(Versioned(1, 1, _)) ++
      f2.map(Versioned(3, 1, _))

  val c1 = Versioned(1, 1, DMNConfiguration("1"))
  val c2 = Versioned(3, 1, DMNConfiguration("2"))

  val configs = Seq(c1, c2)

  implicit class DMNFileOps(f: Seq[DMNFile]) {
    def arr = f.map(f => (f.locationURI, f.bytes.toSeq)).toSet
  }

  test("serialization and joins etc. works") {
    import sparkSession.implicits._
    val ps = providers.toDS.selectExpr("id pid", "version pver", "what.*")
    val ms = models.toDS.selectExpr("id mid", "version mver", "what.*")
    val fs = files.toDS.selectExpr("id fid", "version fver", "what.*")
    val cs = configs.toDS.selectExpr("id cid", "version cver", "what.*")

    import serialization._
    val vfs = readVersionedFilesFromDF(fs, col("fid"), col("fver"), col("locationURI"), col("bytes"))
    val vms = readVersionedModelServicesFromDF(ms, col("mid"), col("mver"), col("name"), col("namespace"), col("service"), col("resultProvider"))
    val vps = readVersionedProvidersFromDF(ps, col("pid"), col("pver"), col("fieldExpression"), col("providerType"), col("contextPath"))
    val cps = readVersionedConfigurationDF(cs.toDF(), col("cid"), col("cver"), col("options"))

    val execs = readVersionedExecutionsFromDF(vfs, vms, vps, cps)
    execs.count shouldBe 2
    val e1 = execs.filter("id = 1 and version = 1").collect().head
    val e2 = execs.filter("id = 3 and version = 1").collect().head

    e1.what.dmnFiles.arr shouldBe f1.arr
    e1.what.model shouldBe m1
    e1.what.contextProviders.toSet shouldBe pseq1.toSet
    e1.what.configuration shouldBe c1.what

    e2.what.dmnFiles.arr shouldBe f2.arr
    e2.what.model shouldBe m2
    e2.what.contextProviders.toSet shouldBe pseq2.toSet
    e2.what.configuration shouldBe c2.what
  }

  test("serialization and joins etc. works - no config") {
    import sparkSession.implicits._
    val ps = providers.toDS.selectExpr("id pid", "version pver", "what.*")
    val ms = models.toDS.selectExpr("id mid", "version mver", "what.*")
    val fs = files.toDS.selectExpr("id fid", "version fver", "what.*")

    import serialization._
    val vfs = readVersionedFilesFromDF(fs, col("fid"), col("fver"), col("locationURI"), col("bytes"))
    val vms = readVersionedModelServicesFromDF(ms, col("mid"), col("mver"), col("name"), col("namespace"), col("service"), col("resultProvider"))
    val vps = readVersionedProvidersFromDF(ps, col("pid"), col("pver"), col("fieldExpression"), col("providerType"), col("contextPath"))

    val execs = readVersionedExecutionsFromDF(vfs, vms, vps)
    execs.count shouldBe 2
    val e1 = execs.filter("id = 1 and version = 1").collect().head
    val e2 = execs.filter("id = 3 and version = 1").collect().head

    e1.what.dmnFiles.arr shouldBe f1.arr
    e1.what.model shouldBe m1
    e1.what.contextProviders.toSet shouldBe pseq1.toSet
    e1.what.configuration shouldBe DMNConfiguration.empty

    e2.what.dmnFiles.arr shouldBe f2.arr
    e2.what.model shouldBe m2
    e2.what.contextProviders.toSet shouldBe pseq2.toSet
    e2.what.configuration shouldBe DMNConfiguration.empty
  }
}
