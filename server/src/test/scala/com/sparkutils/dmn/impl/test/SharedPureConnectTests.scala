package com.sparkutils.dmn.impl.test

import com.sparkutils.dmn.DMN4SparkExtension
import com.sparkutils.testing.SparkTestUtils.{connectMemory, fullClassPathConfig, scoverageClassPathsConfig}
import com.sparkutils.testing.markers.ConnectSafe
import com.sparkutils.testing.sessionStrategies.{GlobalSession, SharedSessions}
import com.sparkutils.testing.{SessionsStateHolder, SparkTestSuite}
import org.scalatest.FunSuite

trait SharedPureConnectTests extends FunSuite with SharedSessions with SparkTestSuite with ConnectSafe {

  private val extensions = Seq(classOf[DMN4SparkExtension]).map(_.getName).reduce(_ + "," + _)

  override val currentSessionsHolder: SessionsStateHolder = GlobalSession

  override val sparkConnectServerConfig: Map[String, String] =
    super.sparkConnectServerConfig() + // useDebugConnectLogs +
      scoverageClassPathsConfig +
      fullClassPathConfig +
      connectMemory("4g") +
      ("spark.sql.extensions" -> extensions)

  override val sparkClassicConfig: Map[String, String] =
    super.sparkClassicConfig() + // useDebugConnectLogs +
      scoverageClassPathsConfig +
      fullClassPathConfig +
      connectMemory("4g") +
      ("spark.sql.extensions" -> extensions)

}