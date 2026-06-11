package com.sparkutils.dmn

import com.sparkutils.dmn.impl._
import org.apache.spark.sql.{Column, ShimUtils}

import java.util.ServiceLoader
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object DMN {

  lazy val dmnRepository: DMNRepository = {
    val serviceLoader = ServiceLoader.load(classOf[DMNRepository])
    val itr = serviceLoader.iterator()
    if (!itr.hasNext) {
      throw new DMNException("No ServiceProvider found for DMNRepository")
    }

    val repo = itr.next()
    repo
  }

  def dmnEval(dmnExecution: DMNExecution, debug: Boolean = false): Column = dmnEvalOpt(dmnExecution, debug).orNull

  /**
   * Runs the dmnExecution with an optional implementation specific debug mode.  If a specific runtime is provided in the [[DMNConfiguration]]
   * an attempt to load it will be made, reverting to the first found DMNRepository SPI implementation.
   *
   * @param dmnExecution The collection of dmn files, providers, model and options
   * @param debug        An implementation specific debug flag passed to the [[DMNResultProvider]]
   * @return
   */
  def dmnEvalOpt(dmnExecution: DMNExecution, debug: Boolean = false): Option[Column] = Some {
    import dmnExecution._

    val repo = configuration.runtime.flatMap { r =>
      if (r == dmnRepository.getClass.getName)
        // we already have it
        Some(dmnRepository)
      else
        ServiceLoader.load(classOf[DMNRepository]).asScala.find(_.getClass.getName == r)
    }.getOrElse(dmnRepository)

    val children = contextProviders.map(p => repo.providerForType(p, debug, configuration))
    val resultProvider = repo.resultProviderForType(model.resultProvider, debug, configuration)

    if (model.service.isDefined && repo.supportsDecisionService)
      ShimUtils.column(DMNDecisionService(repo, dmnFiles, model, configuration, debug, children :+ resultProvider))
    else
      ShimUtils.column(DMNEvaluateAll(repo, dmnFiles, model, configuration, debug, children :+ resultProvider))
  }
}