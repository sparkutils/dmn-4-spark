package com.sparkutils.dmn.impl

import com.sparkutils.dmn._

import java.util.ServiceLoader
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object DMNExpressionImpl {

  lazy val dmnRepository: DMNRepository = {
    val serviceLoader = ServiceLoader.load(classOf[DMNRepository])
    val itr = serviceLoader.iterator()
    if (!itr.hasNext) {
      throw new DMNException("No ServiceProvider found for DMNRepository")
    }

    val repo = itr.next()
    repo
  }

  def dmnEvalExpr[T](dmnExecution: DMNExecution, debug: Boolean = false)(t:DMNExpression => T): T = {
    // Oh I dont like that
    val repo = dmnExecution.configuration.runtime.flatMap { r =>
      if (r == dmnRepository.getClass.getName)
        // we already have it
        Some(dmnRepository)
      else
        ServiceLoader.load(classOf[DMNRepository]).asScala.find(_.getClass.getName == r)
    }.getOrElse(dmnRepository)

    val children = dmnExecution.contextProviders.map(p => repo.providerForType(p, debug, dmnExecution.configuration))
    val resultProvider = repo.resultProviderForType(dmnExecution.model.resultProvider, debug, dmnExecution.configuration)
    val resultColumn = if (dmnExecution.model.service.isDefined && repo.supportsDecisionService)
      DMNDecisionService(repo, dmnExecution.dmnFiles, dmnExecution.model, dmnExecution.configuration, debug, children :+ resultProvider)
    else
      DMNEvaluateAll(repo, dmnExecution.dmnFiles, dmnExecution.model, dmnExecution.configuration, debug, children :+ resultProvider)
    t(resultColumn)
  }

}