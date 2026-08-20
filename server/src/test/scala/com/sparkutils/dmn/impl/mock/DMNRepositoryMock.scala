package com.sparkutils.dmn.impl.mock

import com.sparkutils.dmn._
import com.sparkutils.dmn.impl._
import com.sparkutils.dmn.impl.test.ContextPath
import com.sparkutils.dmn.{DMNConfiguration, DMNFile, DMNInputField}
import org.apache.spark.sql.catalyst.expressions.Literal

class DMNRepositoryMock extends DMNRepository {

  /**
   * Throws DMNException if it can't be constructed
   *
   * @param dmnFiles      the complete set of DMNFiles to load
   * @param configuration options passed from the DMNExecution
   * @return
   */
  override def dmnRuntimeFor(dmnFiles: Seq[DMNFile], configuration: DMNConfiguration): DMNRuntime = new DMNRuntimeMock

  /**
   * The engine may not support calling decision services, evaluation will fall back to "evaluateAll" on the model
   *
   * @return
   */
  override def supportsDecisionService: Boolean = true

  /**
   * Implementation specific providers, usually managed by the dmnEval function.
   * Note at time of calling the source Expression will not be resolved.
   *
   * @param inputField    the configured input field from the DMNExecution
   * @param debug         enable an implementation specific debug mode
   * @param configuration options passed from the DMNExecution
   * @return Either the provider type or throws for an unknown type
   */
  override def providerForType(inputField: DMNInputField, debug: Boolean, configuration: DMNConfiguration): DMNContextProvider[_] = {
    val l = Literal("Mock")
    StringContextProvider(ContextPath(), true, l, None)
  }

  /**
   * Implementation specific result provider
   *
   * @param resultProviderType typically DDL of the result type.  This must be a struct with each of the possible decision names
   *                           entered against their types.
   * @param debug              enable an implementation specific debug mode
   * @param configuration      options passed from the DMNExecution
   * @return
   */
  override def resultProviderForType(resultProviderType: String, debug: Boolean, configuration: DMNConfiguration): DMNResultProvider = new DMNResultProviderMock(Seq.empty)

}