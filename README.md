# dmn-4-spark

A Plugable DMN wrapper API for Spark at row level

## Why?

Although [Quality](https://sparkutils.github.io/quality/) offers very high performance DQ and rule engines it's focus is more on auditability and speed then generality.  DMN offers generality as it's focus, sacrificing auditability (the user must provide it) and [performance](#performance) over the more narrowly focussed Quality.

The aim of this project is to enable a Spark first runtime for DMN processing, allowing customisation of the runtime whilst abstracting from the underlying engine.

## What it is

An API providing versioned serialization of a configurable dmn file set and plugable expression handling.

1. You bring the .dmn files to the engine directly
2. dmn.ContextPath interface allows creation of dmn.Contexts with which to run your engine of choice
3. ContextProvider Expressions are configured as pairs of input column to context path
4. The implementing DMN Runtime then processes this dmn.Context with either a DecisionService or all DecisionServices (implementation dependent)
5. Finally, a ResultProcessor Expression converts a notional dmn.Result to the correct output type

The interfaces, and implementations, are necessarily built using Spark internal APIs to both squeeze performance out of serialization overhead AND to manage rule load time.

## What it is not

An actual DMN engine, this must be provided via an API implementation, currently kogito-4-spark is planned.

It also has no opinion on DMN version support, the user of the library is abstracted from the engine choice but that engine choice is still the determining factor of DMN version support.

## How to use

Depend upon an implementation e.g. [kogito-4-spark](https://github.com/sparkutils/kogito-4-spark) using the same approach as [shim](https://sparkutils.github.io/shim/) and [Quality](https://sparkutils.github.io/quality/) each runtime must be specified.

If you typically build on OSS but deploy to other runtimes the approach is therefore (for maven):

```xml
<properties>
    <kogito4SparkVersion>0.1.3</kogito4SparkVersion>
    <kogito4SparkTestPrefix>3.4.1.oss_</kogito4SparkTestPrefix>
    <kogito4SparkRuntimePrefix>13.1.dbr_</kogito4SparkRuntimePrefix>
    <sparkShortVersion>3.4</sparkShortVersion>
    <scalaCompatVersion>2.12</scalaCompatVersion>    
</properties>

<dependencies>
    <dependency>
        <groupId>com.sparkutils.</groupId>
        <artifactId>kogito-4-spark_${kogito4SparkTestPrefix}${sparkShortVersion}_${scalaCompatVersion}</artifactId>
        <version>${kogito4SparkVersion}</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>com.sparkutils</groupId>
        <artifactId>kogito-4-spark_${kogito4SparkRuntimePrefix}${sparkShortVersion}_${scalaCompatVersion}</artifactId>
        <version>${kogito4SparkVersion}</version>
        <scope>compile</scope>
    </dependency>
</dependencies>
```

The "." at the end of the group id on the kogito4SparkTestPrefix is not a mistake and allows two versions of the same library to be used for different scopes.  It is not advised to develop on a different version of Spark/Scala than you deploy to.

Please refer to the implementation documentation for supported runtimes.

## Result types

As DMN can return anything, which the api supports, there are two key issues in usage:

### Multiple Result Types

If, when using evaluateAll semantics, you have multiple return types each of them can be different.  In order to write out exact types the result provider DDL must be a struct with each result type under the appropriate decision name. e.g. for a Kogito result:

```json
[{
   "decisionName": "booleanEval",
   "result": [true, false, true]
},
{
   "decisionName": "message",
   "result": "it's great"
}]
```

The result ddl would be:

```ddl
struct< booleanEval: array<boolean>, message: string >
```

This allows the engines to process multiple results with the correct types when stored as types.

### Changing of types

Spark 4 may offer (TBD) an approach with Variant types to allow type evolution or changing but for 3.5 usage you must ensure your data types do not change in production, the library cannot manage this for you.

## General Configuration

### Using multiple runtimes

dmn-4-spark supports the use of the first found SPI DMNRuntime but, via the DMNConfiguration.runtime option you can specify a fully qualified class name to use.  If this is not present in the provider list the default (first identified) will be used.  If none are available a DMNException will be thrown.

### Null input contexts

By default, DMNInputFields, when provided with a fieldExpression that evaluates to null result will create the contextPath with a null value.  If, instead of the default, you wish to produce DMN input without that context entry set the DMNInputField's stillSetWhenNull=false.

## Performance

The performance of DMN is dependent on it's engine but there are certain general limitations that need be called out:

1. The DMN Engines do not, unlike Quality, operate as part of Spark
    -    Compilation, even if provided, will not be inlined with WholeStageCodeGen
    -    Expressions used by FEEL cannot be optimised by Spark
    -    Serialisation overhead to the implementations input types may unavoidable (dmn-4-spark mitigates this as far as possible but Strings, Structs etc. will require converison)
2. The startup cost of the DMN Engine is inescapable (it may not matter for your usecase)
    -    Startup time is required for each executor core (this is only likely significant for small datasets / streaming with tight SLAs)
    -    dmn-4-spark attempts to mitigate this by caching implementating Engine runtimes (provided they are thread safe)
    -    dynamic clusters will recreate this runtime for each core used (Spark partition)

### Sample performance

The below information is for illustrative purposes only, the test case, whilst representative of Quality rules and based on existing DMN usage, should be viewed as directional and is created only as a way to guage base differences of simple rule processing with Kogito.  The more complicated a DMN set becomes the more likely the performance will be subject to Rete style optimisations and conversely the more pleasent editing rules in DMN may be over Quality rules (even after https://github.com/sparkutils/quality/issues/74 or similar would be supported in Quality).  It is not advised to make planning decisions or tool choice based on this test alone, rather evaluate based on the actual rules themselves, put simply: measure and YMMV.

That said, the simple test case (15 rules, each with 2 boolean tests and a total of 10 common subexpressions) used in [quality_performance_tests](https://github.com/sparkutils/quality_performance_tests/) found a performance difference of around 23x slower with kogito-4-spark:

![image](https://github.com/user-attachments/assets/fc4bc669-3f52-4c41-b77f-2a22ac15fe89)

[This report view](https://sparkutils.github.io/quality_performance_tests/reports/report_server_1m_count_vs_cache_count_inc_dmn/index.html) is against "json baseline in codegen" (plain Spark creating an array of boolean output), "json no forceEval in codegen compile evals false - extra config" (Qualitys typical audited output) and "json dmn codegen" for the kogito-4-spark run saving to an array of booleans.

NB 1: Although the average here is 0.65ms per row with Kogito other far more complex tests have shown 20ms per row, again YMMV and measurement is essential.
NB 2: The overhead of saving a Quality style audit trail is significant, this would further slow down DMN related times e.g. [in this report](https://sparkutils.github.io/quality_performance_tests/reports/report_server_to_1m_rc5_vs_spark_with_audit/index.html):

![image](https://github.com/user-attachments/assets/012c3ee4-f455-428d-873c-6e351bbaaa0c)

The bottom orange line is the default Spark returning an array of booleans, the middle green line is Quality's results and the top blue line is Spark with the simulated audit trail.
