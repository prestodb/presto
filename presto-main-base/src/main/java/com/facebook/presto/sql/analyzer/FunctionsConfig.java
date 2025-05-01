/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.analyzer;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.presto.operator.aggregation.arrayagg.ArrayAggGroupImplementation;
import com.facebook.presto.operator.aggregation.histogram.HistogramGroupImplementation;
import com.facebook.presto.operator.aggregation.multimapagg.MultimapAggGroupImplementation;
import com.facebook.presto.spi.function.Description;
import jakarta.validation.constraints.Min;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.sql.analyzer.RegexLibrary.JONI;

public class FunctionsConfig
{
    private boolean legacyArrayAgg;
    private boolean useAlternativeFunctionSignatures;
    private boolean legacyMapSubscript;
    private boolean reduceAggForComplexTypesEnabled = true;
    private boolean legacyLogFunction;
    private int re2JDfaStatesLimit = Integer.MAX_VALUE;
    private int re2JDfaRetries = 5;
    private RegexLibrary regexLibrary = JONI;
    private long kHyperLogLogAggregationGroupNumberLimit;
    private boolean limitNumberOfGroupsForKHyperLogLogAggregations = true;
    private boolean useNewNanDefinition = true;
    private HistogramGroupImplementation histogramGroupImplementation = HistogramGroupImplementation.NEW;
    private ArrayAggGroupImplementation arrayAggGroupImplementation = ArrayAggGroupImplementation.NEW;
    private MultimapAggGroupImplementation multimapAggGroupImplementation = MultimapAggGroupImplementation.NEW;
    private boolean legacyRowFieldOrdinalAccess;
    private boolean legacyTimestamp = true;
    private boolean parseDecimalLiteralsAsDouble;
    private boolean fieldNamesInJsonCastEnabled;
    private boolean warnOnPossibleNans;
    private boolean legacyCharToVarcharCoercion;
    private boolean legacyJsonCast = true;
    private boolean canonicalizedJsonExtract;
    private String defaultNamespacePrefix = JAVA_BUILTIN_NAMESPACE.toString();

    @Config("deprecated.legacy-array-agg")
    public FunctionsConfig setLegacyArrayAgg(boolean legacyArrayAgg)
    {
        this.legacyArrayAgg = legacyArrayAgg;
        return this;
    }

    public boolean isLegacyArrayAgg()
    {
        return legacyArrayAgg;
    }

    @Config("deprecated.legacy-log-function")
    public FunctionsConfig setLegacyLogFunction(boolean value)
    {
        this.legacyLogFunction = value;
        return this;
    }

    public boolean isLegacyLogFunction()
    {
        return legacyLogFunction;
    }

    @Config("use-alternative-function-signatures")
    @ConfigDescription("Override intermediate aggregation type of some aggregation functions to be compatible with Velox")
    public FunctionsConfig setUseAlternativeFunctionSignatures(boolean value)
    {
        this.useAlternativeFunctionSignatures = value;
        return this;
    }

    public boolean isUseAlternativeFunctionSignatures()
    {
        return useAlternativeFunctionSignatures;
    }

    @Config("deprecated.legacy-map-subscript")
    public FunctionsConfig setLegacyMapSubscript(boolean value)
    {
        this.legacyMapSubscript = value;
        return this;
    }

    public boolean isLegacyMapSubscript()
    {
        return legacyMapSubscript;
    }

    @Config("reduce-agg-for-complex-types-enabled")
    public FunctionsConfig setReduceAggForComplexTypesEnabled(boolean reduceAggForComplexTypesEnabled)
    {
        this.reduceAggForComplexTypesEnabled = reduceAggForComplexTypesEnabled;
        return this;
    }

    public boolean isReduceAggForComplexTypesEnabled()
    {
        return reduceAggForComplexTypesEnabled;
    }

    @Config("re2j.dfa-states-limit")
    public FunctionsConfig setRe2JDfaStatesLimit(int re2JDfaStatesLimit)
    {
        this.re2JDfaStatesLimit = re2JDfaStatesLimit;
        return this;
    }

    @Min(2)
    public int getRe2JDfaStatesLimit()
    {
        return re2JDfaStatesLimit;
    }

    @Config("re2j.dfa-retries")
    public FunctionsConfig setRe2JDfaRetries(int re2JDfaRetries)
    {
        this.re2JDfaRetries = re2JDfaRetries;
        return this;
    }

    @Min(0)
    public int getRe2JDfaRetries()
    {
        return re2JDfaRetries;
    }

    @Config("regex-library")
    public FunctionsConfig setRegexLibrary(RegexLibrary regexLibrary)
    {
        this.regexLibrary = regexLibrary;
        return this;
    }

    public RegexLibrary getRegexLibrary()
    {
        return regexLibrary;
    }

    @Config("khyperloglog-agg-group-limit")
    @ConfigDescription("Maximum number of groups for khyperloglog_agg per task")
    public FunctionsConfig setKHyperLogLogAggregationGroupNumberLimit(long kHyperLogLogAggregationGroupNumberLimit)
    {
        this.kHyperLogLogAggregationGroupNumberLimit = kHyperLogLogAggregationGroupNumberLimit;
        return this;
    }

    public long getKHyperLogLogAggregationGroupNumberLimit()
    {
        return kHyperLogLogAggregationGroupNumberLimit;
    }

    @Config("limit-khyperloglog-agg-group-number-enabled")
    @ConfigDescription("Enable limiting number of groups for khyperloglog_agg and merge of KHyperLogLog states")
    public FunctionsConfig setLimitNumberOfGroupsForKHyperLogLogAggregations(boolean limitNumberOfGroupsForKHyperLogLogAggregations)
    {
        this.limitNumberOfGroupsForKHyperLogLogAggregations = limitNumberOfGroupsForKHyperLogLogAggregations;
        return this;
    }

    public boolean getLimitNumberOfGroupsForKHyperLogLogAggregations()
    {
        return limitNumberOfGroupsForKHyperLogLogAggregations;
    }

    @Config("use-new-nan-definition")
    @ConfigDescription("Enable functions to use the new consistent NaN definition where NaN=NaN and is sorted largest")
    public FunctionsConfig setUseNewNanDefinition(boolean useNewNanDefinition)
    {
        this.useNewNanDefinition = useNewNanDefinition;
        return this;
    }

    public boolean getUseNewNanDefinition()
    {
        return useNewNanDefinition;
    }

    @Config("histogram.implementation")
    public FunctionsConfig setHistogramGroupImplementation(HistogramGroupImplementation groupByMode)
    {
        this.histogramGroupImplementation = groupByMode;
        return this;
    }

    public HistogramGroupImplementation getHistogramGroupImplementation()
    {
        return histogramGroupImplementation;
    }

    @Config("arrayagg.implementation")
    public FunctionsConfig setArrayAggGroupImplementation(ArrayAggGroupImplementation groupByMode)
    {
        this.arrayAggGroupImplementation = groupByMode;
        return this;
    }

    public ArrayAggGroupImplementation getArrayAggGroupImplementation()
    {
        return arrayAggGroupImplementation;
    }

    @Config("multimapagg.implementation")
    public FunctionsConfig setMultimapAggGroupImplementation(MultimapAggGroupImplementation groupByMode)
    {
        this.multimapAggGroupImplementation = groupByMode;
        return this;
    }

    public MultimapAggGroupImplementation getMultimapAggGroupImplementation()
    {
        return multimapAggGroupImplementation;
    }

    @Config("deprecated.legacy-row-field-ordinal-access")
    public FunctionsConfig setLegacyRowFieldOrdinalAccess(boolean value)
    {
        this.legacyRowFieldOrdinalAccess = value;
        return this;
    }

    public boolean isLegacyRowFieldOrdinalAccess()
    {
        return legacyRowFieldOrdinalAccess;
    }

    @Config("deprecated.legacy-timestamp")
    public FunctionsConfig setLegacyTimestamp(boolean value)
    {
        this.legacyTimestamp = value;
        return this;
    }

    public boolean isLegacyTimestamp()
    {
        return legacyTimestamp;
    }

    @Config("parse-decimal-literals-as-double")
    public FunctionsConfig setParseDecimalLiteralsAsDouble(boolean parseDecimalLiteralsAsDouble)
    {
        this.parseDecimalLiteralsAsDouble = parseDecimalLiteralsAsDouble;
        return this;
    }

    public boolean isParseDecimalLiteralsAsDouble()
    {
        return parseDecimalLiteralsAsDouble;
    }

    @Config("field-names-in-json-cast-enabled")
    public FunctionsConfig setFieldNamesInJsonCastEnabled(boolean fieldNamesInJsonCastEnabled)
    {
        this.fieldNamesInJsonCastEnabled = fieldNamesInJsonCastEnabled;
        return this;
    }

    public boolean isFieldNamesInJsonCastEnabled()
    {
        return fieldNamesInJsonCastEnabled;
    }

    @Config("warn-on-common-nan-patterns")
    @ConfigDescription("Give warnings for operations on DOUBLE/REAL types where NaN issues are common")
    public FunctionsConfig setWarnOnCommonNanPatterns(boolean warnOnPossibleNans)
    {
        this.warnOnPossibleNans = warnOnPossibleNans;
        return this;
    }

    public boolean getWarnOnCommonNanPatterns()
    {
        return warnOnPossibleNans;
    }

    @Config("deprecated.legacy-char-to-varchar-coercion")
    public FunctionsConfig setLegacyCharToVarcharCoercion(boolean value)
    {
        this.legacyCharToVarcharCoercion = value;
        return this;
    }

    public boolean isLegacyCharToVarcharCoercion()
    {
        return legacyCharToVarcharCoercion;
    }

    @Config("legacy-json-cast")
    public FunctionsConfig setLegacyJsonCast(boolean legacyJsonCast)
    {
        this.legacyJsonCast = legacyJsonCast;
        return this;
    }

    public boolean isLegacyJsonCast()
    {
        return legacyJsonCast;
    }

    @Config("canonicalized-json-extract")
    @Description("Extracts json data in a canonicalized manner, and raises a PrestoException when encountering invalid json structures within the input json path")
    public FunctionsConfig setCanonicalizedJsonExtract(boolean canonicalizedJsonExtract)
    {
        this.canonicalizedJsonExtract = canonicalizedJsonExtract;
        return this;
    }

    public boolean isCanonicalizedJsonExtract()
    {
        return canonicalizedJsonExtract;
    }

    @Config("presto.default-namespace")
    @ConfigDescription("Specifies the default function namespace prefix")
    public FunctionsConfig setDefaultNamespacePrefix(String defaultNamespacePrefix)
    {
        this.defaultNamespacePrefix = defaultNamespacePrefix;
        return this;
    }

    public String getDefaultNamespacePrefix()
    {
        return defaultNamespacePrefix;
    }
}
