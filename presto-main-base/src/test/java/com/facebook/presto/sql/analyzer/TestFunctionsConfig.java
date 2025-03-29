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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.operator.aggregation.arrayagg.ArrayAggGroupImplementation;
import com.facebook.presto.operator.aggregation.histogram.HistogramGroupImplementation;
import com.facebook.presto.operator.aggregation.multimapagg.MultimapAggGroupImplementation;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.sql.analyzer.RegexLibrary.JONI;

public class TestFunctionsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(FunctionsConfig.class)
                .setLegacyArrayAgg(false)
                .setUseAlternativeFunctionSignatures(false)
                .setLegacyMapSubscript(false)
                .setReduceAggForComplexTypesEnabled(true)
                .setLegacyLogFunction(false)
                .setRe2JDfaStatesLimit(Integer.MAX_VALUE)
                .setRe2JDfaRetries(5)
                .setRegexLibrary(JONI)
                .setKHyperLogLogAggregationGroupNumberLimit(0)
                .setLimitNumberOfGroupsForKHyperLogLogAggregations(true)
                .setUseNewNanDefinition(true)
                .setHistogramGroupImplementation(HistogramGroupImplementation.NEW)
                .setArrayAggGroupImplementation(ArrayAggGroupImplementation.NEW)
                .setMultimapAggGroupImplementation(MultimapAggGroupImplementation.NEW)
                .setLegacyRowFieldOrdinalAccess(false)
                .setLegacyTimestamp(true)
                .setParseDecimalLiteralsAsDouble(false)
                .setFieldNamesInJsonCastEnabled(false)
                .setWarnOnCommonNanPatterns(false)
                .setLegacyCharToVarcharCoercion(false)
                .setLegacyJsonCast(true)
                .setCanonicalizedJsonExtract(false)
                .setDefaultNamespacePrefix(JAVA_BUILTIN_NAMESPACE.toString()));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("deprecated.legacy-array-agg", "true")
                .put("use-alternative-function-signatures", "true")
                .put("deprecated.legacy-map-subscript", "true")
                .put("reduce-agg-for-complex-types-enabled", "false")
                .put("deprecated.legacy-log-function", "true")
                .put("re2j.dfa-states-limit", "42")
                .put("re2j.dfa-retries", "42")
                .put("regex-library", "RE2J")
                .put("khyperloglog-agg-group-limit", "1000")
                .put("limit-khyperloglog-agg-group-number-enabled", "false")
                .put("use-new-nan-definition", "false")
                .put("histogram.implementation", "LEGACY")
                .put("arrayagg.implementation", "LEGACY")
                .put("multimapagg.implementation", "LEGACY")
                .put("deprecated.legacy-row-field-ordinal-access", "true")
                .put("deprecated.legacy-timestamp", "false")
                .put("parse-decimal-literals-as-double", "true")
                .put("field-names-in-json-cast-enabled", "true")
                .put("warn-on-common-nan-patterns", "true")
                .put("deprecated.legacy-char-to-varchar-coercion", "true")
                .put("legacy-json-cast", "false")
                .put("presto.default-namespace", "native.default")
                .put("canonicalized-json-extract", "true")
                .build();

        FunctionsConfig expected = new FunctionsConfig()
                .setLegacyArrayAgg(true)
                .setUseAlternativeFunctionSignatures(true)
                .setLegacyMapSubscript(true)
                .setReduceAggForComplexTypesEnabled(false)
                .setLegacyLogFunction(true)
                .setRe2JDfaStatesLimit(42)
                .setRe2JDfaRetries(42)
                .setRegexLibrary(RegexLibrary.RE2J)
                .setKHyperLogLogAggregationGroupNumberLimit(1000)
                .setLimitNumberOfGroupsForKHyperLogLogAggregations(false)
                .setUseNewNanDefinition(false)
                .setHistogramGroupImplementation(HistogramGroupImplementation.LEGACY)
                .setArrayAggGroupImplementation(ArrayAggGroupImplementation.LEGACY)
                .setMultimapAggGroupImplementation(MultimapAggGroupImplementation.LEGACY)
                .setLegacyRowFieldOrdinalAccess(true)
                .setLegacyTimestamp(false)
                .setParseDecimalLiteralsAsDouble(true)
                .setFieldNamesInJsonCastEnabled(true)
                .setWarnOnCommonNanPatterns(true)
                .setLegacyCharToVarcharCoercion(true)
                .setLegacyJsonCast(false)
                .setDefaultNamespacePrefix("native.default")
                .setCanonicalizedJsonExtract(true);
        assertFullMapping(properties, expected);
    }
}
