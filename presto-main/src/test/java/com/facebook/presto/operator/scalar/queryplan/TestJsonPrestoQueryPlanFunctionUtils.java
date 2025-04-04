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
package com.facebook.presto.operator.scalar.queryplan;

public class TestJsonPrestoQueryPlanFunctionUtils
{
    private TestJsonPrestoQueryPlanFunctionUtils() {}

    // explain (type distributed, format json) select * from r,s where r.a=s.a;
    public static String joinPlan =
            "{\n" +
                    "   \"0\" : {\n" +
                    "     \"plan\" : {\n" +
                    "   \"id\" : \"8\",\n" +
                    "   \"name\" : \"Output\",\n" +
                    "   \"identifier\" : \"[a, b, a, b]\",\n" +
                    "   \"details\" : \"b := b_1 (1:41)\\n\",\n" +
                    "   \"children\" : [ {\n" +
                    "     \"id\" : \"253\",\n" +
                    "     \"name\" : \"RemoteSource\",\n" +
                    "     \"identifier\" : \"[1]\",\n" +
                    "     \"details\" : \"\",\n" +
                    "     \"children\" : [ ],\n" +
                    "     \"remoteSources\" : [ \"1\" ],\n" +
                    "     \"estimates\" : [ ]\n" +
                    "   } ],\n" +
                    "   \"remoteSources\" : [ ],\n" +
                    "   \"estimates\" : [ {\n" +
                    "     \"outputRowCount\" : 0.0,\n" +
                    "     \"totalSize\" : \"NaN\",\n" +
                    "     \"confident\" : \"LOW\",\n" +
                    "     \"variableStatistics\" : {\n" +
                    "       \"a<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       },\n" +
                    "       \"b<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       },\n" +
                    "       \"b_1<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       }\n" +
                    "     },\n" +
                    "     \"joinNodeStatsEstimate\" : {\n" +
                    "       \"nullJoinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"joinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"nullJoinProbeKeyCount\" : \"NaN\",\n" +
                    "       \"joinProbeKeyCount\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"tableWriterNodeStatsEstimate\" : {\n" +
                    "       \"taskCountIfScaledWriter\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"partialAggregationStatsEstimate\" : {\n" +
                    "       \"inputBytes\" : \"NaN\",\n" +
                    "       \"outputBytes\" : \"NaN\"\n" +
                    "     }\n" +
                    "   } ]\n" +
                    " }\n" +
                    "   },\n" +
                    "   \"1\" : {\n" +
                    "     \"plan\" : {\n" +
                    "   \"id\" : \"230\",\n" +
                    "   \"name\" : \"InnerJoin\",\n" +
                    "   \"identifier\" : \"[(\\\"a\\\" = \\\"a_0\\\")][$hashvalue, $hashvalue_21]\",\n" +
                    "   \"details\" : \"Distribution: PARTITIONED\\n\",\n" +
                    "   \"children\" : [ {\n" +
                    "     \"id\" : \"251\",\n" +
                    "     \"name\" : \"RemoteSource\",\n" +
                    "     \"identifier\" : \"[2]\",\n" +
                    "     \"details\" : \"\",\n" +
                    "     \"children\" : [ ],\n" +
                    "     \"remoteSources\" : [ \"2\" ],\n" +
                    "     \"estimates\" : [ ]\n" +
                    "   }, {\n" +
                    "     \"id\" : \"284\",\n" +
                    "     \"name\" : \"LocalExchange\",\n" +
                    "     \"identifier\" : \"[HASH][$hashvalue_21] (a_0)\",\n" +
                    "     \"details\" : \"\",\n" +
                    "     \"children\" : [ {\n" +
                    "       \"id\" : \"252\",\n" +
                    "       \"name\" : \"RemoteSource\",\n" +
                    "       \"identifier\" : \"[3]\",\n" +
                    "       \"details\" : \"\",\n" +
                    "       \"children\" : [ ],\n" +
                    "       \"remoteSources\" : [ \"3\" ],\n" +
                    "       \"estimates\" : [ ]\n" +
                    "     } ],\n" +
                    "     \"remoteSources\" : [ ],\n" +
                    "     \"estimates\" : [ {\n" +
                    "       \"outputRowCount\" : 0.0,\n" +
                    "       \"totalSize\" : \"NaN\",\n" +
                    "       \"confident\" : \"HIGH\",\n" +
                    "       \"variableStatistics\" : {\n" +
                    "         \"$hashvalue_21<bigint>\" : {\n" +
                    "           \"lowValue\" : \"NaN\",\n" +
                    "           \"highValue\" : \"NaN\",\n" +
                    "           \"nullsFraction\" : 1.0,\n" +
                    "           \"averageRowSize\" : 0.0,\n" +
                    "           \"distinctValuesCount\" : 0.0\n" +
                    "         },\n" +
                    "         \"a_0<integer>\" : {\n" +
                    "           \"lowValue\" : \"NaN\",\n" +
                    "           \"highValue\" : \"NaN\",\n" +
                    "           \"nullsFraction\" : 1.0,\n" +
                    "           \"averageRowSize\" : 0.0,\n" +
                    "           \"distinctValuesCount\" : 0.0\n" +
                    "         },\n" +
                    "         \"b_1<integer>\" : {\n" +
                    "           \"lowValue\" : \"NaN\",\n" +
                    "           \"highValue\" : \"NaN\",\n" +
                    "           \"nullsFraction\" : 1.0,\n" +
                    "           \"averageRowSize\" : 0.0,\n" +
                    "           \"distinctValuesCount\" : 0.0\n" +
                    "         }\n" +
                    "       },\n" +
                    "       \"joinNodeStatsEstimate\" : {\n" +
                    "         \"nullJoinBuildKeyCount\" : \"NaN\",\n" +
                    "         \"joinBuildKeyCount\" : \"NaN\",\n" +
                    "         \"nullJoinProbeKeyCount\" : \"NaN\",\n" +
                    "         \"joinProbeKeyCount\" : \"NaN\"\n" +
                    "       },\n" +
                    "       \"tableWriterNodeStatsEstimate\" : {\n" +
                    "         \"taskCountIfScaledWriter\" : \"NaN\"\n" +
                    "       },\n" +
                    "       \"partialAggregationStatsEstimate\" : {\n" +
                    "         \"inputBytes\" : \"NaN\",\n" +
                    "         \"outputBytes\" : \"NaN\"\n" +
                    "       }\n" +
                    "     } ]\n" +
                    "   } ],\n" +
                    "   \"remoteSources\" : [ ],\n" +
                    "   \"estimates\" : [ {\n" +
                    "     \"outputRowCount\" : 0.0,\n" +
                    "     \"totalSize\" : \"NaN\",\n" +
                    "     \"confident\" : \"LOW\",\n" +
                    "     \"variableStatistics\" : {\n" +
                    "       \"a<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       },\n" +
                    "       \"b<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       },\n" +
                    "       \"b_1<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       }\n" +
                    "     },\n" +
                    "     \"joinNodeStatsEstimate\" : {\n" +
                    "       \"nullJoinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"joinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"nullJoinProbeKeyCount\" : \"NaN\",\n" +
                    "       \"joinProbeKeyCount\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"tableWriterNodeStatsEstimate\" : {\n" +
                    "       \"taskCountIfScaledWriter\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"partialAggregationStatsEstimate\" : {\n" +
                    "       \"inputBytes\" : \"NaN\",\n" +
                    "       \"outputBytes\" : \"NaN\"\n" +
                    "     }\n" +
                    "   } ]\n" +
                    " }\n" +
                    "   },\n" +
                    "   \"2\" : {\n" +
                    "     \"plan\" : {\n" +
                    "   \"id\" : \"313\",\n" +
                    "   \"name\" : \"ScanProject\",\n" +
                    "   \"identifier\" : \"[table = TableHandle {connectorId='hive', connectorHandle='HiveTableHandle{schemaName=tpch, tableName=r, analyzePartitionValues=Optional.empty}', layout='Optional[tpch.r{}]'}, projectLocality = LOCAL]\",\n" +
                    "   \"details\" : \"$hashvalue_20 := combine_hash(BIGINT'0', COALESCE($operator$hash_code(a), BIGINT'0')) (1:55)\\nLAYOUT: tpch.r{}\\nb := b:int:1:REGULAR (1:55)\\na := a:int:0:REGULAR (1:55)\\n\",\n" +
                    "   \"children\" : [ ],\n" +
                    "   \"remoteSources\" : [ ],\n" +
                    "   \"estimates\" : [ {\n" +
                    "     \"outputRowCount\" : 0.0,\n" +
                    "     \"totalSize\" : 0.0,\n" +
                    "     \"confident\" : \"HIGH\",\n" +
                    "     \"variableStatistics\" : {\n" +
                    "       \"a<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       },\n" +
                    "       \"b<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       }\n" +
                    "     },\n" +
                    "     \"joinNodeStatsEstimate\" : {\n" +
                    "       \"nullJoinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"joinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"nullJoinProbeKeyCount\" : \"NaN\",\n" +
                    "       \"joinProbeKeyCount\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"tableWriterNodeStatsEstimate\" : {\n" +
                    "       \"taskCountIfScaledWriter\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"partialAggregationStatsEstimate\" : {\n" +
                    "       \"inputBytes\" : \"NaN\",\n" +
                    "       \"outputBytes\" : \"NaN\"\n" +
                    "     }\n" +
                    "   }, {\n" +
                    "     \"outputRowCount\" : 0.0,\n" +
                    "     \"totalSize\" : \"NaN\",\n" +
                    "     \"confident\" : \"HIGH\",\n" +
                    "     \"variableStatistics\" : {\n" +
                    "       \"$hashvalue_20<bigint>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       },\n" +
                    "       \"a<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       },\n" +
                    "       \"b<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       }\n" +
                    "     },\n" +
                    "     \"joinNodeStatsEstimate\" : {\n" +
                    "       \"nullJoinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"joinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"nullJoinProbeKeyCount\" : \"NaN\",\n" +
                    "       \"joinProbeKeyCount\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"tableWriterNodeStatsEstimate\" : {\n" +
                    "       \"taskCountIfScaledWriter\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"partialAggregationStatsEstimate\" : {\n" +
                    "       \"inputBytes\" : \"NaN\",\n" +
                    "       \"outputBytes\" : \"NaN\"\n" +
                    "     }\n" +
                    "   } ]\n" +
                    " }\n" +
                    "   },\n" +
                    "   \"3\" : {\n" +
                    "     \"plan\" : {\n" +
                    "   \"id\" : \"314\",\n" +
                    "   \"name\" : \"ScanProject\",\n" +
                    "   \"identifier\" : \"[table = TableHandle {connectorId='hive', connectorHandle='HiveTableHandle{schemaName=tpch, tableName=s, analyzePartitionValues=Optional.empty}', layout='Optional[tpch.s{}]'}, projectLocality = LOCAL]\",\n" +
                    "   \"details\" : \"$hashvalue_23 := combine_hash(BIGINT'0', COALESCE($operator$hash_code(a_0), BIGINT'0')) (1:57)\\nLAYOUT: tpch.s{}\\nb_1 := b:int:1:REGULAR (1:57)\\na_0 := a:int:0:REGULAR (1:57)\\n\",\n" +
                    "   \"children\" : [ ],\n" +
                    "   \"remoteSources\" : [ ],\n" +
                    "   \"estimates\" : [ {\n" +
                    "     \"outputRowCount\" : 0.0,\n" +
                    "     \"totalSize\" : 0.0,\n" +
                    "     \"confident\" : \"HIGH\",\n" +
                    "     \"variableStatistics\" : {\n" +
                    "       \"a_0<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       },\n" +
                    "       \"b_1<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       }\n" +
                    "     },\n" +
                    "     \"joinNodeStatsEstimate\" : {\n" +
                    "       \"nullJoinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"joinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"nullJoinProbeKeyCount\" : \"NaN\",\n" +
                    "       \"joinProbeKeyCount\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"tableWriterNodeStatsEstimate\" : {\n" +
                    "       \"taskCountIfScaledWriter\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"partialAggregationStatsEstimate\" : {\n" +
                    "       \"inputBytes\" : \"NaN\",\n" +
                    "       \"outputBytes\" : \"NaN\"\n" +
                    "     }\n" +
                    "   }, {\n" +
                    "     \"outputRowCount\" : 0.0,\n" +
                    "     \"totalSize\" : \"NaN\",\n" +
                    "     \"confident\" : \"HIGH\",\n" +
                    "     \"variableStatistics\" : {\n" +
                    "       \"$hashvalue_23<bigint>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       },\n" +
                    "       \"a_0<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       },\n" +
                    "       \"b_1<integer>\" : {\n" +
                    "         \"lowValue\" : \"NaN\",\n" +
                    "         \"highValue\" : \"NaN\",\n" +
                    "         \"nullsFraction\" : 1.0,\n" +
                    "         \"averageRowSize\" : 0.0,\n" +
                    "         \"distinctValuesCount\" : 0.0\n" +
                    "       }\n" +
                    "     },\n" +
                    "     \"joinNodeStatsEstimate\" : {\n" +
                    "       \"nullJoinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"joinBuildKeyCount\" : \"NaN\",\n" +
                    "       \"nullJoinProbeKeyCount\" : \"NaN\",\n" +
                    "       \"joinProbeKeyCount\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"tableWriterNodeStatsEstimate\" : {\n" +
                    "       \"taskCountIfScaledWriter\" : \"NaN\"\n" +
                    "     },\n" +
                    "     \"partialAggregationStatsEstimate\" : {\n" +
                    "       \"inputBytes\" : \"NaN\",\n" +
                    "       \"outputBytes\" : \"NaN\"\n" +
                    "     }\n" +
                    "   } ]\n" +
                    " }\n" +
                    "   }\n" +
                    " }";

    public static final String scrubbedJoinPlan =
            "{\n" +
                    "  \"0\" : {\n" +
                    "    \"plan\" : {\n" +
                    "      \"id\" : \"PLANID\",\n" +
                    "      \"name\" : \"Output\",\n" +
                    "      \"identifier\" : \"IDENTIFIER\",\n" +
                    "      \"details\" : \"DETAILS\",\n" +
                    "      \"children\" : [ {\n" +
                    "        \"id\" : \"PLANID\",\n" +
                    "        \"name\" : \"RemoteSource\",\n" +
                    "        \"identifier\" : \"IDENTIFIER\",\n" +
                    "        \"details\" : \"DETAILS\",\n" +
                    "        \"children\" : [ ],\n" +
                    "        \"remoteSources\" : [ \"1\" ],\n" +
                    "        \"estimates\" : [ ]\n" +
                    "      } ],\n" +
                    "      \"remoteSources\" : [ ],\n" +
                    "      \"estimates\" : [ ]\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"1\" : {\n" +
                    "    \"plan\" : {\n" +
                    "      \"id\" : \"PLANID\",\n" +
                    "      \"name\" : \"InnerJoin\",\n" +
                    "      \"identifier\" : \"IDENTIFIER\",\n" +
                    "      \"details\" : \"DETAILS\",\n" +
                    "      \"children\" : [ {\n" +
                    "        \"id\" : \"PLANID\",\n" +
                    "        \"name\" : \"RemoteSource\",\n" +
                    "        \"identifier\" : \"IDENTIFIER\",\n" +
                    "        \"details\" : \"DETAILS\",\n" +
                    "        \"children\" : [ ],\n" +
                    "        \"remoteSources\" : [ \"2\" ],\n" +
                    "        \"estimates\" : [ ]\n" +
                    "      }, {\n" +
                    "        \"id\" : \"PLANID\",\n" +
                    "        \"name\" : \"LocalExchange\",\n" +
                    "        \"identifier\" : \"IDENTIFIER\",\n" +
                    "        \"details\" : \"DETAILS\",\n" +
                    "        \"children\" : [ {\n" +
                    "          \"id\" : \"PLANID\",\n" +
                    "          \"name\" : \"RemoteSource\",\n" +
                    "          \"identifier\" : \"IDENTIFIER\",\n" +
                    "          \"details\" : \"DETAILS\",\n" +
                    "          \"children\" : [ ],\n" +
                    "          \"remoteSources\" : [ \"3\" ],\n" +
                    "          \"estimates\" : [ ]\n" +
                    "        } ],\n" +
                    "        \"remoteSources\" : [ ],\n" +
                    "        \"estimates\" : [ ]\n" +
                    "      } ],\n" +
                    "      \"remoteSources\" : [ ],\n" +
                    "      \"estimates\" : [ ]\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"2\" : {\n" +
                    "    \"plan\" : {\n" +
                    "      \"id\" : \"PLANID\",\n" +
                    "      \"name\" : \"ScanProject\",\n" +
                    "      \"identifier\" : \"tableName=r\",\n" +
                    "      \"details\" : \"DETAILS\",\n" +
                    "      \"children\" : [ ],\n" +
                    "      \"remoteSources\" : [ ],\n" +
                    "      \"estimates\" : [ ]\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"3\" : {\n" +
                    "    \"plan\" : {\n" +
                    "      \"id\" : \"PLANID\",\n" +
                    "      \"name\" : \"ScanProject\",\n" +
                    "      \"identifier\" : \"tableName=s\",\n" +
                    "      \"details\" : \"DETAILS\",\n" +
                    "      \"children\" : [ ],\n" +
                    "      \"remoteSources\" : [ ],\n" +
                    "      \"estimates\" : [ ]\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
}
