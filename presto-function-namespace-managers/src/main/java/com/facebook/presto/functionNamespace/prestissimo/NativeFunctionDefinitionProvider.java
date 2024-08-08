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
package com.facebook.presto.functionNamespace.prestissimo;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.UdfFunctionSignatureMap;
import com.facebook.presto.spi.NodeManager;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

public class NativeFunctionDefinitionProvider
        implements FunctionDefinitionProvider
{
    private static final Logger log = Logger.get(NativeFunctionDefinitionProvider.class);

    private final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec;

    @Inject
    public NativeFunctionDefinitionProvider(JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> nativeFunctionSignatureMapJsonCodec)
    {
        this.nativeFunctionSignatureMapJsonCodec = nativeFunctionSignatureMapJsonCodec;
    }

    @Override
    public UdfFunctionSignatureMap getUdfDefinition(NodeManager nodeManager)
            throws IllegalStateException
    {
        try {
            String responseBody = "{\n" +
                    "  \"abs\": [\n" +
                    "    {\n" +
                    "      \"docString\": \"native.default.abs\",\n" +
                    "      \"functionKind\": \"SCALAR\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"real\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"real\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"RETURNS_NULL_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"docString\": \"native.default.abs\",\n" +
                    "      \"functionKind\": \"SCALAR\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"smallint\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"smallint\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"RETURNS_NULL_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"docString\": \"native.default.abs\",\n" +
                    "      \"functionKind\": \"SCALAR\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"integer\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"integer\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"RETURNS_NULL_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"docString\": \"native.default.abs\",\n" +
                    "      \"functionKind\": \"SCALAR\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"double\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"double\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"RETURNS_NULL_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"docString\": \"native.default.abs\",\n" +
                    "      \"functionKind\": \"SCALAR\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"bigint\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"bigint\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"RETURNS_NULL_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"docString\": \"native.default.abs\",\n" +
                    "      \"functionKind\": \"SCALAR\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"tinyint\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"tinyint\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"RETURNS_NULL_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"docString\": \"native.default.abs\",\n" +
                    "      \"functionKind\": \"SCALAR\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"DECIMAL(a_precision,a_scale)\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"DECIMAL(a_precision,a_scale)\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"CALLED_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    }\n" +
                    "  ],\n" +
                    "\"not\": [\n" +
                    "    {\n" +
                    "      \"docString\": \"native.default.not\",\n" +
                    "      \"functionKind\": \"SCALAR\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"boolean\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"boolean\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"CALLED_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    }\n" +
                    " ],\n " +
                    "  \"array_constructor\": [\n" +
                    "    {\n" +
                    "      \"docString\": \"native.default.array_constructor\",\n" +
                    "      \"functionKind\": \"SCALAR\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"array(unknown)\",\n" +
                    "      \"paramTypes\": [],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"CALLED_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"docString\": \"native.default.array_constructor\",\n" +
                    "      \"functionKind\": \"SCALAR\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"array(T)\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"T\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"CALLED_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [\n" +
                    "        {\n" +
                    "          \"comparableRequired\": false,\n" +
                    "          \"name\": \"T\",\n" +
                    "          \"nonDecimalNumericRequired\": false,\n" +
                    "          \"orderableRequired\": false,\n" +
                    "          \"variadicBound\": \"\"\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"variableArity\": true\n" +
                    "    }\n" +
                    "  ],\n" +
                    "  \"lower\": [\n" +
                    "    {\n" +
                    "      \"docString\": \"native.default.lower\",\n" +
                    "      \"functionKind\": \"SCALAR\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"varchar\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"varchar\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"CALLED_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    }\n" +
                    "  ],\n" +
                    "  \"count\": [\n" +
                    "    {\n" +
                    "      \"aggregateMetadata\": {\n" +
                    "        \"intermediateType\": \"bigint\",\n" +
                    "        \"isOrderSensitive\": true\n" +
                    "      },\n" +
                    "      \"docString\": \"native.default.count\",\n" +
                    "      \"functionKind\": \"AGGREGATE\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"bigint\",\n" +
                    "      \"paramTypes\": [],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"CALLED_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"aggregateMetadata\": {\n" +
                    "        \"intermediateType\": \"bigint\",\n" +
                    "        \"isOrderSensitive\": true\n" +
                    "      },\n" +
                    "      \"docString\": \"native.default.count\",\n" +
                    "      \"functionKind\": \"AGGREGATE\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"bigint\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"T\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"CALLED_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [\n" +
                    "        {\n" +
                    "          \"comparableRequired\": false,\n" +
                    "          \"name\": \"T\",\n" +
                    "          \"nonDecimalNumericRequired\": false,\n" +
                    "          \"orderableRequired\": false,\n" +
                    "          \"variadicBound\": \"\"\n" +
                    "        }\n" +
                    "      ],\n" +
                    "      \"variableArity\": false\n" +
                    "    }\n" +
                    "  ],\n" +
                    "  \"corr\": [\n" +
                    "    {\n" +
                    "      \"aggregateMetadata\": {\n" +
                    "        \"intermediateType\": \"row(double,bigint,double,double,double,double)\",\n" +
                    "        \"isOrderSensitive\": true\n" +
                    "      },\n" +
                    "      \"docString\": \"native.default.corr\",\n" +
                    "      \"functionKind\": \"AGGREGATE\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"double\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"double\",\n" +
                    "        \"double\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"CALLED_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    },\n" +
                    "    {\n" +
                    "      \"aggregateMetadata\": {\n" +
                    "        \"intermediateType\": \"row(double,bigint,double,double,double,double)\",\n" +
                    "        \"isOrderSensitive\": true\n" +
                    "      },\n" +
                    "      \"docString\": \"native.default.corr\",\n" +
                    "      \"functionKind\": \"AGGREGATE\",\n" +
                    "      \"functionVisibility\": \"PUBLIC\",\n" +
                    "      \"outputType\": \"real\",\n" +
                    "      \"paramTypes\": [\n" +
                    "        \"real\",\n" +
                    "        \"real\"\n" +
                    "      ],\n" +
                    "      \"routineCharacteristics\": {\n" +
                    "        \"determinism\": \"DETERMINISTIC\",\n" +
                    "        \"language\": \"CPP\",\n" +
                    "        \"nullCallClause\": \"CALLED_ON_NULL_INPUT\"\n" +
                    "      },\n" +
                    "      \"schema\": \"default\",\n" +
                    "      \"typeVariableConstraints\": [],\n" +
                    "      \"variableArity\": false\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}";
            Map<String, List<JsonBasedUdfFunctionMetadata>> nativeFunctionSignatureMap = nativeFunctionSignatureMapJsonCodec.fromJson(responseBody);
            return new UdfFunctionSignatureMap(ImmutableMap.copyOf(nativeFunctionSignatureMap));
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to get function definition for NativeFunctionNamespaceManager, " + e.getMessage());
        }
    }
}
