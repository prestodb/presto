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
package com.facebook.presto.server;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionId;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.NOT_DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.JAVA;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;

@Path("/v1/functions")
public class FunctionResource
{
    private final BuiltInTypeAndFunctionNamespaceManager manager;
    private final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> jsonCodec;
    private String etag = "\"etag\"";
    public static final String FUNCTION_CATALOG = "remote";

    @Inject
    public FunctionResource(BuiltInTypeAndFunctionNamespaceManager manager, JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> jsonCodec)
    {
        this.manager = manager;
        this.jsonCodec = jsonCodec;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getFunctions()
    {
        Map<String, List<JsonBasedUdfFunctionMetadata>> udfMap = new HashMap<>();
        Collection<SqlFunction> builtInFunctions = manager.listFunctions(Optional.empty(), Optional.empty());
        for (SqlFunction function : builtInFunctions) {
            if (function.getSignature().getKind() == FunctionKind.SCALAR) {
                JsonBasedUdfFunctionMetadata metadata = sqlFunctionToMetadata(function);
                String functionName = function.getSignature().getName().toString();
                List<JsonBasedUdfFunctionMetadata> functionList = new ArrayList<>();

                if (udfMap.containsKey(functionName)) {
                    functionList = udfMap.get(functionName);
                }
                functionList.add(metadata);
                udfMap.put(functionName, functionList);
            }
        }
        return jsonCodec.toJson(udfMap);
    }

    private static JsonBasedUdfFunctionMetadata sqlFunctionToMetadata(SqlFunction function)
    {
        return new JsonBasedUdfFunctionMetadata(
                function.getDescription() != null ? function.getDescription() : "",
                function.getSignature().getKind(),
                function.getSignature().getReturnType(),
                function.getSignature().getArgumentTypes(),
                function.getSignature().getName().getSchemaName(),
                new RoutineCharacteristics(
                        JAVA,
                        function.isDeterministic() ? DETERMINISTIC : NOT_DETERMINISTIC,
                        function.isCalledOnNullInput() ? CALLED_ON_NULL_INPUT : RETURNS_NULL_ON_NULL_INPUT),
                Optional.empty(),
                Optional.of(new SqlFunctionId(function.getSignature().getName(), function.getSignature().getArgumentTypes())),
                Optional.of("1"));
    }

    @GET
    @Path("/{schema}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getFunctionsBySchema(@PathParam("schema") String schema)
    {
        Map<String, List<JsonBasedUdfFunctionMetadata>> udfMap = new HashMap<>();
        Collection<SqlFunction> builtInFunctions = manager.listFunctions(Optional.empty(), Optional.empty());

        for (SqlFunction function : builtInFunctions) {
            if (!function.getSignature().getName().getSchemaName().equals(schema)) {
                continue;
            }

            if (function.getSignature().getKind() != FunctionKind.SCALAR) {
                continue;
            }

            String functionName = function.getSignature().getName().getObjectName();
            List<JsonBasedUdfFunctionMetadata> functionList = new ArrayList<>();
            if (udfMap.containsKey(functionName)) {
                functionList = udfMap.get(functionName);
            }

            JsonBasedUdfFunctionMetadata metadata = sqlFunctionToMetadata(function);
            functionList.add(metadata);
            udfMap.put(functionName, functionList);
        }
        return jsonCodec.toJson(udfMap);
    }

    @GET
    @Path("/{schema}/{functionName}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getFunctionsBySchemaAndName(@PathParam("schema") String schema, @PathParam("functionName") String functionName)
    {
        Map<String, List<JsonBasedUdfFunctionMetadata>> udfMap = new HashMap<>();
        Collection<SqlFunction> functionList = manager.getFunctions(Optional.empty(), new QualifiedObjectName("presto", schema, functionName));

        List<JsonBasedUdfFunctionMetadata> filteredList = new ArrayList<>();
        for (SqlFunction function : functionList) {
            if (function.getSignature().getKind() != FunctionKind.SCALAR) {
                continue;
            }

            if (function.getSignature().getName().getSchemaName().equals(schema) &&
                    function.getSignature().getName().getObjectName().equals(functionName)) {
                filteredList.add(sqlFunctionToMetadata(function));
            }
        }
        if (!filteredList.isEmpty()) {
            udfMap.put(functionName, filteredList);
        }

        return jsonCodec.toJson(udfMap);
    }

    @HEAD
    public Response getFunctionHeaders()
    {
        return Response.ok()
                .header("ETag", etag)
                .build();
    }
}
