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
import com.facebook.presto.PrestoMediaTypes;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.NOT_DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.JAVA;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readSerializedPage;
import static com.facebook.presto.spi.page.PagesSerdeUtil.writeSerializedPage;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Double.longBitsToDouble;

@Path("/v1/functions")
public class FunctionResource
{
    private final FunctionAndTypeManager manager;
    private final JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> jsonCodec;
    private final PagesSerde pagesSerde;
    private String etag = "\"etag\"";

    @Inject
    public FunctionResource(FunctionAndTypeManager manager, JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>> jsonCodec)
    {
        this.manager = manager;
        this.jsonCodec = jsonCodec;
        this.pagesSerde = createPagesSerde();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getFunctions()
    {
        Map<String, List<JsonBasedUdfFunctionMetadata>> udfMap = new HashMap<>();
        Collection<SqlFunction> builtInFunctions = manager.listBuiltInFunctions();
        for (SqlFunction function : builtInFunctions) {
            if (function.getSignature().getKind() != FunctionKind.SCALAR) {
                continue;
            }

            if (function.getVisibility() == SqlFunctionVisibility.HIDDEN) {
                continue;
            }

            JsonBasedUdfFunctionMetadata metadata = sqlFunctionToMetadata(function);
            String functionName = function.getSignature().getName().getObjectName();
            List<JsonBasedUdfFunctionMetadata> functionList = new ArrayList<>();

            if (udfMap.containsKey(functionName)) {
                functionList = udfMap.get(functionName);
            }
            functionList.add(metadata);
            udfMap.put(functionName, functionList);
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
                function.getSignature().isVariableArity(),
                new RoutineCharacteristics(
                        JAVA,
                        function.isDeterministic() ? DETERMINISTIC : NOT_DETERMINISTIC,
                        function.isCalledOnNullInput() ? CALLED_ON_NULL_INPUT : RETURNS_NULL_ON_NULL_INPUT),
                Optional.empty(),
                Optional.of(
                        new SqlFunctionId(
                                function.getSignature().getName(),
                                function.getSignature().getArgumentTypes())),
                Optional.of("1"),
                Optional.of(function.getSignature().getTypeVariableConstraints()),
                Optional.ofNullable(function.getSignature().getLongVariableConstraints()));
    }

    @GET
    @Path("/{schema}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getFunctionsBySchema(@PathParam("schema") String schema)
    {
        Map<String, List<JsonBasedUdfFunctionMetadata>> udfMap = new HashMap<>();
        Collection<SqlFunction> builtInFunctions = manager.listBuiltInFunctions();

        for (SqlFunction function : builtInFunctions) {
            if (!function.getSignature().getName().getSchemaName().equals(schema)) {
                continue;
            }

            if (function.getSignature().getKind() != FunctionKind.SCALAR) {
                continue;
            }

            if (function.getVisibility() == SqlFunctionVisibility.HIDDEN) {
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
        Collection<SqlFunction> functionList = manager.listBuiltInFunctions();

        List<JsonBasedUdfFunctionMetadata> filteredList = new ArrayList<>();
        for (SqlFunction function : functionList) {
            if (function.getSignature().getKind() != FunctionKind.SCALAR) {
                continue;
            }

            if (function.getVisibility() == SqlFunctionVisibility.HIDDEN) {
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

    @POST
    @Path("/{schema}/{functionName}/{functionId}/{version}")
    @Consumes(PrestoMediaTypes.PRESTO_PAGES)
    @Produces(PrestoMediaTypes.PRESTO_PAGES)
    public byte[] execute(
            @PathParam("schema") String schema,
            @PathParam("functionName") String functionName,
            @PathParam("functionId") String functionId,
            @PathParam("version") String version,
            byte[] serializedPageByteArray)
    {
        Slice slice = wrappedBuffer(serializedPageByteArray);
        SerializedPage serializedPage = readSerializedPage(new BasicSliceInput(slice));
        Page inputPage = pagesSerde.deserialize(serializedPage);

        // Use functionId to retrieve argument types
        List<TypeSignatureProvider> argumentTypeSignatures = extractArgumentTypeSignatures(functionId);

        FunctionHandle functionHandle = manager.lookupFunction(functionName, argumentTypeSignatures);
        BuiltInScalarFunctionImplementation functionImplementation = (BuiltInScalarFunctionImplementation) manager.getJavaScalarFunctionImplementation(functionHandle);

        Object[] inputValues = new Object[inputPage.getChannelCount()];
        for (int i = 0; i < inputPage.getChannelCount(); i++) {
            TypeSignatureProvider typeSignatureProvider = argumentTypeSignatures.get(i);
            Type type = manager.getType(typeSignatureProvider.getTypeSignature());

            inputValues[i] = deserializeBlock(type, inputPage.getBlock(i));
        }

        Object result = executeFunction(functionImplementation, inputValues);
        Type returnType = manager.getType(manager.getFunctionMetadata(functionHandle).getReturnType());
        Page outputPage = createResultPage(returnType, result);
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput((int) outputPage.getRetainedSizeInBytes());
        writeSerializedPage(sliceOutput, pagesSerde.serialize(outputPage));

        return sliceOutput.slice().byteArray();
    }

    public static List<TypeSignatureProvider> extractArgumentTypeSignatures(String encodedFunctionId)
    {
        String functionId;
        try {
            functionId = URLDecoder.decode(encodedFunctionId, StandardCharsets.UTF_8.toString());
        }
        catch (UnsupportedEncodingException e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Invalid functionId !");
        }

        SqlFunctionId sqlFunctionId = SqlFunctionId.parseSqlFunctionId(functionId);
        return sqlFunctionId.getArgumentTypes().stream()
                .map(TypeSignatureProvider::new)
                .collect(Collectors.toList());
    }

    private Object deserializeBlock(Type type, Block block)
    {
        if (block.isNull(0)) {
            return null;
        }

        switch (type.getTypeSignature().getBase()) {
            case "boolean":
                return block.getByte(0) != 0;
            case "integer":
            case "bigint":
            case "smallint":
            case "tinyint":
            case "real":
            case "interval day to second":
            case "interval year to month":
            case "timestamp":
            case "time":
                return block.toLong(0);
            case "double":
                return longBitsToDouble(block.toLong(0));
            case "varchar":
            case "varbinary":
                return block.getSlice(0, 0, block.getSliceLength(0));
            default:
                throw new IllegalArgumentException("Unsupported type for deserialization: " + type);
        }
    }

    private Object executeFunction(BuiltInScalarFunctionImplementation functionImplementation, Object[] arguments)
    {
        MethodHandle methodHandle = functionImplementation.getMethodHandle();
        try {
            return methodHandle.invokeWithArguments(arguments);
        }
        catch (Throwable throwable) {
            throw new RuntimeException("Error during function execution", throwable);
        }
    }

    private static PagesSerde createPagesSerde()
    {
        return new PagesSerde(new BlockEncodingManager(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private Page createResultPage(Type type, Object result)
    {
        PageBuilder pageBuilder = new PageBuilder(Collections.singletonList(type));
        pageBuilder.declarePosition();
        BlockBuilder output = pageBuilder.getBlockBuilder(0);
        switch (type.getTypeSignature().getBase()) {
            case "integer":
            case "bigint":
            case "smallint":
            case "tinyint":
            case "real":
            case "interval day to second":
            case "interval year to month":
            case "timestamp":
            case "time":
                type.writeLong(output, (Long) result);
                break;
            case "double":
                type.writeDouble(output, (Double) result);
                break;
            case "boolean":
                type.writeBoolean(output, (Boolean) result);
                break;
            case "varchar":
            case "char":
            case "json":
            case "varbinary":
                if (result instanceof Slice) {
                    type.writeSlice(output, (Slice) result);
                }
                else if (result instanceof byte[]) {
                    type.writeSlice(output, Slices.wrappedBuffer((byte[]) result));
                }
                else {
                    type.writeSlice(output, Slices.utf8Slice(result.toString()));
                }
                break;
            case "decimal":
                BigDecimal bd = (BigDecimal) result;
                type.writeSlice(output, Decimals.encodeScaledValue(bd));
                break;
            case "object":
            case "array":
            case "row":
            case "map":
                type.writeObject(output, result);
                break;
        }
        return pageBuilder.build();
    }

    @HEAD
    public Response getFunctionHeaders()
    {
        return Response.ok()
                .header("ETag", etag)
                .build();
    }
}
