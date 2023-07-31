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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.util.JsonUtil.JsonGeneratorWriter;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.JsonOperators.JSON_FACTORY;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.withVariadicBound;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.JsonUtil.JsonGeneratorWriter.createJsonGeneratorWriter;
import static com.facebook.presto.util.JsonUtil.canCastToJson;
import static com.facebook.presto.util.JsonUtil.createJsonGenerator;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;

public class RowToJsonCast
        extends SqlOperator
{
    public static final RowToJsonCast ROW_TO_JSON = new RowToJsonCast();

    private static final MethodHandle METHOD_HANDLE = methodHandle(RowToJsonCast.class, "toJson", List.class, List.class, SqlFunctionProperties.class, Block.class);
    private static final int ESTIMATED_FIELD_VALUE_SIZE = 40;
    private static final int ESTIMATED_FIELD_KEY_SIZE = 10;

    private RowToJsonCast()
    {
        super(OperatorType.CAST,
                ImmutableList.of(withVariadicBound("T", "row")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.JSON),
                ImmutableList.of(parseTypeSignature("T")));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type type = boundVariables.getTypeVariable("T");
        checkCondition(canCastToJson(type), INVALID_CAST_ARGUMENT, "Cannot cast %s to JSON", type);

        List<Type> fieldTypes = type.getTypeParameters();
        List<JsonGeneratorWriter> fieldWriters = new ArrayList<>(fieldTypes.size());
        List<TypeSignatureParameter> typeSignatureParameters = type.getTypeSignature().getParameters();
        List fieldNames = typeSignatureParameters.stream()
                .map(typeSignatureParameter -> typeSignatureParameter.getNamedTypeSignature().getName().orElse(""))
                .collect(Collectors.toList());
        checkCondition(fieldNames.size() == fieldTypes.size(), INVALID_ARGUMENTS, "The number of field names (%d) and field types (%d) should match in type %s ", fieldNames.size(), fieldTypes.size(), type);
        fieldTypes.forEach(fieldType -> fieldWriters.add(createJsonGeneratorWriter(fieldType)));

        MethodHandle methodHandle = METHOD_HANDLE.bindTo(fieldNames).bindTo(fieldWriters);

        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    private static Slice toJsonArray(List<JsonGeneratorWriter> fieldWriters, SqlFunctionProperties session, Block block)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(ESTIMATED_FIELD_VALUE_SIZE);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeStartArray();
                for (int i = 0; i < block.getPositionCount(); i++) {
                    fieldWriters.get(i).writeJsonValue(jsonGenerator, block, i, session);
                }
                jsonGenerator.writeEndArray();
            }
            return output.slice();
        }
        catch (IOException e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @UsedByGeneratedCode
    public static Slice toJson(List<String> fieldNames, List<JsonGeneratorWriter> fieldWriters, SqlFunctionProperties session, Block block)
    {
        if (session.isFieldNamesInJsonCastEnabled()) {
            return toJsonObject(fieldNames, fieldWriters, session, block);
        }
        return toJsonArray(fieldWriters, session, block);
    }

    private static Slice toJsonObject(List<String> fieldNames, List<JsonGeneratorWriter> fieldWriters, SqlFunctionProperties session, Block block)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(ESTIMATED_FIELD_KEY_SIZE + ESTIMATED_FIELD_VALUE_SIZE);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeStartObject();
                for (int i = 0; i < block.getPositionCount(); i++) {
                    jsonGenerator.writeFieldName(fieldNames.get(i));
                    fieldWriters.get(i).writeJsonValue(jsonGenerator, block, i, session);
                }
                jsonGenerator.writeEndObject();
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
