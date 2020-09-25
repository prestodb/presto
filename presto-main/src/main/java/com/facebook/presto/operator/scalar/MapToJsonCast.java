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
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.TreeMap;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.JsonOperators.JSON_FACTORY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.JsonUtil.JsonGeneratorWriter;
import static com.facebook.presto.util.JsonUtil.ObjectKeyProvider;
import static com.facebook.presto.util.JsonUtil.canCastToJson;
import static com.facebook.presto.util.JsonUtil.createJsonGenerator;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;

public class MapToJsonCast
        extends SqlOperator
{
    public static final MapToJsonCast MAP_TO_JSON = new MapToJsonCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapToJsonCast.class, "toJson", ObjectKeyProvider.class, JsonGeneratorWriter.class, SqlFunctionProperties.class, Block.class);

    private MapToJsonCast()
    {
        super(OperatorType.CAST,
                ImmutableList.of(typeVariable("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.JSON),
                ImmutableList.of(parseTypeSignature("map(K,V)")));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        Type mapType = functionAndTypeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
        checkCondition(canCastToJson(mapType), INVALID_CAST_ARGUMENT, "Cannot cast %s to JSON", mapType);

        ObjectKeyProvider provider = ObjectKeyProvider.createObjectKeyProvider(keyType);
        JsonGeneratorWriter writer = JsonGeneratorWriter.createJsonGeneratorWriter(valueType);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(provider).bindTo(writer);

        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Slice toJson(ObjectKeyProvider provider, JsonGeneratorWriter writer, SqlFunctionProperties properties, Block block)
    {
        try {
            Map<String, Integer> orderedKeyToValuePosition = new TreeMap<>();
            for (int i = 0; i < block.getPositionCount(); i += 2) {
                String objectKey = provider.getObjectKey(block, i);
                orderedKeyToValuePosition.put(objectKey, i + 1);
            }

            SliceOutput output = new DynamicSliceOutput(40);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeStartObject();
                for (Map.Entry<String, Integer> entry : orderedKeyToValuePosition.entrySet()) {
                    jsonGenerator.writeFieldName(entry.getKey());
                    writer.writeJsonValue(jsonGenerator, block, entry.getValue(), properties);
                }
                jsonGenerator.writeEndObject();
            }
            return output.slice();
        }
        catch (IOException e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
