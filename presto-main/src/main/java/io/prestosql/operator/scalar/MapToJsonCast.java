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
package io.prestosql.operator.scalar;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.JsonOperators.JSON_FACTORY;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.JsonUtil.JsonGeneratorWriter;
import static io.prestosql.util.JsonUtil.ObjectKeyProvider;
import static io.prestosql.util.JsonUtil.canCastToJson;
import static io.prestosql.util.JsonUtil.createJsonGenerator;
import static io.prestosql.util.Reflection.methodHandle;

public class MapToJsonCast
        extends SqlOperator
{
    public static final MapToJsonCast MAP_TO_JSON = new MapToJsonCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapToJsonCast.class, "toJson", ObjectKeyProvider.class, JsonGeneratorWriter.class, ConnectorSession.class, Block.class);

    private MapToJsonCast()
    {
        super(OperatorType.CAST,
                ImmutableList.of(typeVariable("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.JSON),
                ImmutableList.of(parseTypeSignature("map(K,V)")));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        Type mapType = typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
        checkCondition(canCastToJson(mapType), INVALID_CAST_ARGUMENT, "Cannot cast %s to JSON", mapType);

        ObjectKeyProvider provider = ObjectKeyProvider.createObjectKeyProvider(keyType);
        JsonGeneratorWriter writer = JsonGeneratorWriter.createJsonGeneratorWriter(valueType);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(provider).bindTo(writer);

        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Slice toJson(ObjectKeyProvider provider, JsonGeneratorWriter writer, ConnectorSession session, Block block)
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
                    writer.writeJsonValue(jsonGenerator, block, entry.getValue(), session);
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
