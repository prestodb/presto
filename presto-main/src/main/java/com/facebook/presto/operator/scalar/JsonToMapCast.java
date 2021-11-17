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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.util.JsonCastException;
import com.facebook.presto.util.JsonUtil.BlockBuilderAppender;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.JsonUtil.BlockBuilderAppender.createBlockBuilderAppender;
import static com.facebook.presto.util.JsonUtil.HashTable;
import static com.facebook.presto.util.JsonUtil.JSON_FACTORY;
import static com.facebook.presto.util.JsonUtil.canCastFromJson;
import static com.facebook.presto.util.JsonUtil.createJsonParser;
import static com.facebook.presto.util.JsonUtil.truncateIfNecessaryForErrorMessage;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class JsonToMapCast
        extends SqlOperator
{
    public static final JsonToMapCast JSON_TO_MAP = new JsonToMapCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonToMapCast.class, "toMap", MapType.class, BlockBuilderAppender.class, BlockBuilderAppender.class, SqlFunctionProperties.class, Slice.class);

    private JsonToMapCast()
    {
        super(OperatorType.CAST,
                ImmutableList.of(comparableTypeParameter("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("map(K,V)"),
                ImmutableList.of(parseTypeSignature(StandardTypes.JSON)));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        MapType mapType = (MapType) functionAndTypeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(TypeSignatureParameter.of(keyType.getTypeSignature()), TypeSignatureParameter.of(valueType.getTypeSignature())));
        checkCondition(canCastFromJson(mapType), INVALID_CAST_ARGUMENT, "Cannot cast JSON to %s", mapType);

        BlockBuilderAppender keyAppender = createBlockBuilderAppender(mapType.getKeyType());
        BlockBuilderAppender valueAppender = createBlockBuilderAppender(mapType.getValueType());
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(mapType).bindTo(keyAppender).bindTo(valueAppender);
        return new BuiltInScalarFunctionImplementation(
                true,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Block toMap(MapType mapType, BlockBuilderAppender keyAppender, BlockBuilderAppender valueAppender, SqlFunctionProperties properties, Slice json)
    {
        try (JsonParser jsonParser = createJsonParser(JSON_FACTORY, json)) {
            jsonParser.nextToken();
            if (jsonParser.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }

            if (jsonParser.getCurrentToken() != START_OBJECT) {
                throw new JsonCastException(format("Expected a json object, but got %s", jsonParser.getText()));
            }
            BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
            BlockBuilder singleMapBlockBuilder = mapBlockBuilder.beginBlockEntry();
            HashTable hashTable = new HashTable(mapType.getKeyType(), singleMapBlockBuilder);
            int position = 0;
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                keyAppender.append(jsonParser, singleMapBlockBuilder);
                jsonParser.nextToken();
                valueAppender.append(jsonParser, singleMapBlockBuilder);

                // Duplicate key detection is required even if the JSON is valid.
                // For example: CAST(JSON '{"1": 1, "01": 2}' AS MAP<INTEGER, INTEGER>).
                if (!hashTable.addIfAbsent(position)) {
                    throw new JsonCastException("Duplicate keys are not allowed");
                }
                position += 2;
            }
            if (jsonParser.nextToken() != null) {
                throw new JsonCastException(format("Unexpected trailing token: %s", jsonParser.getText()));
            }

            mapBlockBuilder.closeEntry();
            return mapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
        }
        catch (PrestoException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s. %s\n%s", mapType, e.getMessage(), truncateIfNecessaryForErrorMessage(json)), e);
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s.\n%s", mapType, truncateIfNecessaryForErrorMessage(json)), e);
        }
    }
}
