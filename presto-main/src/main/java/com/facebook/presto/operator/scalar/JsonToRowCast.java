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
import com.facebook.presto.common.block.SingleRowBlockWriter;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.RowType.Field;
import com.facebook.presto.common.type.StandardTypes;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.withVariadicBound;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.JsonUtil.BlockBuilderAppender.createBlockBuilderAppender;
import static com.facebook.presto.util.JsonUtil.JSON_FACTORY;
import static com.facebook.presto.util.JsonUtil.canCastFromJson;
import static com.facebook.presto.util.JsonUtil.createJsonParser;
import static com.facebook.presto.util.JsonUtil.getFieldNameToIndex;
import static com.facebook.presto.util.JsonUtil.parseJsonToSingleRowBlock;
import static com.facebook.presto.util.JsonUtil.truncateIfNecessaryForErrorMessage;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class JsonToRowCast
        extends SqlOperator
{
    public static final JsonToRowCast JSON_TO_ROW = new JsonToRowCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonToRowCast.class, "toRow", RowType.class, BlockBuilderAppender[].class, Optional.class, SqlFunctionProperties.class, Slice.class);

    private JsonToRowCast()
    {
        super(OperatorType.CAST,
                ImmutableList.of(withVariadicBound("T", "row")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature(StandardTypes.JSON)));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        RowType rowType = (RowType) boundVariables.getTypeVariable("T");
        checkCondition(canCastFromJson(rowType), INVALID_CAST_ARGUMENT, "Cannot cast JSON to %s", rowType);

        List<Field> rowFields = rowType.getFields();
        BlockBuilderAppender[] fieldAppenders = rowFields.stream()
                .map(rowField -> createBlockBuilderAppender(rowField.getType()))
                .toArray(BlockBuilderAppender[]::new);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(rowType).bindTo(fieldAppenders).bindTo(getFieldNameToIndex(rowFields));
        return new BuiltInScalarFunctionImplementation(
                true,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Block toRow(
            RowType rowType,
            BlockBuilderAppender[] fieldAppenders,
            Optional<Map<String, Integer>> fieldNameToIndex,
            SqlFunctionProperties properties,
            Slice json)
    {
        try (JsonParser jsonParser = createJsonParser(JSON_FACTORY, json)) {
            jsonParser.nextToken();
            if (jsonParser.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }

            if (jsonParser.getCurrentToken() != START_ARRAY && jsonParser.getCurrentToken() != START_OBJECT) {
                throw new JsonCastException(format("Expected a json array or object, but got %s", jsonParser.getText()));
            }

            BlockBuilder rowBlockBuilder = rowType.createBlockBuilder(null, 1);
            parseJsonToSingleRowBlock(
                    jsonParser,
                    (SingleRowBlockWriter) rowBlockBuilder.beginBlockEntry(),
                    fieldAppenders,
                    fieldNameToIndex);
            rowBlockBuilder.closeEntry();

            if (jsonParser.nextToken() != null) {
                throw new JsonCastException(format("Unexpected trailing token: %s", jsonParser.getText()));
            }
            return rowType.getObject(rowBlockBuilder, 0);
        }
        catch (PrestoException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s. %s\n%s", rowType, e.getMessage(), truncateIfNecessaryForErrorMessage(json)), e);
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast to %s.\n%s", rowType, truncateIfNecessaryForErrorMessage(json)), e);
        }
    }
}
