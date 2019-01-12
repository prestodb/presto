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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.SingleRowBlockWriter;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.RowType.Field;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.util.JsonCastException;
import io.prestosql.util.JsonUtil.BlockBuilderAppender;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.metadata.Signature.withVariadicBound;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.JsonUtil.BlockBuilderAppender.createBlockBuilderAppender;
import static io.prestosql.util.JsonUtil.JSON_FACTORY;
import static io.prestosql.util.JsonUtil.canCastFromJson;
import static io.prestosql.util.JsonUtil.createJsonParser;
import static io.prestosql.util.JsonUtil.getFieldNameToIndex;
import static io.prestosql.util.JsonUtil.parseJsonToSingleRowBlock;
import static io.prestosql.util.JsonUtil.truncateIfNecessaryForErrorMessage;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.String.format;

public class JsonToRowCast
        extends SqlOperator
{
    public static final JsonToRowCast JSON_TO_ROW = new JsonToRowCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonToRowCast.class, "toRow", RowType.class, BlockBuilderAppender[].class, Optional.class, ConnectorSession.class, Slice.class);

    private JsonToRowCast()
    {
        super(OperatorType.CAST,
                ImmutableList.of(withVariadicBound("T", "row")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature(StandardTypes.JSON)));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        RowType rowType = (RowType) boundVariables.getTypeVariable("T");
        checkCondition(canCastFromJson(rowType), INVALID_CAST_ARGUMENT, "Cannot cast JSON to %s", rowType);

        List<Field> rowFields = rowType.getFields();
        BlockBuilderAppender[] fieldAppenders = rowFields.stream()
                .map(rowField -> createBlockBuilderAppender(rowField.getType()))
                .toArray(BlockBuilderAppender[]::new);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(rowType).bindTo(fieldAppenders).bindTo(getFieldNameToIndex(rowFields));
        return new ScalarFunctionImplementation(
                true,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Block toRow(
            RowType rowType,
            BlockBuilderAppender[] fieldAppenders,
            Optional<Map<String, Integer>> fieldNameToIndex,
            ConnectorSession connectorSession,
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
