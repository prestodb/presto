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
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.withVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;
import static com.facebook.presto.type.TypeJsonUtils.canCastFromJson;
import static com.facebook.presto.type.TypeJsonUtils.stackRepresentationToObject;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class JsonToRowCast
        extends SqlOperator
{
    public static final JsonToRowCast JSON_TO_ROW = new JsonToRowCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonToRowCast.class, "toRow", Type.class, ConnectorSession.class, Slice.class);

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
        Type type = boundVariables.getTypeVariable("T");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(type);
        checkCondition(canCastFromJson(type), INVALID_CAST_ARGUMENT, "Cannot cast JSON to %s", type);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), methodHandle, isDeterministic());
    }

    @UsedByGeneratedCode
    public static Block toRow(Type rowType, ConnectorSession connectorSession, Slice json)
    {
        try {
            List<?> row = (List<?>) stackRepresentationToObject(connectorSession, json, rowType);
            if (row == null) {
                return null;
            }
            BlockBuilder blockBuilder = new InterleavedBlockBuilder(rowType.getTypeParameters(), new BlockBuilderStatus(), row.size());
            for (int field = 0; field < rowType.getTypeParameters().size(); field++) {
                appendToBlockBuilder(rowType.getTypeParameters().get(field), row.get(field), blockBuilder);
            }
            return blockBuilder.build();
        }
        catch (RuntimeException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to " + rowType, e);
        }
    }
}
