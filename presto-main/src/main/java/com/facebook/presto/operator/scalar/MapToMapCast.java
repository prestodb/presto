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

import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;

@ScalarOperator(CAST)
public final class MapToMapCast
{
    private MapToMapCast() {}

    @TypeParameter("FK")
    @TypeParameter("FV")
    @TypeParameter("TK")
    @TypeParameter("TV")
    @SqlType("map(TK,TV)")
    public static Block toMap(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"TK", "TK"}) MethodHandle toKeyEqualsFunction,
            @OperatorDependency(operator = CAST, returnType = "TK", argumentTypes = {"FK"}) MethodHandle keyCastFunction,
            @OperatorDependency(operator = CAST, returnType = "TV", argumentTypes = {"FV"}) MethodHandle valueCastFunction,
            @TypeParameter("FK") Type fromKeyType,
            @TypeParameter("FV") Type fromValueType,
            @TypeParameter("TK") Type toKeyType,
            @TypeParameter("TV") Type toValueType,
            ConnectorSession session,
            @SqlType("map(FK,FV)") Block fromMap)
    {
        // loop over all the parameter types and bind ConnectorSession if needed
        // TODO: binding `ConnectorSession` should be done in function invocation framework
        Class<?>[] parameterTypes = keyCastFunction.type().parameterArray();
        for (int i = 0; i < parameterTypes.length; i++) {
            if (parameterTypes[i] == ConnectorSession.class) {
                keyCastFunction = MethodHandles.insertArguments(keyCastFunction, i, session);
                break;
            }
        }
        parameterTypes = valueCastFunction.type().parameterArray();
        for (int i = 0; i < parameterTypes.length; i++) {
            if (parameterTypes[i] == ConnectorSession.class) {
                valueCastFunction = MethodHandles.insertArguments(valueCastFunction, i, session);
                break;
            }
        }

        TypedSet typedSet = new TypedSet(toKeyType, fromMap.getPositionCount() / 2);
        BlockBuilder keyBlockBuilder = toKeyType.createBlockBuilder(new BlockBuilderStatus(), fromMap.getPositionCount() / 2);
        for (int i = 0; i < fromMap.getPositionCount(); i += 2) {
            Object fromKey = readNativeValue(fromKeyType, fromMap, i);
            try {
                Object toKey = keyCastFunction.invoke(fromKey);
                if (toKey == null) {
                    throw new PrestoException(StandardErrorCode.INVALID_CAST_ARGUMENT, "map key is null");
                }
                appendToBlockBuilder(toKeyType, toKey, keyBlockBuilder);
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, t);
            }
        }
        Block keyBlock = keyBlockBuilder.build();
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(toKeyType, toValueType), new BlockBuilderStatus(), fromMap.getPositionCount());
        for (int i = 0; i < fromMap.getPositionCount(); i += 2) {
            if (!typedSet.contains(keyBlock, i / 2)) {
                typedSet.add(keyBlock, i / 2);
                toKeyType.appendTo(keyBlock, i / 2, blockBuilder);
                Object fromValue = readNativeValue(fromValueType, fromMap, i + 1);
                try {
                    Object toValue = valueCastFunction.invoke(fromValue);
                    appendToBlockBuilder(toValueType, toValue, blockBuilder);
                }
                catch (Throwable t) {
                    Throwables.propagateIfInstanceOf(t, Error.class);
                    Throwables.propagateIfInstanceOf(t, PrestoException.class);
                    throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, t);
                }
            }
            else {
                // if there are duplicated keys, fail it!
                throw new PrestoException(StandardErrorCode.INVALID_CAST_ARGUMENT, "duplicate keys");
            }
        }
        return blockBuilder.build();
    }
}
