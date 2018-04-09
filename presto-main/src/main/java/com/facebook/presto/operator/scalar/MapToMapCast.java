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
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.util.Failures.internalError;

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
            @TypeParameter("map(TK,TV)") Type toMapType,
            ConnectorSession session,
            @SqlType("map(FK,FV)") Block fromMap)
    {
        boolean keyCastRequiresSession = keyCastFunction.type().parameterArray()[0] == ConnectorSession.class;
        boolean valueCastRequiresSession = valueCastFunction.type().parameterArray()[0] == ConnectorSession.class;

        TypedSet typedSet = new TypedSet(toKeyType, fromMap.getPositionCount() / 2, "map-to-map cast");
        BlockBuilder keyBlockBuilder = toKeyType.createBlockBuilder(new BlockBuilderStatus(), fromMap.getPositionCount() / 2);
        for (int i = 0; i < fromMap.getPositionCount(); i += 2) {
            Object fromKey = readNativeValue(fromKeyType, fromMap, i);
            try {
                Object toKey;
                // TODO: binding `ConnectorSession` should be done in function invocation framework
                if (!keyCastRequiresSession) {
                    toKey = keyCastFunction.invoke(fromKey);
                }
                else {
                    toKey = keyCastFunction.invoke(session, fromKey);
                }

                if (toKey == null) {
                    throw new PrestoException(StandardErrorCode.INVALID_CAST_ARGUMENT, "map key is null");
                }
                writeNativeValue(toKeyType, keyBlockBuilder, toKey);
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        Block keyBlock = keyBlockBuilder.build();

        BlockBuilder mapBlockBuilder = toMapType.createBlockBuilder(new BlockBuilderStatus(), 1);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();
        for (int i = 0; i < fromMap.getPositionCount(); i += 2) {
            if (!typedSet.contains(keyBlock, i / 2)) {
                typedSet.add(keyBlock, i / 2);
                toKeyType.appendTo(keyBlock, i / 2, blockBuilder);
                if (fromMap.isNull(i + 1)) {
                    blockBuilder.appendNull();
                    continue;
                }

                Object fromValue = readNativeValue(fromValueType, fromMap, i + 1);
                try {
                    Object toValue;
                    // TODO: binding `ConnectorSession` should be done in function invocation framework
                    if (!valueCastRequiresSession) {
                        toValue = valueCastFunction.invoke(fromValue);
                    }
                    else {
                        toValue = valueCastFunction.invoke(session, fromValue);
                    }

                    writeNativeValue(toValueType, blockBuilder, toValue);
                }
                catch (Throwable t) {
                    throw internalError(t);
                }
            }
            else {
                // if there are duplicated keys, fail it!
                throw new PrestoException(StandardErrorCode.INVALID_CAST_ARGUMENT, "duplicate keys");
            }
        }

        mapBlockBuilder.closeEntry();
        return (Block) toMapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }
}
