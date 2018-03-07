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
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.sql.gen.VarArgsToArrayAdapterGenerator.MethodHandleAndConstructor;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.gen.VarArgsToArrayAdapterGenerator.generateVarArgsToArrayAdapter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.Math.min;
import static java.util.Collections.nCopies;

public final class MapConcatFunction
        extends SqlScalarFunction
{
    public static final MapConcatFunction MAP_CONCAT_FUNCTION = new MapConcatFunction();

    private static final String FUNCTION_NAME = "map_concat";
    private static final String DESCRIPTION = "Concatenates given maps";

    private static final MethodHandle USER_STATE_FACTORY = methodHandle(MapConcatFunction.class, "createMapState", MapType.class);
    private static final MethodHandle METHOD_HANDLE = methodHandle(MapConcatFunction.class, "mapConcat", MapType.class, Object.class, Block[].class);

    private MapConcatFunction()
    {
        super(new Signature(FUNCTION_NAME,
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("map(K,V)"),
                ImmutableList.of(parseTypeSignature("map(K,V)")),
                true));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return DESCRIPTION;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments to " + FUNCTION_NAME);
        }

        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        MapType mapType = (MapType) typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));

        MethodHandleAndConstructor methodHandleAndConstructor = generateVarArgsToArrayAdapter(
                Block.class,
                Block.class,
                arity,
                METHOD_HANDLE.bindTo(mapType),
                USER_STATE_FACTORY.bindTo(mapType));

        return new ScalarFunctionImplementation(
                false,
                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandleAndConstructor.getMethodHandle(),
                Optional.of(methodHandleAndConstructor.getConstructor()),
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Object createMapState(MapType mapType)
    {
        return new PageBuilder(ImmutableList.of(mapType));
    }

    @UsedByGeneratedCode
    public static Block mapConcat(MapType mapType, Object state, Block[] maps)
    {
        int entries = 0;
        int lastMapIndex = maps.length - 1;
        int firstMapIndex = lastMapIndex;
        for (int i = 0; i < maps.length; i++) {
            entries += maps[i].getPositionCount();
            if (maps[i].getPositionCount() > 0) {
                lastMapIndex = i;
                firstMapIndex = min(firstMapIndex, i);
            }
        }
        if (lastMapIndex == firstMapIndex) {
            return maps[lastMapIndex];
        }

        PageBuilder pageBuilder = (PageBuilder) state;
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        // TODO: we should move TypedSet into user state as well
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        TypedSet typedSet = new TypedSet(keyType, entries / 2, FUNCTION_NAME);
        BlockBuilder mapBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();

        // the last map
        Block map = maps[lastMapIndex];
        for (int i = 0; i < map.getPositionCount(); i += 2) {
            typedSet.add(map, i);
            keyType.appendTo(map, i, blockBuilder);
            valueType.appendTo(map, i + 1, blockBuilder);
        }
        // the map between the last and the first
        for (int idx = lastMapIndex - 1; idx > firstMapIndex; idx--) {
            map = maps[idx];
            for (int i = 0; i < map.getPositionCount(); i += 2) {
                if (!typedSet.contains(map, i)) {
                    typedSet.add(map, i);
                    keyType.appendTo(map, i, blockBuilder);
                    valueType.appendTo(map, i + 1, blockBuilder);
                }
            }
        }
        // the first map
        map = maps[firstMapIndex];
        for (int i = 0; i < map.getPositionCount(); i += 2) {
            if (!typedSet.contains(map, i)) {
                keyType.appendTo(map, i, blockBuilder);
                valueType.appendTo(map, i + 1, blockBuilder);
            }
        }

        mapBlockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return mapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }
}
