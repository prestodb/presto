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

import com.google.common.collect.ImmutableList;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.operator.aggregation.TypedSet;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.sql.gen.VarArgsToArrayAdapterGenerator.MethodHandleAndConstructor;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.gen.VarArgsToArrayAdapterGenerator.generateVarArgsToArrayAdapter;
import static io.prestosql.util.Reflection.methodHandle;
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
