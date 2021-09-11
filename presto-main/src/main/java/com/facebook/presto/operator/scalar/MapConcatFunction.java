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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.SingleMapBlock;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.aggregation.OptimizedTypedSet;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.gen.VarArgsToArrayAdapterGenerator.VarArgMethodHandle;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
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

    private static final MethodHandle METHOD_HANDLE = methodHandle(MapConcatFunction.class, "mapConcat", MapType.class, Block[].class);

    private MapConcatFunction()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, FUNCTION_NAME),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("map(K,V)"),
                ImmutableList.of(parseTypeSignature("map(K,V)")),
                true));
    }

    @Override
    public final SqlFunctionVisibility getVisibility()
    {
        return PUBLIC;
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
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments to " + FUNCTION_NAME);
        }

        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        MapType mapType = (MapType) functionAndTypeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));

        VarArgMethodHandle varArgMethodHandle = generateVarArgsToArrayAdapter(
                Block.class,
                Block.class,
                arity,
                METHOD_HANDLE.bindTo(mapType));

        return new BuiltInScalarFunctionImplementation(
                false,
                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                varArgMethodHandle.getMethodHandle());
    }

    @UsedByGeneratedCode
    public static Block mapConcat(MapType mapType, Block[] maps)
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

        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();

        // We need to divide the entries by 2 because the maps array is SingleMapBlocks and it had the positionCount twice as large as a normal Block
        OptimizedTypedSet typedSet = new OptimizedTypedSet(keyType, maps.length, entries / 2);
        for (int i = lastMapIndex; i >= firstMapIndex; i--) {
            SingleMapBlock singleMapBlock = (SingleMapBlock) maps[i];
            Block keyBlock = singleMapBlock.getKeyBlock();
            typedSet.union(keyBlock);
        }

        List<SelectedPositions> selectedPositionsList = typedSet.getPositionsForBlocks();
        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, selectedPositionsList.size());
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();

        for (int i = lastMapIndex; i >= firstMapIndex; i--) {
            SingleMapBlock singleMapBlock = (SingleMapBlock) maps[i];
            // selectedPositions was ordered by addUnion sequence therefore the order should be reversed.
            SelectedPositions selectedPositions = selectedPositionsList.get(lastMapIndex - i);
            assert selectedPositions.isList();

            int[] positions = selectedPositions.getPositions();
            for (int j = 0; j < selectedPositions.size(); j++) {
                int position = positions[j];
                keyType.appendTo(singleMapBlock, 2 * position, blockBuilder);
                valueType.appendTo(singleMapBlock, 2 * position + 1, blockBuilder);
            }
        }

        mapBlockBuilder.closeEntry();
        return mapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }
}
