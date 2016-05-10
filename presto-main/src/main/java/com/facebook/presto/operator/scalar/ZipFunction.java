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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.join;
import static java.lang.invoke.MethodType.methodType;

public final class ZipFunction
        extends SqlScalarFunction
{
    public static final int MIN_ARITY = 2;
    public static final int MAX_ARITY = 4;
    public static final ZipFunction[] ZIP_FUNCTIONS;

    private static final String FUNCTION_NAME = "zip";
    private static final MethodHandle METHOD_HANDLE = methodHandle(ZipFunction.class, "zip", List.class, Block[].class);

    static
    {
        ZIP_FUNCTIONS = new ZipFunction[MAX_ARITY - MIN_ARITY + 1];
        for (int arity = MIN_ARITY; arity <= MAX_ARITY; ++arity) {
            ZIP_FUNCTIONS[arity - MIN_ARITY] = new ZipFunction(arity);
        }
    }

    private List<String> constraintNames;

    private ZipFunction(int arity)
    {
        this(IntStream.rangeClosed(1, arity).mapToObj(s -> "T" + s).collect(toImmutableList()));
    }

    private ZipFunction(List<String> constraintNames)
    {
        super(FUNCTION_NAME,
                constraintNames.stream().map(Signature::typeVariable).collect(toImmutableList()),
                ImmutableList.of(),
                "array(row(" + join(",", constraintNames) + "))",
                constraintNames.stream().map(name -> "array(" + name + ")").collect(toImmutableList()));
        this.constraintNames = constraintNames;
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
        return "Merges the given arrays, element-wise, into a single array of rows.";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        List<Type> constraintTypes = this.constraintNames.stream().map(constraintName -> boundVariables.getTypeVariable(constraintName)).collect(toImmutableList());
        List<Boolean> nullableArguments = constraintTypes.stream().map(constraintType -> false).collect(toImmutableList());
        List<Class<?>> javaArgumentTypes = constraintTypes.stream().map(constraintType -> Block.class).collect(toImmutableList());
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(constraintTypes).asVarargsCollector(Block[].class).asType(methodType(Block.class, javaArgumentTypes));
        return new ScalarFunctionImplementation(false, nullableArguments, methodHandle, isDeterministic());
    }

    @UsedByGeneratedCode
    public static Block zip(List<Type> types, Block... arrays)
    {
        int biggestCardinality = 0;
        for (Block array : arrays) {
            biggestCardinality = Math.max(biggestCardinality, array.getPositionCount());
        }
        RowType rowType = new RowType(types, Optional.empty());
        BlockBuilder outputBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), biggestCardinality);
        for (int outputPosition = 0; outputPosition < biggestCardinality; ++outputPosition) {
            BlockBuilder rowBuilder = outputBuilder.beginBlockEntry();
            for (int fieldIndex = 0; fieldIndex < arrays.length; ++fieldIndex) {
                if (arrays[fieldIndex].getPositionCount() <= outputPosition) {
                    rowBuilder.appendNull();
                }
                else {
                    types.get(fieldIndex).appendTo(arrays[fieldIndex], outputPosition, rowBuilder);
                }
            }
            outputBuilder.closeEntry();
        }
        return outputBuilder.build();
    }
}
