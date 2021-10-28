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
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.join;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;

public final class ZipFunction
        extends SqlScalarFunction
{
    public static final int MIN_ARITY = 2;
    public static final int MAX_ARITY = 7;
    public static final ZipFunction[] ZIP_FUNCTIONS;

    private static final MethodHandle METHOD_HANDLE = methodHandle(ZipFunction.class, "zip", List.class, Block[].class);

    static {
        ZIP_FUNCTIONS = new ZipFunction[MAX_ARITY - MIN_ARITY + 1];
        for (int arity = MIN_ARITY; arity <= MAX_ARITY; arity++) {
            ZIP_FUNCTIONS[arity - MIN_ARITY] = new ZipFunction(arity);
        }
    }

    private final List<String> typeParameters;

    private ZipFunction(int arity)
    {
        this(IntStream.rangeClosed(1, arity).mapToObj(s -> "T" + s).collect(toImmutableList()));
    }

    private ZipFunction(List<String> typeParameters)
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "zip"),
                FunctionKind.SCALAR,
                typeParameters.stream().map(Signature::typeVariable).collect(toImmutableList()),
                ImmutableList.of(),
                parseTypeSignature("array(row(" + join(",", typeParameters) + "))"),
                typeParameters.stream().map(name -> "array(" + name + ")").map(TypeSignature::parseTypeSignature).collect(toImmutableList()),
                false));
        this.typeParameters = typeParameters;
    }

    @Override
    public SqlFunctionVisibility getVisibility()
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
        return "Merges the given arrays, element-wise, into a single array of rows.";
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        List<Type> types = this.typeParameters.stream().map(boundVariables::getTypeVariable).collect(toImmutableList());
        List<ArgumentProperty> argumentProperties = nCopies(types.size(), valueTypeArgumentProperty(RETURN_NULL_ON_NULL));
        List<Class<?>> javaArgumentTypes = nCopies(types.size(), Block.class);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(types).asVarargsCollector(Block[].class).asType(methodType(Block.class, javaArgumentTypes));
        return new BuiltInScalarFunctionImplementation(false, argumentProperties, methodHandle);
    }

    @UsedByGeneratedCode
    public static Block zip(List<Type> types, Block... arrays)
    {
        int biggestCardinality = 0;
        for (Block array : arrays) {
            biggestCardinality = Math.max(biggestCardinality, array.getPositionCount());
        }
        RowType rowType = RowType.anonymous(types);
        BlockBuilder outputBuilder = rowType.createBlockBuilder(null, biggestCardinality);
        for (int outputPosition = 0; outputPosition < biggestCardinality; outputPosition++) {
            BlockBuilder rowBuilder = outputBuilder.beginBlockEntry();
            for (int fieldIndex = 0; fieldIndex < arrays.length; fieldIndex++) {
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
