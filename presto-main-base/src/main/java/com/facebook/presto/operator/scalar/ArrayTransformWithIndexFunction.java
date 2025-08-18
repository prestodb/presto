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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.function.ComplexTypeFunctionDescriptor;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.LambdaArgumentDescriptor;
import com.facebook.presto.spi.function.LambdaDescriptor;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.gen.lambda.BinaryFunctionInterface;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.functionTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public final class ArrayTransformWithIndexFunction
        extends SqlScalarFunction
{
    public static final ArrayTransformWithIndexFunction ARRAY_TRANSFORM_WITH_INDEX_FUNCTION = new ArrayTransformWithIndexFunction();
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayTransformWithIndexFunction.class, "transformWithIndex", Type.class, ArrayType.class, Block.class, BinaryFunctionInterface.class);

    private final ComplexTypeFunctionDescriptor descriptor;

    private ArrayTransformWithIndexFunction()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "transform_with_index"),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T"), typeVariable("U")),
                ImmutableList.of(),
                parseTypeSignature("array(U)"),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature("function(T,bigint,U)")),
                false));
        descriptor = new ComplexTypeFunctionDescriptor(
                true,
                ImmutableList.of(new LambdaDescriptor(1, ImmutableMap.of(0, new LambdaArgumentDescriptor(0, ComplexTypeFunctionDescriptor::prependAllSubscripts)))),
                Optional.of(ImmutableSet.of(0)),
                Optional.of(ComplexTypeFunctionDescriptor::clearRequiredSubfields),
                getSignature());
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return PUBLIC;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return "apply lambda to each element of the array along with its index";
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type inputElementType = boundVariables.getTypeVariable("T");
        Type outputElementType = boundVariables.getTypeVariable("U");
        ArrayType outputArrayType = new ArrayType(outputElementType);
        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        functionTypeArgumentProperty(BinaryFunctionInterface.class)),
                METHOD_HANDLE.bindTo(inputElementType).bindTo(outputArrayType));
    }

    @Override
    public ComplexTypeFunctionDescriptor getComplexTypeFunctionDescriptor()
    {
        return descriptor;
    }

    public static Block transformWithIndex(
            Type inputElementType,
            ArrayType outputArrayType,
            Block inputArray,
            BinaryFunctionInterface function)
    {
        Type outputElementType = outputArrayType.getElementType();
        int positionCount = inputArray.getPositionCount();

        BlockBuilder arrayBlockBuilder = outputArrayType.createBlockBuilder(null, positionCount);
        BlockBuilder blockBuilder = arrayBlockBuilder.beginBlockEntry();

        for (int position = 0; position < positionCount; position++) {
            Object inputElement;
            if (inputArray.isNull(position)) {
                inputElement = null;
            }
            else {
                inputElement = readNativeValue(inputElementType, inputArray, position);
            }

            Object output;
            try {
                // Pass element and 1-based index to the lambda function
                output = function.apply(inputElement, (long) (position + 1));
            }
            catch (Throwable throwable) {
                throw new RuntimeException(format("Error applying lambda function at position %d", position), throwable);
            }
            writeNativeValue(outputElementType, blockBuilder, output);
        }

        arrayBlockBuilder.closeEntry();
        return outputArrayType.getObject(arrayBlockBuilder, arrayBlockBuilder.getPositionCount() - 1);
    }

    private static Object readNativeValue(Type type, Block block, int position)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            return type.getBoolean(block, position);
        }
        else if (javaType == long.class) {
            return type.getLong(block, position);
        }
        else if (javaType == double.class) {
            return type.getDouble(block, position);
        }
        else if (javaType == Slice.class) {
            return type.getSlice(block, position);
        }
        else {
            return type.getObject(block, position);
        }
    }

    private static void writeNativeValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            Class<?> javaType = type.getJavaType();
            if (javaType == boolean.class) {
                type.writeBoolean(blockBuilder, (Boolean) value);
            }
            else if (javaType == long.class) {
                type.writeLong(blockBuilder, (Long) value);
            }
            else if (javaType == double.class) {
                type.writeDouble(blockBuilder, (Double) value);
            }
            else if (javaType == Slice.class) {
                type.writeSlice(blockBuilder, (Slice) value);
            }
            else {
                type.writeObject(blockBuilder, value);
            }
        }
    }
}