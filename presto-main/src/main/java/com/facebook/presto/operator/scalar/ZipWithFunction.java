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

import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.gen.lambda.BinaryFunctionInterface;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.functionTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.Math.max;

public final class ZipWithFunction
        extends SqlScalarFunction
{
    public static final ZipWithFunction ZIP_WITH_FUNCTION = new ZipWithFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ZipWithFunction.class, "zipWith", Type.class, Type.class, ArrayType.class, Object.class, Block.class, Block.class, BinaryFunctionInterface.class);
    private static final MethodHandle STATE_FACTORY = methodHandle(ZipWithFunction.class, "createState", ArrayType.class);

    private ZipWithFunction()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "zip_with"),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T"), typeVariable("U"), typeVariable("R")),
                ImmutableList.of(),
                parseTypeSignature("array(R)"),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature("array(U)"), parseTypeSignature("function(T,U,R)")),
                false));
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
        return "merge two arrays, element-wise, into a single array using the lambda function";
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type leftElementType = boundVariables.getTypeVariable("T");
        Type rightElementType = boundVariables.getTypeVariable("U");
        Type outputElementType = boundVariables.getTypeVariable("R");
        ArrayType outputArrayType = new ArrayType(outputElementType);
        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        functionTypeArgumentProperty(BinaryFunctionInterface.class)),
                METHOD_HANDLE.bindTo(leftElementType).bindTo(rightElementType).bindTo(outputArrayType),
                Optional.of(STATE_FACTORY.bindTo(outputArrayType)));
    }

    public static Object createState(ArrayType arrayType)
    {
        return new PageBuilder(ImmutableList.of(arrayType));
    }

    public static Block zipWith(
            Type leftElementType,
            Type rightElementType,
            ArrayType outputArrayType,
            Object state,
            Block leftBlock,
            Block rightBlock,
            BinaryFunctionInterface function)
    {
        Type outputElementType = outputArrayType.getElementType();
        int leftPositionCount = leftBlock.getPositionCount();
        int rightPositionCount = rightBlock.getPositionCount();
        int outputPositionCount = max(leftPositionCount, rightPositionCount);

        PageBuilder pageBuilder = (PageBuilder) state;
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }
        BlockBuilder arrayBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder blockBuilder = arrayBlockBuilder.beginBlockEntry();

        for (int position = 0; position < outputPositionCount; position++) {
            Object left = position < leftPositionCount ? readNativeValue(leftElementType, leftBlock, position) : null;
            Object right = position < rightPositionCount ? readNativeValue(rightElementType, rightBlock, position) : null;
            Object output;
            try {
                output = function.apply(left, right);
            }
            catch (Throwable throwable) {
                // Restore pageBuilder into a consistent state.
                arrayBlockBuilder.closeEntry();
                pageBuilder.declarePosition();

                throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
            writeNativeValue(outputElementType, blockBuilder, output);
        }

        arrayBlockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return outputArrayType.getObject(arrayBlockBuilder, arrayBlockBuilder.getPositionCount() - 1);
    }
}
