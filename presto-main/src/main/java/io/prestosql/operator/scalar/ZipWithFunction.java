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
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.gen.lambda.BinaryFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.functionTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.spi.type.TypeUtils.writeNativeValue;
import static io.prestosql.util.Reflection.methodHandle;
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
                "zip_with",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T"), typeVariable("U"), typeVariable("R")),
                ImmutableList.of(),
                parseTypeSignature("array(R)"),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature("array(U)"), parseTypeSignature("function(T,U,R)")),
                false));
    }

    @Override
    public boolean isHidden()
    {
        return false;
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
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type leftElementType = boundVariables.getTypeVariable("T");
        Type rightElementType = boundVariables.getTypeVariable("U");
        Type outputElementType = boundVariables.getTypeVariable("R");
        ArrayType outputArrayType = new ArrayType(outputElementType);
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        functionTypeArgumentProperty(BinaryFunctionInterface.class)),
                METHOD_HANDLE.bindTo(leftElementType).bindTo(rightElementType).bindTo(outputArrayType),
                Optional.of(STATE_FACTORY.bindTo(outputArrayType)),
                isDeterministic());
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
