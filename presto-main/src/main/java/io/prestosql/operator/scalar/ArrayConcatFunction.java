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
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.gen.VarArgsToArrayAdapterGenerator;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.gen.VarArgsToArrayAdapterGenerator.generateVarArgsToArrayAdapter;
import static io.prestosql.util.Reflection.methodHandle;
import static java.util.Collections.nCopies;

public final class ArrayConcatFunction
        extends SqlScalarFunction
{
    public static final ArrayConcatFunction ARRAY_CONCAT_FUNCTION = new ArrayConcatFunction();

    private static final String FUNCTION_NAME = "concat";
    private static final String DESCRIPTION = "Concatenates given arrays";

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayConcatFunction.class, "concat", Type.class, Object.class, Block[].class);
    private static final MethodHandle USER_STATE_FACTORY = methodHandle(ArrayConcatFunction.class, "createState", Type.class);

    private ArrayConcatFunction()
    {
        super(new Signature(FUNCTION_NAME,
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("array(E)"),
                ImmutableList.of(parseTypeSignature("array(E)")),
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
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more arguments to " + FUNCTION_NAME);
        }

        Type elementType = boundVariables.getTypeVariable("E");

        VarArgsToArrayAdapterGenerator.MethodHandleAndConstructor methodHandleAndConstructor = generateVarArgsToArrayAdapter(
                Block.class,
                Block.class,
                arity,
                METHOD_HANDLE.bindTo(elementType),
                USER_STATE_FACTORY.bindTo(elementType));

        return new ScalarFunctionImplementation(
                false,
                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandleAndConstructor.getMethodHandle(),
                Optional.of(methodHandleAndConstructor.getConstructor()),
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Object createState(Type elementType)
    {
        return new PageBuilder(ImmutableList.of(elementType));
    }

    @UsedByGeneratedCode
    public static Block concat(Type elementType, Object state, Block[] blocks)
    {
        int resultPositionCount = 0;

        // fast path when there is at most one non empty block
        Block nonEmptyBlock = null;
        for (int i = 0; i < blocks.length; i++) {
            resultPositionCount += blocks[i].getPositionCount();
            if (blocks[i].getPositionCount() > 0) {
                nonEmptyBlock = blocks[i];
            }
        }
        if (nonEmptyBlock == null) {
            return blocks[0];
        }
        if (resultPositionCount == nonEmptyBlock.getPositionCount()) {
            return nonEmptyBlock;
        }

        PageBuilder pageBuilder = (PageBuilder) state;
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
            Block block = blocks[blockIndex];
            for (int i = 0; i < block.getPositionCount(); i++) {
                elementType.appendTo(block, i, blockBuilder);
            }
        }
        pageBuilder.declarePositions(resultPositionCount);
        return blockBuilder.getRegion(blockBuilder.getPositionCount() - resultPositionCount, resultPositionCount);
    }
}
