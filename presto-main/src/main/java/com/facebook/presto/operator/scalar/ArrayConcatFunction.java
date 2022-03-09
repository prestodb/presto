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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.gen.VarArgsToArrayAdapterGenerator;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.sql.gen.VarArgsToArrayAdapterGenerator.generateVarArgsToArrayAdapter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.util.Collections.nCopies;

public final class ArrayConcatFunction
        extends SqlScalarFunction
{
    public static final ArrayConcatFunction ARRAY_CONCAT_FUNCTION = new ArrayConcatFunction();

    private static final String FUNCTION_NAME = "concat";
    private static final String DESCRIPTION = "Concatenates given arrays";

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayConcatFunction.class, "concat", Type.class, Block[].class);

    private ArrayConcatFunction()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, FUNCTION_NAME),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("array(E)"),
                ImmutableList.of(parseTypeSignature("array(E)")),
                true));
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
        return DESCRIPTION;
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more arguments to " + FUNCTION_NAME);
        }

        Type elementType = boundVariables.getTypeVariable("E");

        VarArgsToArrayAdapterGenerator.VarArgMethodHandle varArgMethodHandle = generateVarArgsToArrayAdapter(
                Block.class,
                Block.class,
                arity,
                METHOD_HANDLE.bindTo(elementType));

        return new BuiltInScalarFunctionImplementation(
                false,
                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                varArgMethodHandle.getMethodHandle());
    }

    @UsedByGeneratedCode
    public static Block concat(Type elementType, Block[] blocks)
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

        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, resultPositionCount);
        for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
            Block block = blocks[blockIndex];
            for (int i = 0; i < block.getPositionCount(); i++) {
                elementType.appendTo(block, i, blockBuilder);
            }
        }
        return blockBuilder.build();
    }
}
