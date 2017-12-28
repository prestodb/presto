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
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayIdentityFunction
        extends SqlScalarFunction
{
    public static final ArrayIdentityFunction ARRAY_IDENTITY_FUNCTION = new ArrayIdentityFunction();
    private static final String FUNCTION_NAME = "array_identity";

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayIdentityFunction.class, "identity", Type.class, Type.class, Object.class, Block.class);
    private static final MethodHandle STATE_FACTORY = methodHandle(ArrayIdentityFunction.class, "createState", ArrayType.class);

    private ArrayIdentityFunction()
    {
        super(new Signature(FUNCTION_NAME,
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("array(E)"),
                ImmutableList.of(parseTypeSignature("array(E)")),
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
        return true;
    }

    @Override
    public String getDescription()
    {
        return "array identity";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type elementType = boundVariables.getTypeVariable("E");
        Type arrayType = typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(elementType).bindTo(arrayType);
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(false),
                ImmutableList.of(false),
                ImmutableList.of(Optional.empty()),
                methodHandle,
                Optional.of(STATE_FACTORY.bindTo(arrayType)),
                false,
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Object createState(ArrayType arrayType)
    {
        return new PageBuilder(ImmutableList.of(arrayType));
    }

    public static Block identity(Type type, Type arrayType, Object state, Block array)
    {
        PageBuilder pageBuilder = (PageBuilder) state;
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }
        BlockBuilder arrayBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder blockBuilder = arrayBlockBuilder.beginBlockEntry();

        for (int i = 0; i < array.getPositionCount(); i++) {
            if (array.isNull(i)) {
                blockBuilder.appendNull();
            }
            else {
                type.appendTo(array, i, blockBuilder);
            }
        }

        arrayBlockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return (Block) arrayType.getObject(arrayBlockBuilder, arrayBlockBuilder.getPositionCount() - 1);
    }
}
