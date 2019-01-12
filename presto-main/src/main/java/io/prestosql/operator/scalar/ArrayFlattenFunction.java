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
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.lang.invoke.MethodHandle;

import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Math.toIntExact;

public class ArrayFlattenFunction
        extends SqlScalarFunction
{
    public static final ArrayFlattenFunction ARRAY_FLATTEN_FUNCTION = new ArrayFlattenFunction();
    private static final String FUNCTION_NAME = "flatten";
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayFlattenFunction.class, FUNCTION_NAME, Type.class, Type.class, Block.class);

    private ArrayFlattenFunction()
    {
        super(new Signature(FUNCTION_NAME,
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("array(E)"),
                ImmutableList.of(parseTypeSignature("array(array(E))")),
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
        return "Flattens the given array";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type elementType = boundVariables.getTypeVariable("E");
        Type arrayType = typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(elementType).bindTo(arrayType);
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    public static Block flatten(Type type, Type arrayType, Block array)
    {
        if (array.getPositionCount() == 0) {
            return type.createBlockBuilder(null, 0).build();
        }

        BlockBuilder builder = type.createBlockBuilder(null, array.getPositionCount(), toIntExact(array.getSizeInBytes() / array.getPositionCount()));
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (!array.isNull(i)) {
                Block subArray = (Block) arrayType.getObject(array, i);
                for (int j = 0; j < subArray.getPositionCount(); j++) {
                    type.appendTo(subArray, j, builder);
                }
            }
        }
        return builder.build();
    }
}
