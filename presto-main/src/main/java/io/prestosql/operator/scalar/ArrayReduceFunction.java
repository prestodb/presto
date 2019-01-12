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
import com.google.common.primitives.Primitives;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.gen.lambda.BinaryFunctionInterface;
import io.prestosql.sql.gen.lambda.UnaryFunctionInterface;

import java.lang.invoke.MethodHandle;

import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.functionTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.util.Reflection.methodHandle;

public final class ArrayReduceFunction
        extends SqlScalarFunction
{
    public static final ArrayReduceFunction ARRAY_REDUCE_FUNCTION = new ArrayReduceFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayReduceFunction.class, "reduce", Type.class, Block.class, Object.class, BinaryFunctionInterface.class, UnaryFunctionInterface.class);

    private ArrayReduceFunction()
    {
        super(new Signature(
                "reduce",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T"), typeVariable("S"), typeVariable("R")),
                ImmutableList.of(),
                parseTypeSignature("R"),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature("S"), parseTypeSignature("function(S,T,S)"), parseTypeSignature("function(S,R)")),
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
        return "Reduce elements of the array into a single value";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type inputType = boundVariables.getTypeVariable("T");
        Type intermediateType = boundVariables.getTypeVariable("S");
        Type outputType = boundVariables.getTypeVariable("R");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(inputType);
        return new ScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        functionTypeArgumentProperty(BinaryFunctionInterface.class),
                        functionTypeArgumentProperty(UnaryFunctionInterface.class)),
                methodHandle.asType(
                        methodHandle.type()
                                .changeParameterType(1, Primitives.wrap(intermediateType.getJavaType()))
                                .changeReturnType(Primitives.wrap(outputType.getJavaType()))),
                isDeterministic());
    }

    public static Object reduce(
            Type inputType,
            Block block,
            Object initialIntermediateValue,
            BinaryFunctionInterface inputFunction,
            UnaryFunctionInterface outputFunction)
    {
        int positionCount = block.getPositionCount();
        Object intermediateValue = initialIntermediateValue;
        for (int position = 0; position < positionCount; position++) {
            Object input = readNativeValue(inputType, block, position);
            intermediateValue = inputFunction.apply(intermediateValue, input);
        }
        return outputFunction.apply(intermediateValue);
    }
}
