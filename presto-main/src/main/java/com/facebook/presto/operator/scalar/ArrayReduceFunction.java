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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ArrayReduceFunction
        extends SqlScalarFunction
{
    public static final ArrayReduceFunction ARRAY_REDUCE_FUNCTION = new ArrayReduceFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayReduceFunction.class, "reduce", Type.class, Block.class, Object.class, MethodHandle.class, MethodHandle.class);

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
                ImmutableList.of(false, true, false, false),
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
            MethodHandle inputFunction,
            MethodHandle outputFunction)
    {
        int positionCount = block.getPositionCount();
        Object intermediateValue = initialIntermediateValue;
        for (int position = 0; position < positionCount; position++) {
            Object input = readNativeValue(inputType, block, position);
            try {
                intermediateValue = inputFunction.invoke(intermediateValue, input);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
        }
        try {
            return outputFunction.invoke(intermediateValue);
        }
        catch (Throwable throwable) {
            throw Throwables.propagate(throwable);
        }
    }
}
