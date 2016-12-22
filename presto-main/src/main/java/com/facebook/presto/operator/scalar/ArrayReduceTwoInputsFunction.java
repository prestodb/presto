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

import static java.lang.Math.min;

public final class ArrayReduceTwoInputsFunction
        extends SqlScalarFunction
{
    public static final ArrayReduceTwoInputsFunction ARRAY_REDUCE_TWO_INPUTS_FUNCTION = new ArrayReduceTwoInputsFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayReduceTwoInputsFunction.class, "reduce", Type.class, Type.class, Block.class, Block.class, Object.class, MethodHandle.class, MethodHandle.class);

    private ArrayReduceTwoInputsFunction()
    {
        super(new Signature(
                "reduce",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T"), typeVariable("Q"), typeVariable("S"), typeVariable("R")),
                ImmutableList.of(),
                parseTypeSignature("R"),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature("array(Q)"), parseTypeSignature("S"), parseTypeSignature("function(S,T,Q,S)"), parseTypeSignature("function(S,R)")),
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
        Type leftInputType = boundVariables.getTypeVariable("T");
        Type rightInputType = boundVariables.getTypeVariable("Q");
        Type intermediateType = boundVariables.getTypeVariable("S");
        Type outputType = boundVariables.getTypeVariable("R");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(leftInputType).bindTo(rightInputType);
        return new ScalarFunctionImplementation(
                true,
                ImmutableList.of(false, true, true, false, false),
                methodHandle.asType(
                        methodHandle.type()
                                .changeParameterType(2, Primitives.wrap(intermediateType.getJavaType()))
                                .changeReturnType(Primitives.wrap(outputType.getJavaType()))),
                isDeterministic());
    }

    public static Object reduce(
            Type leftInputType,
            Type rightInputType,
            Block leftBlock,
            Block rightBlock,
            Object initialIntermediateValue,
            MethodHandle inputFunction,
            MethodHandle outputFunction)
    {
        int leftPositionCount = leftBlock.getPositionCount();
        int rightPositionCount = rightBlock.getPositionCount();
        int minPositionCount = min(leftPositionCount, rightPositionCount);
        Object intermediateValue = initialIntermediateValue;
        for (int position = 0; position < minPositionCount; position++) {
            Object leftInput = readNativeValue(leftInputType, leftBlock, position);
            Object rightInput = readNativeValue(rightInputType, rightBlock, position);
            try {
                intermediateValue = inputFunction.invoke(intermediateValue, leftInput, rightInput);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
        }
        // If the length of the two arrays are different, use null as the rest of elements in the shorter one.
        for (int position = minPositionCount; position < leftPositionCount; position++) {
            try {
                intermediateValue = inputFunction.invoke(intermediateValue, readNativeValue(leftInputType, leftBlock, position), null);
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
        }
        for (int position = minPositionCount; position < rightPositionCount; position++) {
            try {
                intermediateValue = inputFunction.invoke(intermediateValue, null, readNativeValue(rightInputType, rightBlock, position));
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
