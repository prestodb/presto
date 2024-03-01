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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.gen.lambda.BinaryFunctionInterface;
import com.facebook.presto.sql.gen.lambda.UnaryFunctionInterface;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.functionTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ArrayReduceFunction
        extends SqlScalarFunction
{
    public static final ArrayReduceFunction ARRAY_REDUCE_FUNCTION = new ArrayReduceFunction();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayReduceFunction.class, "reduce", Type.class, Block.class, Object.class, BinaryFunctionInterface.class, UnaryFunctionInterface.class);

    private ArrayReduceFunction()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "reduce"),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T"), typeVariable("S"), typeVariable("R")),
                ImmutableList.of(),
                parseTypeSignature("R"),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature("S"), parseTypeSignature("function(S,T,S)"), parseTypeSignature("function(S,R)")),
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
    public boolean isCalledOnNullInput()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Reduce elements of the array into a single value";
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type inputType = boundVariables.getTypeVariable("T");
        Type intermediateType = boundVariables.getTypeVariable("S");
        Type outputType = boundVariables.getTypeVariable("R");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(inputType);
        return new BuiltInScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(USE_BOXED_TYPE),
                        functionTypeArgumentProperty(BinaryFunctionInterface.class),
                        functionTypeArgumentProperty(UnaryFunctionInterface.class)),
                methodHandle.asType(
                        methodHandle.type()
                                .changeParameterType(1, Primitives.wrap(intermediateType.getJavaType()))
                                .changeReturnType(Primitives.wrap(outputType.getJavaType()))));
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
