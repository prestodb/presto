package com.facebook.presto.operator.scalar;
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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.util.Reflection.methodHandle;

public class RowEqualOperator
        extends SqlOperator
{
    public static final RowEqualOperator ROW_EQUAL = new RowEqualOperator(EQUAL, true);
    public static final RowEqualOperator ROW_NOT_EQUAL = new RowEqualOperator(NOT_EQUAL, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowEqualOperator.class, "testEquals", Type.class, List.class, Boolean.class, Block.class, Block.class);
    private final Boolean testEqualsTo;

    RowEqualOperator(OperatorType operatorType, Boolean testEqualsTo)
    {
        super(operatorType,
                ImmutableList.of(comparableWithVariadicBound("T", "row")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BOOLEAN),
                ImmutableList.of(parseTypeSignature("T"), parseTypeSignature("T")));
        this.testEqualsTo = testEqualsTo;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        ImmutableList.Builder<MethodHandle> argumentMethods = ImmutableList.builder();
        Type type = boundVariables.getTypeVariable("T");
        for (Type parameterType : type.getTypeParameters()) {
            Signature signature = functionRegistry.resolveOperator(EQUAL, ImmutableList.of(parameterType, parameterType));
            argumentMethods.add(functionRegistry.getScalarFunctionImplementation(signature).getMethodHandle());
        }
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(false, false),
                METHOD_HANDLE.bindTo(type).bindTo(argumentMethods.build()).bindTo(testEqualsTo),
                isDeterministic());
    }

    public static boolean testEquals(Type rowType, List<MethodHandle> argumentMethods, Boolean testEqualsTo, Block leftRow, Block rightRow)
    {
        return equals(rowType, argumentMethods, leftRow, rightRow) == testEqualsTo;
    }

    private static boolean equals(Type rowType, List<MethodHandle> argumentMethods, Block leftRow, Block rightRow)
    {
        List<Type> fieldTypes = rowType.getTypeParameters();
        for (int i = 0; i < leftRow.getPositionCount(); i++) {
            checkElementNotNull(leftRow.isNull(i));
            checkElementNotNull(rightRow.isNull(i));
            Type type = fieldTypes.get(i);
            Object leftValue = readNativeValue(type, leftRow, i);
            Object rightValue = readNativeValue(type, rightRow, i);
            try {
                if (!(boolean) argumentMethods.get(i).invoke(leftValue, rightValue)) {
                    return false;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
        return true;
    }

    private static void checkElementNotNull(boolean isNull)
    {
        if (isNull) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "ROW comparison not supported for fields with null elements");
        }
    }
}
