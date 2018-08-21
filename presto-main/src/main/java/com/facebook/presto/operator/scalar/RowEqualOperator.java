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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.util.Failures.internalError;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class RowEqualOperator
        extends SqlOperator
{
    public static final RowEqualOperator ROW_EQUAL = new RowEqualOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowEqualOperator.class, "equals", RowType.class, List.class, Block.class, Block.class);

    private RowEqualOperator()
    {
        super(EQUAL,
                ImmutableList.of(comparableWithVariadicBound("T", "row")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BOOLEAN),
                ImmutableList.of(parseTypeSignature("T"), parseTypeSignature("T")));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        RowType type = (RowType) boundVariables.getTypeVariable("T");
        return new ScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                METHOD_HANDLE
                        .bindTo(type)
                        .bindTo(resolveFieldEqualOperators(type, functionRegistry)),
                isDeterministic());
    }

    public static List<MethodHandle> resolveFieldEqualOperators(RowType rowType, FunctionRegistry functionRegistry)
    {
        return rowType.getTypeParameters().stream()
                .map(type -> resolveEqualOperator(type, functionRegistry))
                .collect(toImmutableList());
    }

    private static MethodHandle resolveEqualOperator(Type type, FunctionRegistry functionRegistry)
    {
        Signature operator = functionRegistry.resolveOperator(EQUAL, ImmutableList.of(type, type));
        ScalarFunctionImplementation implementation = functionRegistry.getScalarFunctionImplementation(operator);
        return implementation.getMethodHandle();
    }

    public static Boolean equals(RowType rowType, List<MethodHandle> fieldEqualOperators, Block leftRow, Block rightRow)
    {
        boolean indeterminate = false;
        for (int fieldIndex = 0; fieldIndex < leftRow.getPositionCount(); fieldIndex++) {
            if (leftRow.isNull(fieldIndex) || rightRow.isNull(fieldIndex)) {
                indeterminate = true;
                continue;
            }
            Type fieldType = rowType.getTypeParameters().get(fieldIndex);
            Object leftField = readNativeValue(fieldType, leftRow, fieldIndex);
            Object rightField = readNativeValue(fieldType, rightRow, fieldIndex);
            try {
                MethodHandle equalOperator = fieldEqualOperators.get(fieldIndex);
                Boolean result = (Boolean) equalOperator.invoke(leftField, rightField);
                if (result == null) {
                    indeterminate = true;
                }
                else if (!result) {
                    return false;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }

        if (indeterminate) {
            return null;
        }
        return true;
    }
}
