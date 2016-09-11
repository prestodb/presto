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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Defaults.defaultValue;

public class RowDistinctFromOperator
        extends SqlOperator
{
    public static final RowDistinctFromOperator ROW_DISTINCT_FROM = new RowDistinctFromOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowDistinctFromOperator.class, "isDistinctFrom", Type.class, List.class, Block.class, Block.class);

    private RowDistinctFromOperator()
    {
        super(IS_DISTINCT_FROM,
                ImmutableList.of(comparableWithVariadicBound("T", "row")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BOOLEAN),
                ImmutableList.of(parseTypeSignature("T"), parseTypeSignature("T")));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        ImmutableList.Builder<MethodHandle> argumentMethods = ImmutableList.builder();
        Type type = boundVariables.getTypeVariable("T");
        for (Type parameterType : type.getTypeParameters()) {
            Signature signature = functionRegistry.resolveOperator(IS_DISTINCT_FROM, ImmutableList.of(parameterType, parameterType));
            argumentMethods.add(functionRegistry.getScalarFunctionImplementation(signature).getMethodHandle());
        }
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(true, true),
                METHOD_HANDLE.bindTo(type).bindTo(argumentMethods.build()),
                isDeterministic());
    }

    public static boolean isDistinctFrom(Type rowType, List<MethodHandle> argumentMethods, Block leftRow, Block rightRow)
    {
        boolean leftNull = leftRow == null;
        boolean rightNull = rightRow == null;
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        List<Type> fieldTypes = rowType.getTypeParameters();
        for (int i = 0; i < leftRow.getPositionCount(); i++) {
            Type type = fieldTypes.get(i);
            Object leftValue = readNativeValue(type, leftRow, i);
            boolean leftValueNull = leftValue == null;
            if (leftValueNull) {
                leftValue = defaultValue(type.getJavaType());
            }
            Object rightValue = readNativeValue(type, rightRow, i);
            boolean rightValueNull = rightValue == null;
            if (rightValueNull) {
                rightValue = defaultValue(type.getJavaType());
            }
            try {
                if ((boolean) argumentMethods.get(i).invoke(
                        leftValue,
                        leftValueNull,
                        rightValue,
                        rightValueNull)) {
                    return true;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
        return false;
    }
}
