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
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.util.Reflection.methodHandle;

public class RowEqualOperator
        extends SqlOperator
{
    public static final RowEqualOperator ROW_EQUAL = new RowEqualOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowEqualOperator.class, "equals", List.class, Type.class, Block.class, Block.class);

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
        Type type = boundVariables.getTypeVariable("T");
        List<Type> internalTypes = type.getTypeParameters();
        ImmutableList.Builder<MethodHandle> equalFunctions = ImmutableList.builder();
        for (int i = 0; i < internalTypes.size(); i++) {
            TypeSignature typeSignature = internalTypes.get(i).getTypeSignature();
            Signature signature = internalOperator(
                    EQUAL.name(),
                    BOOLEAN.getTypeSignature(),
                    ImmutableList.of(typeSignature, typeSignature));
            ScalarFunctionImplementation function = functionRegistry.getScalarFunctionImplementation(signature);
            equalFunctions.add(function.getMethodHandle());
        }
        return new ScalarFunctionImplementation(true, ImmutableList.of(false, false), METHOD_HANDLE.bindTo(equalFunctions.build()).bindTo(type), isDeterministic());
    }

    public static Boolean equals(List<MethodHandle> equalFunctions, Type rowType, Block leftRow, Block rightRow)
    {
        boolean foundNull = false;
        List<Type> internalTypes = rowType.getTypeParameters();
        for (int i = 0; i < internalTypes.size(); i++) {
            if (leftRow.isNull(i) || rightRow.isNull(i)) {
                foundNull = true;
                continue;
            }
            Object leftValue = readNativeValue(internalTypes.get(i), leftRow, i);
            Object rightValue = readNativeValue(internalTypes.get(i), rightRow, i);
            try {
                Boolean result = (Boolean) equalFunctions.get(i).invoke(leftValue, rightValue);
                if (result == null) {
                    foundNull = true;
                }
                else if (!result) {
                    return false;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
        if (foundNull) {
            return null;
        }
        return true;
    }
}
