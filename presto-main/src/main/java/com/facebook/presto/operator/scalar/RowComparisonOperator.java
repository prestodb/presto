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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.function.Signature.orderableWithVariadicBound;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.util.Failures.internalError;

public abstract class RowComparisonOperator
        extends SqlOperator
{
    protected RowComparisonOperator(OperatorType operatorType)
    {
        super(operatorType,
                ImmutableList.of(orderableWithVariadicBound("T", StandardTypes.ROW)),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BOOLEAN),
                ImmutableList.of(parseTypeSignature("T"), parseTypeSignature("T")));
    }

    protected List<MethodHandle> getMethodHandles(RowType type, FunctionAndTypeManager functionAndTypeManager, OperatorType operatorType)
    {
        ImmutableList.Builder<MethodHandle> argumentMethods = ImmutableList.builder();
        for (Type parameterType : type.getTypeParameters()) {
            FunctionHandle operatorHandle = functionAndTypeManager.resolveOperator(operatorType, fromTypes(parameterType, parameterType));
            argumentMethods.add(functionAndTypeManager.getBuiltInScalarFunctionImplementation(operatorHandle).getMethodHandle());
        }
        return argumentMethods.build();
    }

    protected static int compare(
            RowType rowType,
            List<MethodHandle> comparisonFunctions,
            Block leftRow,
            Block rightRow)
    {
        for (int i = 0; i < leftRow.getPositionCount(); i++) {
            checkElementNotNull(leftRow.isNull(i), "null value at position " + i);
            checkElementNotNull(rightRow.isNull(i), "null value at position " + i);
            Type type = rowType.getTypeParameters().get(i);
            Object leftElement = readNativeValue(type, leftRow, i);
            Object rightElement = readNativeValue(type, rightRow, i);
            try {
                if ((boolean) comparisonFunctions.get(i).invoke(leftElement, rightElement)) {
                    return 1;
                }
                if ((boolean) comparisonFunctions.get(i).invoke(rightElement, leftElement)) {
                    return -1;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        return 0;
    }
}
