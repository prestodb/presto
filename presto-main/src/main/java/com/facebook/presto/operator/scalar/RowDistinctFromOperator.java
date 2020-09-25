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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.FunctionInvoker;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ReturnPlaceConvention;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ScalarImplementationChoice;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.InvocationConvention;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.BLOCK_AND_POSITION;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static com.facebook.presto.spi.function.Signature.comparableWithVariadicBound;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.Failures.internalError;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Defaults.defaultValue;

public class RowDistinctFromOperator
        extends SqlOperator
{
    public static final RowDistinctFromOperator ROW_DISTINCT_FROM = new RowDistinctFromOperator();
    private static final MethodHandle METHOD_HANDLE_NULL_FLAG = methodHandle(RowDistinctFromOperator.class, "isDistinctFrom", Type.class, List.class, Block.class, boolean.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_BLOCK_POSITION = methodHandle(RowDistinctFromOperator.class, "isDistinctFrom", Type.class, List.class, Block.class, int.class, Block.class, int.class);

    private RowDistinctFromOperator()
    {
        super(IS_DISTINCT_FROM,
                ImmutableList.of(comparableWithVariadicBound("T", "row")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BOOLEAN),
                ImmutableList.of(parseTypeSignature("T"), parseTypeSignature("T")));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        ImmutableList.Builder<MethodHandle> argumentMethods = ImmutableList.builder();
        Type type = boundVariables.getTypeVariable("T");
        for (Type parameterType : type.getTypeParameters()) {
            FunctionHandle operatorHandle = functionAndTypeManager.resolveOperator(IS_DISTINCT_FROM, fromTypes(parameterType, parameterType));
            FunctionInvoker functionInvoker = functionAndTypeManager.getFunctionInvokerProvider().createFunctionInvoker(
                    operatorHandle,
                    Optional.of(new InvocationConvention(
                            ImmutableList.of(NULL_FLAG, NULL_FLAG),
                            InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL,
                            false)));
            argumentMethods.add(functionInvoker.methodHandle());
        }
        return new BuiltInScalarFunctionImplementation(
                ImmutableList.of(
                        new ScalarImplementationChoice(
                                false,
                                ImmutableList.of(valueTypeArgumentProperty(USE_NULL_FLAG), valueTypeArgumentProperty(USE_NULL_FLAG)),
                                ReturnPlaceConvention.STACK,
                                METHOD_HANDLE_NULL_FLAG.bindTo(type).bindTo(argumentMethods.build()),
                                Optional.empty()),
                        new ScalarImplementationChoice(
                                false,
                                ImmutableList.of(valueTypeArgumentProperty(BLOCK_AND_POSITION), valueTypeArgumentProperty(BLOCK_AND_POSITION)),
                                ReturnPlaceConvention.STACK,
                                METHOD_HANDLE_BLOCK_POSITION.bindTo(type).bindTo(argumentMethods.build()),
                                Optional.empty())));
    }

    public static boolean isDistinctFrom(Type rowType, List<MethodHandle> argumentMethods, Block leftRow, boolean leftNull, Block rightRow, boolean rightNull)
    {
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
                throw internalError(t);
            }
        }
        return false;
    }

    public static boolean isDistinctFrom(Type rowType, List<MethodHandle> argumentMethods, Block leftRow, int leftPosition, Block rightRow, int rightPosition)
    {
        return isDistinctFrom(
                rowType,
                argumentMethods,
                (Block) rowType.getObject(leftRow, leftPosition),
                leftRow.isNull(leftPosition),
                (Block) rowType.getObject(rightRow, rightPosition),
                rightRow.isNull(rightPosition));
    }
}
