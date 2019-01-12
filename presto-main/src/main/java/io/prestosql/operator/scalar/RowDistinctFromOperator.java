package io.prestosql.operator.scalar;
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

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionInvoker;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.operator.scalar.ScalarFunctionImplementation.ScalarImplementationChoice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Defaults.defaultValue;
import static io.prestosql.metadata.Signature.comparableWithVariadicBound;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.BLOCK_AND_POSITION;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.util.Failures.internalError;
import static io.prestosql.util.Reflection.methodHandle;

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
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        ImmutableList.Builder<MethodHandle> argumentMethods = ImmutableList.builder();
        Type type = boundVariables.getTypeVariable("T");
        for (Type parameterType : type.getTypeParameters()) {
            Signature signature = functionRegistry.resolveOperator(IS_DISTINCT_FROM, ImmutableList.of(parameterType, parameterType));
            FunctionInvoker functionInvoker = functionRegistry.getFunctionInvokerProvider().createFunctionInvoker(
                    signature,
                    Optional.of(new InvocationConvention(
                            ImmutableList.of(NULL_FLAG, NULL_FLAG),
                            InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL,
                            false)));
            argumentMethods.add(functionInvoker.methodHandle());
        }
        return new ScalarFunctionImplementation(
                ImmutableList.of(
                        new ScalarImplementationChoice(
                                false,
                                ImmutableList.of(valueTypeArgumentProperty(USE_NULL_FLAG), valueTypeArgumentProperty(USE_NULL_FLAG)),
                                METHOD_HANDLE_NULL_FLAG.bindTo(type).bindTo(argumentMethods.build()),
                                Optional.empty()),
                        new ScalarImplementationChoice(
                                false,
                                ImmutableList.of(valueTypeArgumentProperty(BLOCK_AND_POSITION), valueTypeArgumentProperty(BLOCK_AND_POSITION)),
                                METHOD_HANDLE_BLOCK_POSITION.bindTo(type).bindTo(argumentMethods.build()),
                                Optional.empty())),
                isDeterministic());
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
