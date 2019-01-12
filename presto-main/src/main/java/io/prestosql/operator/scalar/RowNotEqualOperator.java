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
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.prestosql.metadata.Signature.comparableWithVariadicBound;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Reflection.methodHandle;

public class RowNotEqualOperator
        extends SqlOperator
{
    public static final RowNotEqualOperator ROW_NOT_EQUAL = new RowNotEqualOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowNotEqualOperator.class, "notEqual", RowType.class, List.class, Block.class, Block.class);

    private RowNotEqualOperator()
    {
        super(NOT_EQUAL,
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
                        .bindTo(RowEqualOperator.resolveFieldEqualOperators(type, functionRegistry)),
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Boolean notEqual(RowType rowType, List<MethodHandle> fieldEqualOperators, Block leftRow, Block rightRow)
    {
        Boolean result = RowEqualOperator.equals(rowType, fieldEqualOperators, leftRow, rightRow);
        if (result == null) {
            return null;
        }
        return !result;
    }
}
