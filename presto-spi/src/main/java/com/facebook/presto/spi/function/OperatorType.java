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
package com.facebook.presto.spi.function;

import com.facebook.presto.spi.relation.FullyQualifiedName;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public enum OperatorType
{
    ADD("+", false, false),
    SUBTRACT("-", false, false),
    MULTIPLY("*", false, false),
    DIVIDE("/", false, false),
    MODULUS("%", false, false),
    NEGATION("-", false, false),
    EQUAL("=", false, false),
    NOT_EQUAL("<>", false, false),
    LESS_THAN("<", false, false),
    LESS_THAN_OR_EQUAL("<=", false, false),
    GREATER_THAN(">", false, false),
    GREATER_THAN_OR_EQUAL(">=", false, false),
    BETWEEN("BETWEEN", false, false),
    CAST("CAST", false, true),
    SUBSCRIPT("[]", false, true),
    HASH_CODE("HASH CODE", false, false),
    SATURATED_FLOOR_CAST("SATURATED FLOOR CAST", false, false),
    IS_DISTINCT_FROM("IS DISTINCT FROM", true, false),
    XX_HASH_64("XX HASH 64", false, false),
    INDETERMINATE("INDETERMINATE", true, false);

    private static final Map<FullyQualifiedName, OperatorType> OPERATOR_TYPES = Arrays.stream(OperatorType.values()).collect(toMap(OperatorType::getFunctionName, Function.identity()));

    private final String operator;
    private final FullyQualifiedName functionName;
    private final boolean calledOnNullInput;
    private final boolean canReturnNullOnNonNullInput; //default behavior for the operator, can be overwritten for each implementation.

    OperatorType(String operator, boolean calledOnNullInput, boolean canReturnNullOnNonNullInput)
    {
        this.operator = operator;
        this.functionName = FullyQualifiedName.of("presto.default.$operator$" + name());
        this.calledOnNullInput = calledOnNullInput;
        this.canReturnNullOnNonNullInput = canReturnNullOnNonNullInput;
    }

    public String getOperator()
    {
        return operator;
    }

    public FullyQualifiedName getFunctionName()
    {
        return functionName;
    }

    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }

    public boolean isCanReturnNullOnNonNullInput()
    {
        return canReturnNullOnNonNullInput;
    }

    public static Optional<OperatorType> tryGetOperatorType(FullyQualifiedName operatorName)
    {
        return Optional.ofNullable(OPERATOR_TYPES.get(operatorName));
    }

    public boolean isComparisonOperator()
    {
        return this.equals(EQUAL) ||
                this.equals(NOT_EQUAL) ||
                this.equals(LESS_THAN) ||
                this.equals(LESS_THAN_OR_EQUAL) ||
                this.equals(GREATER_THAN) ||
                this.equals(GREATER_THAN_OR_EQUAL) ||
                this.equals(IS_DISTINCT_FROM);
    }

    public boolean isArithmeticOperator()
    {
        return this.equals(ADD) || this.equals(SUBTRACT) || this.equals(MULTIPLY) || this.equals(DIVIDE) || this.equals(MODULUS);
    }
}
