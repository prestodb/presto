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
package com.facebook.presto.common.function;

import com.facebook.presto.common.QualifiedObjectName;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public enum OperatorType
{
    // TODO: Move out this class. Ideally this class should not be in presto-common module.

    ADD("+", false),
    SUBTRACT("-", false),
    MULTIPLY("*", false),
    DIVIDE("/", false),
    MODULUS("%", false),
    NEGATION("-", false),
    EQUAL("=", false),
    NOT_EQUAL("<>", false),
    LESS_THAN("<", false),
    LESS_THAN_OR_EQUAL("<=", false),
    GREATER_THAN(">", false),
    GREATER_THAN_OR_EQUAL(">=", false),
    BETWEEN("BETWEEN", false),
    CAST("CAST", false),
    SUBSCRIPT("[]", false),
    HASH_CODE("HASH CODE", false),
    SATURATED_FLOOR_CAST("SATURATED FLOOR CAST", false),
    IS_DISTINCT_FROM("IS DISTINCT FROM", true),
    XX_HASH_64("XX HASH 64", false),
    INDETERMINATE("INDETERMINATE", true);

    private static final Map<QualifiedObjectName, OperatorType> OPERATOR_TYPES = Arrays.stream(OperatorType.values()).collect(toMap(OperatorType::getFunctionName, Function.identity()));

    private final String operator;
    private final QualifiedObjectName functionName;
    private final boolean calledOnNullInput;

    OperatorType(String operator, boolean calledOnNullInput)
    {
        this.operator = operator;
        this.functionName = QualifiedObjectName.valueOf("presto", "default", "$operator$" + name());
        this.calledOnNullInput = calledOnNullInput;
    }

    public String getOperator()
    {
        return operator;
    }

    public QualifiedObjectName getFunctionName()
    {
        return functionName;
    }

    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
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

    public static Optional<OperatorType> tryGetOperatorType(QualifiedObjectName operatorName)
    {
        return Optional.ofNullable(OPERATOR_TYPES.get(operatorName));
    }

    public static OperatorType flip(OperatorType operator)
    {
        switch (operator) {
            case EQUAL:
                return EQUAL;
            case NOT_EQUAL:
                return NOT_EQUAL;
            case LESS_THAN:
                return GREATER_THAN;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL;
            case GREATER_THAN:
                return LESS_THAN;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM:
                return IS_DISTINCT_FROM;
            default:
                throw new IllegalArgumentException("Unsupported flip non-comparison operator: " + operator);
        }
    }

    public static OperatorType negate(OperatorType operator)
    {
        switch (operator) {
            case EQUAL:
                return NOT_EQUAL;
            case NOT_EQUAL:
                return EQUAL;
            case LESS_THAN:
                return GREATER_THAN_OR_EQUAL;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN;
            case GREATER_THAN:
                return LESS_THAN_OR_EQUAL;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN;
            default:
                throw new IllegalArgumentException("Unsupported negate non-comparison operator: " + operator);
        }
    }
}
