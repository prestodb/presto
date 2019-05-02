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
package com.facebook.presto.sql.relational;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.QualifiedName;

import java.util.List;

import static com.facebook.presto.metadata.OperatorSignatureUtils.mangleOperatorName;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.DIVIDE;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.MODULUS;
import static com.facebook.presto.spi.function.OperatorType.MULTIPLY;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.tree.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class StandardFunctionResolution
{
    private final FunctionManager functionManager;

    public StandardFunctionResolution(FunctionManager functionManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    public FunctionHandle notFunction()
    {
        return functionManager.lookupFunction(QualifiedName.of("not"), fromTypes(BOOLEAN));
    }

    public FunctionHandle likeVarcharFunction()
    {
        return functionManager.lookupFunction(QualifiedName.of("LIKE"), fromTypes(VARCHAR, LIKE_PATTERN));
    }

    public FunctionHandle likeCharFunction(Type valueType)
    {
        checkArgument(valueType instanceof CharType, "Expected CHAR value type");
        return functionManager.lookupFunction(QualifiedName.of("LIKE"), fromTypes(valueType, LIKE_PATTERN));
    }

    public boolean isLikeFunction(FunctionHandle functionHandle)
    {
        return functionHandle.getSignature().getName().toUpperCase().equals("LIKE");
    }

    public FunctionHandle likePatternFunction()
    {
        return functionManager.lookupFunction(QualifiedName.of("LIKE_PATTERN"), fromTypes(VARCHAR, VARCHAR));
    }

    public boolean isCastFunction(FunctionHandle functionHandle)
    {
        return functionHandle.getSignature().getName().equals(mangleOperatorName(OperatorType.CAST.name()));
    }

    public FunctionHandle arithmeticFunction(ArithmeticBinaryExpression.Operator operator, Type leftType, Type rightType)
    {
        OperatorType operatorType;
        switch (operator) {
            case ADD:
                operatorType = ADD;
                break;
            case SUBTRACT:
                operatorType = SUBTRACT;
                break;
            case MULTIPLY:
                operatorType = MULTIPLY;
                break;
            case DIVIDE:
                operatorType = DIVIDE;
                break;
            case MODULUS:
                operatorType = MODULUS;
                break;
            default:
                throw new IllegalStateException("Unknown arithmetic operator: " + operator);
        }
        return functionManager.resolveOperator(operatorType, fromTypes(leftType, rightType));
    }

    public FunctionHandle arrayConstructor(List<? extends Type> argumentTypes)
    {
        return functionManager.lookupFunction(QualifiedName.of(ARRAY_CONSTRUCTOR), fromTypes(argumentTypes));
    }

    public FunctionHandle comparisonFunction(ComparisonExpression.Operator operator, Type leftType, Type rightType)
    {
        OperatorType operatorType;
        switch (operator) {
            case EQUAL:
                operatorType = EQUAL;
                break;
            case NOT_EQUAL:
                operatorType = NOT_EQUAL;
                break;
            case LESS_THAN:
                operatorType = LESS_THAN;
                break;
            case LESS_THAN_OR_EQUAL:
                operatorType = LESS_THAN_OR_EQUAL;
                break;
            case GREATER_THAN:
                operatorType = GREATER_THAN;
                break;
            case GREATER_THAN_OR_EQUAL:
                operatorType = GREATER_THAN_OR_EQUAL;
                break;
            case IS_DISTINCT_FROM:
                operatorType = IS_DISTINCT_FROM;
                break;
            default:
                throw new IllegalStateException("Unsupported comparison operator type: " + operator);
        }

        return functionManager.resolveOperator(operatorType, fromTypes(leftType, rightType));
    }

    public FunctionHandle tryFunction(Type returnType)
    {
        return functionManager.lookupFunction(QualifiedName.of("TRY"), fromTypes(returnType));
    }
}
