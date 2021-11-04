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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.DIVIDE;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.MODULUS;
import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.function.OperatorType.NEGATION;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.function.OperatorType.SUBTRACT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.tree.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class FunctionResolution
        implements StandardFunctionResolution
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public FunctionResolution(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    @Override
    public FunctionHandle notFunction()
    {
        return functionAndTypeManager.lookupFunction("not", fromTypes(BOOLEAN));
    }

    public boolean isNotFunction(FunctionHandle functionHandle)
    {
        return notFunction().equals(functionHandle);
    }

    @Override
    public FunctionHandle likeVarcharFunction()
    {
        return functionAndTypeManager.lookupFunction("LIKE", fromTypes(VARCHAR, LIKE_PATTERN));
    }

    @Override
    public FunctionHandle likeCharFunction(Type valueType)
    {
        checkArgument(valueType instanceof CharType, "Expected CHAR value type");
        return functionAndTypeManager.lookupFunction("LIKE", fromTypes(valueType, LIKE_PATTERN));
    }

    @Override
    public boolean isLikeFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getName().equals(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "LIKE"));
    }

    @Override
    public FunctionHandle likePatternFunction()
    {
        return functionAndTypeManager.lookupFunction("LIKE_PATTERN", fromTypes(VARCHAR, VARCHAR));
    }

    @Override
    public boolean isLikePatternFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getName().equals(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "LIKE_PATTERN"));
    }

    @Override
    public boolean isCastFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getOperatorType().equals(Optional.of(OperatorType.CAST));
    }

    public boolean isTryCastFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getName().equals(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "TRY_CAST"));
    }

    public boolean isArrayConstructor(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getName().equals(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, ARRAY_CONSTRUCTOR));
    }

    @Override
    public FunctionHandle betweenFunction(Type valueType, Type lowerBoundType, Type upperBoundType)
    {
        return functionAndTypeManager.lookupFunction(BETWEEN.getFunctionName().getObjectName(), fromTypes(valueType, lowerBoundType, upperBoundType));
    }

    @Override
    public boolean isBetweenFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getOperatorType().equals(Optional.of(BETWEEN));
    }

    @Override
    public FunctionHandle arithmeticFunction(OperatorType operator, Type leftType, Type rightType)
    {
        checkArgument(operator.isArithmeticOperator(), format("unexpected arithmetic type %s", operator));
        return functionAndTypeManager.resolveOperator(operator, fromTypes(leftType, rightType));
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
        return arithmeticFunction(operatorType, leftType, rightType);
    }

    @Override
    public boolean isArithmeticFunction(FunctionHandle functionHandle)
    {
        Optional<OperatorType> operatorType = functionAndTypeManager.getFunctionMetadata(functionHandle).getOperatorType();
        return operatorType.isPresent() && operatorType.get().isArithmeticOperator();
    }

    @Override
    public FunctionHandle negateFunction(Type type)
    {
        return functionAndTypeManager.lookupFunction(NEGATION.getFunctionName().getObjectName(), fromTypes(type));
    }

    @Override
    public boolean isNegateFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getOperatorType().equals(Optional.of(NEGATION));
    }

    @Override
    public FunctionHandle arrayConstructor(List<? extends Type> argumentTypes)
    {
        return functionAndTypeManager.lookupFunction(ARRAY_CONSTRUCTOR, fromTypes(argumentTypes));
    }

    @Override
    public FunctionHandle comparisonFunction(OperatorType operator, Type leftType, Type rightType)
    {
        checkArgument(operator.isComparisonOperator(), format("unexpected comparison type %s", operator));
        return functionAndTypeManager.resolveOperator(operator, fromTypes(leftType, rightType));
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

        return comparisonFunction(operatorType, leftType, rightType);
    }

    @Override
    public boolean isComparisonFunction(FunctionHandle functionHandle)
    {
        Optional<OperatorType> operatorType = functionAndTypeManager.getFunctionMetadata(functionHandle).getOperatorType();
        return operatorType.isPresent() && operatorType.get().isComparisonOperator();
    }

    @Override
    public FunctionHandle subscriptFunction(Type baseType, Type indexType)
    {
        return functionAndTypeManager.lookupFunction(SUBSCRIPT.getFunctionName().getObjectName(), fromTypes(baseType, indexType));
    }

    @Override
    public boolean isSubscriptFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getOperatorType().equals(Optional.of(SUBSCRIPT));
    }

    public FunctionHandle tryFunction(Type returnType)
    {
        return functionAndTypeManager.lookupFunction("$internal$try", fromTypes(returnType));
    }

    public boolean isTryFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getName().equals("$internal$try");
    }

    public boolean isFailFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getName().equals(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "fail"));
    }

    @Override
    public boolean isCountFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getName().equals(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "count"));
    }

    @Override
    public FunctionHandle countFunction()
    {
        return functionAndTypeManager.lookupFunction("count", ImmutableList.of());
    }

    @Override
    public FunctionHandle countFunction(Type valueType)
    {
        return functionAndTypeManager.lookupFunction("count", fromTypes(valueType));
    }

    @Override
    public boolean isMaxFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getName().equals(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "max"));
    }

    @Override
    public FunctionHandle maxFunction(Type valueType)
    {
        return functionAndTypeManager.lookupFunction("max", fromTypes(valueType));
    }

    @Override
    public boolean isMinFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeManager.getFunctionMetadata(functionHandle).getName().equals(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "min"));
    }

    @Override
    public FunctionHandle minFunction(Type valueType)
    {
        return functionAndTypeManager.lookupFunction("min", fromTypes(valueType));
    }
}
