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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.sql.analyzer.FunctionAndTypeResolver;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.QualifiedName;
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
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.tree.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class FunctionResolution
        implements StandardFunctionResolution
{
    private final FunctionAndTypeResolver functionAndTypeResolver;
    private final List<QualifiedObjectName> windowValueFunctions;

    public FunctionResolution(FunctionAndTypeResolver functionAndTypeResolver)
    {
        this.functionAndTypeResolver = requireNonNull(functionAndTypeResolver, "functionManager is null");
        this.windowValueFunctions = ImmutableList.of(
                functionAndTypeResolver.qualifyObjectName(QualifiedName.of("lead")),
                functionAndTypeResolver.qualifyObjectName(QualifiedName.of("lag")),
                functionAndTypeResolver.qualifyObjectName(QualifiedName.of("first_value")),
                functionAndTypeResolver.qualifyObjectName(QualifiedName.of("last_value")),
                functionAndTypeResolver.qualifyObjectName(QualifiedName.of("nth_value")));
    }

    @Override
    public FunctionHandle notFunction()
    {
        return functionAndTypeResolver.lookupFunction("not", fromTypes(BOOLEAN));
    }

    public boolean isNotFunction(FunctionHandle functionHandle)
    {
        return notFunction().equals(functionHandle);
    }

    @Override
    public FunctionHandle likeVarcharFunction()
    {
        return functionAndTypeResolver.lookupFunction("LIKE", fromTypes(VARCHAR, LIKE_PATTERN));
    }

    public boolean supportsLikePatternFunction()
    {
        try {
            functionAndTypeResolver.lookupFunction("LIKE_PATTERN", fromTypes(VARCHAR, VARCHAR));
            return true;
        }
        catch (PrestoException e) {
            if (e.getErrorCode() == StandardErrorCode.FUNCTION_NOT_FOUND.toErrorCode()) {
                return false;
            }
            throw e;
        }
    }

    public FunctionHandle likeVarcharVarcharFunction()
    {
        return functionAndTypeResolver.lookupFunction("LIKE", fromTypes(VARCHAR, VARCHAR));
    }

    public FunctionHandle likeVarcharVarcharVarcharFunction()
    {
        return functionAndTypeResolver.lookupFunction("LIKE", fromTypes(VARCHAR, VARCHAR, VARCHAR));
    }

    @Override
    public FunctionHandle likeCharFunction(Type valueType)
    {
        checkArgument(valueType instanceof CharType, "Expected CHAR value type");
        return functionAndTypeResolver.lookupFunction("LIKE", fromTypes(valueType, LIKE_PATTERN));
    }

    @Override
    public boolean isLikeFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "LIKE"));
    }

    @Override
    public FunctionHandle likePatternFunction()
    {
        return functionAndTypeResolver.lookupFunction("LIKE_PATTERN", fromTypes(VARCHAR, VARCHAR));
    }

    @Override
    public boolean isLikePatternFunction(FunctionHandle functionHandle)
    {
        QualifiedObjectName name =
                supportsLikePatternFunction() ?
                        functionAndTypeResolver.qualifyObjectName(QualifiedName.of("LIKE_PATTERN")) :
                        QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "LIKE_PATTERN");
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(name);
    }

    @Override
    public boolean isCastFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getOperatorType().equals(Optional.of(OperatorType.CAST));
    }

    public boolean isTryCastFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "TRY_CAST"));
    }

    public boolean isArrayConstructor(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of(ARRAY_CONSTRUCTOR)));
    }

    @Override
    public FunctionHandle betweenFunction(Type valueType, Type lowerBoundType, Type upperBoundType)
    {
        return functionAndTypeResolver.lookupFunction(BETWEEN.getFunctionName().getObjectName(), fromTypes(valueType, lowerBoundType, upperBoundType));
    }

    @Override
    public boolean isBetweenFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getOperatorType().equals(Optional.of(BETWEEN));
    }

    @Override
    public FunctionHandle arithmeticFunction(OperatorType operator, Type leftType, Type rightType)
    {
        checkArgument(operator.isArithmeticOperator(), format("unexpected arithmetic type %s", operator));
        return functionAndTypeResolver.resolveOperator(operator, fromTypes(leftType, rightType));
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
        Optional<OperatorType> operatorType = functionAndTypeResolver.getFunctionMetadata(functionHandle).getOperatorType();
        return operatorType.isPresent() && operatorType.get().isArithmeticOperator();
    }

    @Override
    public FunctionHandle negateFunction(Type type)
    {
        return functionAndTypeResolver.lookupFunction(NEGATION.getFunctionName().getObjectName(), fromTypes(type));
    }

    @Override
    public boolean isNegateFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getOperatorType().equals(Optional.of(NEGATION));
    }

    @Override
    public FunctionHandle arrayConstructor(List<? extends Type> argumentTypes)
    {
        return functionAndTypeResolver.lookupFunction(ARRAY_CONSTRUCTOR, fromTypes(argumentTypes));
    }

    @Override
    public FunctionHandle comparisonFunction(OperatorType operator, Type leftType, Type rightType)
    {
        checkArgument(operator.isComparisonOperator(), format("unexpected comparison type %s", operator));
        return functionAndTypeResolver.resolveOperator(operator, fromTypes(leftType, rightType));
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
        Optional<OperatorType> operatorType = functionAndTypeResolver.getFunctionMetadata(functionHandle).getOperatorType();
        return operatorType.isPresent() && operatorType.get().isComparisonOperator();
    }

    public boolean isEqualsFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getOperatorType().map(EQUAL::equals).orElse(false);
    }

    @Override
    public FunctionHandle subscriptFunction(Type baseType, Type indexType)
    {
        return functionAndTypeResolver.resolveOperator(SUBSCRIPT, fromTypes(baseType, indexType));
    }

    @Override
    public boolean isSubscriptFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getOperatorType().equals(Optional.of(SUBSCRIPT));
    }

    public FunctionHandle tryFunction(Type returnType)
    {
        return functionAndTypeResolver.lookupFunction("$internal$try", fromTypes(returnType));
    }

    public boolean isTryFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().getObjectName().equals("$internal$try");
    }

    public boolean isFailFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("fail")));
    }

    @Override
    public boolean isCountFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("count")));
    }

    @Override
    public boolean isCountIfFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("count_if")));
    }

    @Override
    public FunctionHandle countFunction()
    {
        return functionAndTypeResolver.lookupFunction("count", ImmutableList.of());
    }

    @Override
    public FunctionHandle countFunction(Type valueType)
    {
        return functionAndTypeResolver.lookupFunction("count", fromTypes(valueType));
    }

    public boolean isMaxByFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("max_by")));
    }

    public boolean isMinByFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("min_by")));
    }

    @Override
    public FunctionHandle arbitraryFunction(Type valueType)
    {
        return functionAndTypeResolver.lookupFunction("arbitrary", fromTypes(valueType));
    }

    @Override
    public boolean isMaxFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("max")));
    }

    @Override
    public FunctionHandle maxFunction(Type valueType)
    {
        return functionAndTypeResolver.lookupFunction("max", fromTypes(valueType));
    }

    @Override
    public FunctionHandle greatestFunction(List<Type> valueTypes)
    {
        return functionAndTypeResolver.lookupFunction("greatest", fromTypes(valueTypes));
    }

    @Override
    public boolean isMinFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("min")));
    }

    @Override
    public FunctionHandle minFunction(Type valueType)
    {
        return functionAndTypeResolver.lookupFunction("min", fromTypes(valueType));
    }

    @Override
    public FunctionHandle leastFunction(List<Type> valueTypes)
    {
        return functionAndTypeResolver.lookupFunction("least", fromTypes(valueTypes));
    }

    @Override
    public boolean isApproximateCountDistinctFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("approx_distinct")));
    }

    @Override
    public FunctionHandle approximateCountDistinctFunction(Type valueType)
    {
        return functionAndTypeResolver.lookupFunction("approx_distinct", fromTypes(valueType));
    }

    @Override
    public boolean isApproximateSetFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("approx_set")));
    }

    @Override
    public FunctionHandle approximateSetFunction(Type valueType)
    {
        return functionAndTypeResolver.lookupFunction("approx_set", fromTypes(valueType));
    }

    public boolean isArrayContainsFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("contains")));
    }

    public boolean isElementAtFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("element_at")));
    }

    public boolean isWindowValueFunction(FunctionHandle functionHandle)
    {
        return windowValueFunctions.contains(functionAndTypeResolver.getFunctionMetadata(functionHandle).getName());
    }

    public boolean isMapSubSetFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("map_subset")));
    }

    public boolean isMapFilterFunction(FunctionHandle functionHandle)
    {
        return functionAndTypeResolver.getFunctionMetadata(functionHandle).getName().equals(functionAndTypeResolver.qualifyObjectName(QualifiedName.of("map_filter")));
    }

    @Override
    public FunctionHandle lookupBuiltInFunction(String functionName, List<Type> inputTypes)
    {
        return functionAndTypeResolver.lookupFunction(functionName, fromTypes(inputTypes));
    }
}
