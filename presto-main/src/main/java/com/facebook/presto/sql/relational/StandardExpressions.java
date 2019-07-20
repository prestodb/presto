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
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.CastType.TRY_CAST;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.google.common.base.Preconditions.checkArgument;

public class StandardExpressions
{
    private final FunctionManager functionManager;
    private final StandardFunctionResolution functionResolution;

    public StandardExpressions(FunctionManager functionManager)
    {
        this.functionManager = functionManager;
        this.functionResolution = new FunctionResolution(functionManager);
    }

    public boolean isOperation(RowExpression expression, OperatorType type)
    {
        if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            Optional<OperatorType> expresionOperatorType = functionManager.getFunctionMetadata(call.getFunctionHandle()).getOperatorType();
            if (expresionOperatorType.isPresent()) {
                return expresionOperatorType.get() == type;
            }
        }
        return false;
    }

    public RowExpression cast(RowExpression expression, Type targetType)
    {
        return call(functionManager, "CAST", targetType, expression);
    }

    public RowExpression try_cast(RowExpression expression, Type targetType)
    {
        return call(TRY_CAST.name(), functionManager.lookupCast(TRY_CAST, expression.getType().getTypeSignature(), targetType.getTypeSignature()), targetType, expression);
    }

    public CallExpression compare(OperatorType type, RowExpression left, RowExpression right)
    {
        return call(
                type.getFunctionName().getSuffix(),
                functionResolution.comparisonFunction(type, left.getType(), right.getType()),
                BOOLEAN,
                left,
                right);
    }

    public CallExpression arithmetic(OperatorType type, RowExpression left, RowExpression right)
    {
        return call(
                type.getFunctionName().getSuffix(),
                functionResolution.arithmeticFunction(type, left.getType(), right.getType()),
                left.getType(),
                left,
                right);
    }

    public static RowExpression getLeft(RowExpression expression)
    {
        checkArgument(expression instanceof CallExpression && ((CallExpression) expression).getArguments().size() == 2, "must be binary call expression");
        return ((CallExpression) expression).getArguments().get(0);
    }

    public static RowExpression getRight(RowExpression expression)
    {
        checkArgument(expression instanceof CallExpression && ((CallExpression) expression).getArguments().size() == 2, "must be binary call expression");
        return ((CallExpression) expression).getArguments().get(1);
    }

    public boolean isComparison(RowExpression expression)
    {
        if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            return functionResolution.isComparisonFunction(call.getFunctionHandle());
        }
        return false;
    }

    public boolean isDeterministic(RowExpression expression)
    {
        return new RowExpressionDeterminismEvaluator(functionManager).isDeterministic(expression);
    }

    public static boolean isInPredicate(RowExpression expression)
    {
        if (expression instanceof SpecialFormExpression) {
            return ((SpecialFormExpression) expression).getForm() == SpecialFormExpression.Form.IN;
        }
        return false;
    }

    public static RowExpression getInPredicateKey(RowExpression expression)
    {
        return ((SpecialFormExpression) expression).getArguments().get(0);
    }

    public static List<RowExpression> getInPredicateValueList(RowExpression expression)
    {
        List<RowExpression> arguments = ((SpecialFormExpression) expression).getArguments();
        return arguments.subList(1, arguments.size());
    }
}
