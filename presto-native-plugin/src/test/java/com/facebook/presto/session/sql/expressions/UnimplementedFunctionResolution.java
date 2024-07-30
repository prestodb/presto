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
package com.facebook.presto.session.sql.expressions;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;

import java.util.List;

class UnimplementedFunctionResolution
        implements StandardFunctionResolution
{
    @Override
    public FunctionHandle notFunction()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNotFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle negateFunction(Type type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNegateFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle likeVarcharFunction()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle likeCharFunction(Type valueType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLikeFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle likePatternFunction()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLikePatternFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle arrayConstructor(List<? extends Type> argumentTypes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle arithmeticFunction(OperatorType operator, Type leftType, Type rightType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isArithmeticFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle comparisonFunction(OperatorType operator, Type leftType, Type rightType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isComparisonFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEqualsFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle betweenFunction(Type valueType, Type lowerBoundType, Type upperBoundType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBetweenFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle subscriptFunction(Type baseType, Type indexType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSubscriptFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCastFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCountFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle countFunction()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle countFunction(Type valueType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMaxFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle maxFunction(Type valueType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle greatestFunction(List<Type> valueTypes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMinFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle minFunction(Type valueType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle leastFunction(List<Type> valueTypes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isApproximateCountDistinctFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle approximateCountDistinctFunction(Type valueType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isApproximateSetFunction(FunctionHandle functionHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionHandle approximateSetFunction(Type valueType)
    {
        throw new UnsupportedOperationException();
    }
}
