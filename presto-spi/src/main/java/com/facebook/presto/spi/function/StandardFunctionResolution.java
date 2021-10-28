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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;

import java.util.List;

public interface StandardFunctionResolution
{
    FunctionHandle notFunction();

    boolean isNotFunction(FunctionHandle functionHandle);

    FunctionHandle negateFunction(Type type);

    boolean isNegateFunction(FunctionHandle functionHandle);

    FunctionHandle likeVarcharFunction();

    FunctionHandle likeCharFunction(Type valueType);

    boolean isLikeFunction(FunctionHandle functionHandle);

    FunctionHandle likePatternFunction();

    boolean isLikePatternFunction(FunctionHandle functionHandle);

    FunctionHandle arrayConstructor(List<? extends Type> argumentTypes);

    FunctionHandle arithmeticFunction(OperatorType operator, Type leftType, Type rightType);

    boolean isArithmeticFunction(FunctionHandle functionHandle);

    FunctionHandle comparisonFunction(OperatorType operator, Type leftType, Type rightType);

    boolean isComparisonFunction(FunctionHandle functionHandle);

    FunctionHandle betweenFunction(Type valueType, Type lowerBoundType, Type upperBoundType);

    boolean isBetweenFunction(FunctionHandle functionHandle);

    FunctionHandle subscriptFunction(Type baseType, Type indexType);

    boolean isSubscriptFunction(FunctionHandle functionHandle);

    boolean isCastFunction(FunctionHandle functionHandle);

    boolean isCountFunction(FunctionHandle functionHandle);

    FunctionHandle countFunction();

    FunctionHandle countFunction(Type valueType);

    boolean isMaxFunction(FunctionHandle functionHandle);

    FunctionHandle maxFunction(Type valueType);

    boolean isMinFunction(FunctionHandle functionHandle);

    FunctionHandle minFunction(Type valueType);
}
