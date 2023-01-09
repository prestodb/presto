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

package com.facebook.presto.hive.functions.aggregation;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HiveAggregationFunctionDescription
{
    private final QualifiedObjectName name;
    private final List<Type> parameterTypes;
    private final List<Type> intermediateTypes;
    private final Type finalType;
    private final boolean decomposable;
    private final boolean orderSensitive;

    public HiveAggregationFunctionDescription(
            QualifiedObjectName name,
            List<Type> parameterTypes,
            List<Type> intermediateTypes,
            Type finalType,
            boolean decomposable,
            boolean orderSensitive)
    {
        this.name = requireNonNull(name);
        this.parameterTypes = requireNonNull(parameterTypes);
        this.intermediateTypes = requireNonNull(intermediateTypes);
        this.finalType = requireNonNull(finalType);
        this.decomposable = decomposable;
        this.orderSensitive = orderSensitive;
    }

    public String getName()
    {
        return name.getObjectName();
    }

    public List<Type> getParameterTypes()
    {
        return parameterTypes;
    }

    public List<Type> getIntermediateTypes()
    {
        return intermediateTypes;
    }

    public Type getFinalType()
    {
        return finalType;
    }

    public boolean isDecomposable()
    {
        return decomposable;
    }

    public boolean isOrderSensitive()
    {
        return orderSensitive;
    }
}
