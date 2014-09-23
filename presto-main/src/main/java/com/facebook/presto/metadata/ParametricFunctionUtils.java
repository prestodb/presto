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
package com.facebook.presto.metadata;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

public final class ParametricFunctionUtils
{
    private ParametricFunctionUtils() {}

    public static Predicate<ParametricFunction> isAggregationPredicate()
    {
        return new Predicate<ParametricFunction>()
        {
            @Override
            public boolean apply(ParametricFunction function)
            {
                return function.isAggregate();
            }
        };
    }

    public static Predicate<ParametricFunction> isHiddenPredicate()
    {
        return new Predicate<ParametricFunction>()
        {
            @Override
            public boolean apply(ParametricFunction function)
            {
                return function.isHidden();
            }
        };
    }

    public static Function<ParametricFunction, String> nameGetter()
    {
        return new Function<ParametricFunction, String>()
        {
            @Override
            public String apply(ParametricFunction input)
            {
                return input.getSignature().getName();
            }
        };
    }
}
