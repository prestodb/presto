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
package com.facebook.presto.spi.predicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class TupleExpression<C>
{
    Map<C, NullableValue> extractFixedValues()
    {
        return new HashMap<>();
    }

    public List<C> getColumnDomains()
    {
        return new ArrayList<C>();
    }

    public abstract <U> TupleExpression transform(Function<C, U> function);

    boolean isAll()
    {
        return false;
    }

    boolean isNone()
    {
        return false;
    }

    public abstract <R, T> R accept(TupleExpressionVisitor<R, T> visitor, T context);

    public abstract String getName();
}
