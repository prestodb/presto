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
package io.prestosql.operator.aggregation;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AggregationHeader
{
    private final String name;
    private final Optional<String> description;
    private final boolean decomposable;
    private final boolean orderSensitive;
    private final boolean hidden;

    public AggregationHeader(String name, Optional<String> description, boolean decomposable, boolean orderSensitive, boolean hidden)
    {
        this.name = requireNonNull(name, "name cannot be null");
        this.description = requireNonNull(description, "description cannot be null");
        this.decomposable = decomposable;
        this.orderSensitive = orderSensitive;
        this.hidden = hidden;
    }

    public String getName()
    {
        return name;
    }

    public Optional<String> getDescription()
    {
        return description;
    }

    public boolean isDecomposable()
    {
        return decomposable;
    }

    public boolean isOrderSensitive()
    {
        return orderSensitive;
    }

    public boolean isHidden()
    {
        return hidden;
    }
}
