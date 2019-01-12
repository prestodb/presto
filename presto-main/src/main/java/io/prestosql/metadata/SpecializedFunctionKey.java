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
package io.prestosql.metadata;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SpecializedFunctionKey
{
    private final SqlFunction function;
    private final BoundVariables boundVariables;
    private final int arity;

    public SpecializedFunctionKey(SqlFunction function, BoundVariables boundVariables, int arity)
    {
        this.function = requireNonNull(function, "function is null");
        this.boundVariables = requireNonNull(boundVariables, "boundVariables is null");
        this.arity = arity;
    }

    public SqlFunction getFunction()
    {
        return function;
    }

    public BoundVariables getBoundVariables()
    {
        return boundVariables;
    }

    public int getArity()
    {
        return arity;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SpecializedFunctionKey that = (SpecializedFunctionKey) o;

        return Objects.equals(boundVariables, that.boundVariables) &&
                Objects.equals(function.getSignature(), that.function.getSignature()) &&
                arity == that.arity;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function.getSignature(), boundVariables, arity);
    }
}
