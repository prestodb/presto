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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SpecializedFunctionKey
{
    private final SqlFunction function;
    private final Map<String, Type> boundTypeParameters;
    private final List<TypeSignature> parameterTypes;

    public SpecializedFunctionKey(SqlFunction function, Map<String, Type> boundTypeParameters, List<TypeSignature> parameterTypes)
    {
        this.function = requireNonNull(function, "function is null");
        this.boundTypeParameters = requireNonNull(boundTypeParameters, "boundTypeParameters is null");
        this.parameterTypes = requireNonNull(parameterTypes, "parameterTypes is null");
    }

    public SqlFunction getFunction()
    {
        return function;
    }

    public Map<String, Type> getBoundTypeParameters()
    {
        return boundTypeParameters;
    }

    public List<TypeSignature> getParameterTypes()
    {
        return parameterTypes;
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

        return Objects.equals(parameterTypes, that.parameterTypes) &&
                Objects.equals(boundTypeParameters, that.boundTypeParameters) &&
                Objects.equals(function.getSignature(), that.function.getSignature());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function.getSignature(), boundTypeParameters, parameterTypes);
    }
}
