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

import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.QualifiedName;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class FirstOrderFunctionKey
{
    private final QualifiedName name;
    private final List<TypeSignature> parameterTypes;

    public FirstOrderFunctionKey(QualifiedName name, List<TypeSignature> parameterTypes)
    {
        this.name = requireNonNull(name, "name is null");
        this.parameterTypes = requireNonNull(parameterTypes, "parameterTypes is null");
    }

    public QualifiedName getName()
    {
        return name;
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

        FirstOrderFunctionKey that = (FirstOrderFunctionKey) o;

        return Objects.equals(this.name, that.name) &&
                Objects.equals(this.parameterTypes, that.parameterTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, parameterTypes);
    }
}
