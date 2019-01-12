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
package io.prestosql.spi.type;

import java.util.Objects;
import java.util.Optional;

public class NamedType
{
    private final Optional<RowFieldName> name;
    private final Type type;

    public NamedType(Optional<RowFieldName> name, Type type)
    {
        this.name = name;
        this.type = type;
    }

    public Optional<RowFieldName> getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
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

        NamedType other = (NamedType) o;

        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }
}
