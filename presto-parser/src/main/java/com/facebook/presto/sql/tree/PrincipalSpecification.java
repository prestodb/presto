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
package com.facebook.presto.sql.tree;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PrincipalSpecification
{
    public enum Type
    {
        UNSPECIFIED, USER, ROLE
    }

    private final Type type;
    private final String name;

    public PrincipalSpecification(Type type, String name)
    {
        this.type = requireNonNull(type, "type is null");
        this.name = requireNonNull(name, "name is null").toLowerCase(ENGLISH);
    }

    public Type getType()
    {
        return type;
    }

    public String getName()
    {
        return name;
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
        PrincipalSpecification that = (PrincipalSpecification) o;
        return type == that.type &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, name);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("name", name)
                .toString();
    }
}
