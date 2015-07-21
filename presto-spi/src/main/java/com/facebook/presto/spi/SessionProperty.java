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
package com.facebook.presto.spi;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SessionProperty
{
    private final String name;
    private final String description;

    public SessionProperty(String name, String description)
    {
        this.name = requireNonNull(name);
        this.description = requireNonNull(description);
    }

    public String getName()
    {
        return name;
    }

    public String getDescription()
    {
        return description;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, description);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SessionProperty other = (SessionProperty) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.description, other.description);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        return builder.append("SessionProperty: { name = ").
                append(name).
                append(", description = ").
                append(description).
                append(" }").toString();
    }
}
