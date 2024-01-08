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
package com.facebook.presto.spi.session;

import com.facebook.presto.common.type.Type;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SessionPropertyMetadata
{
    private final String name;
    private final String description;
    private final Type sqlType;
    private final String defaultValue;
    private final boolean hidden;

    public SessionPropertyMetadata(
            String name,
            String description,
            Type sqlType,
            String defaultValue,
            boolean hidden)
    {
        this.name = requireNonNull(name, "name is null");
        this.description = requireNonNull(description, "description is null");
        this.sqlType = requireNonNull(sqlType, "sqlType is null");
        this.defaultValue = defaultValue;
        this.hidden = hidden;

        if (name.isEmpty() || !name.trim().toLowerCase(ENGLISH).equals(name)) {
            throw new IllegalArgumentException(format("Invalid property name '%s'", name));
        }
        if (description.isEmpty() || !description.trim().equals(description)) {
            throw new IllegalArgumentException(format("Invalid property description '%s'", description));
        }
    }

    /**
     * Name of the property.  This must be a valid identifier.
     */
    public String getName()
    {
        return name;
    }

    /**
     * Description for the end user.
     */
    public String getDescription()
    {
        return description;
    }

    /**
     * SQL type of the property.
     */
    public Type getSqlType()
    {
        return sqlType;
    }

    /**
     * Gets the default value for this property.
     */
    public String getDefaultValue()
    {
        return defaultValue;
    }

    /**
     * Is this property hidden from users?
     */
    public boolean isHidden()
    {
        return hidden;
    }
}
