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

import com.facebook.presto.common.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SessionPropertyMetadata
{
    private final String name;

    private final String description;

    private final TypeSignature typeSignature;

    private final String defaultValue;

    private final boolean hidden;

    @JsonCreator
    public SessionPropertyMetadata(
            @JsonProperty("name") String name,
            @JsonProperty("description") String description,
            @JsonProperty("typeSignature") TypeSignature typeSignature,
            @JsonProperty("defaultValue") String defaultValue,
            @JsonProperty("hidden") boolean hidden)
    {
        this.name = requireNonNull(name, "name is null");
        this.description = requireNonNull(description, "description is null");
        this.typeSignature = requireNonNull(typeSignature, "typeSignature is null");
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
    @JsonProperty
    public String getName()
    {
        return name;
    }

    /**
     * Description for the end user.
     */
    @JsonProperty
    public String getDescription()
    {
        return description;
    }

    /**
     * TypeSignature of the property.
     */
    @JsonProperty
    public TypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    /**
     * Gets the default value for this property.
     */
    @JsonProperty
    public String getDefaultValue()
    {
        return defaultValue;
    }

    /**
     * Is this property hidden from users?
     */
    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SessionPropertyMetadata)) {
            return false;
        }
        SessionPropertyMetadata that = (SessionPropertyMetadata) o;
        return hidden == that.hidden && Objects.equals(name, that.name) && Objects.equals(description, that.description) && Objects.equals(typeSignature, that.typeSignature) && Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, description, typeSignature, defaultValue, hidden);
    }
}
