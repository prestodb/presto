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
package com.facebook.presto.spi.function.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.annotation.Nullable;

import static com.facebook.presto.spi.function.table.Preconditions.checkArgument;
import static com.facebook.presto.spi.function.table.Preconditions.checkNotNullOrEmpty;

/**
 * Abstract class to capture the three supported argument types for a table function:
 * - Table arguments
 * - Descriptor arguments
 * - SQL scalar arguments
 * <p>
 * Each argument is named, and either passed positionally or in a `arg_name => value` convention.
 * <p>
 * Default values are allowed for all arguments except Table arguments.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DescriptorArgumentSpecification.class, name = "descriptor"),
        @JsonSubTypes.Type(value = TableArgumentSpecification.class, name = "table"),
        @JsonSubTypes.Type(value = ScalarArgumentSpecification.class, name = "scalar")})
public abstract class ArgumentSpecification
{
    public static final String argumentType = "Abstract";
    private final String name;
    private final boolean required;

    // native representation
    private final Object defaultValue;

    ArgumentSpecification(String name, boolean required, @Nullable Object defaultValue)
    {
        this.name = checkNotNullOrEmpty(name, "name");
        checkArgument(!required || defaultValue == null, "non-null default value for a required argument");
        this.required = required;
        this.defaultValue = defaultValue;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public boolean isRequired()
    {
        return required;
    }

    @JsonProperty
    public Object getDefaultValue()
    {
        return defaultValue;
    }

    public String getArgumentType()
    {
        return argumentType;
    }
}
