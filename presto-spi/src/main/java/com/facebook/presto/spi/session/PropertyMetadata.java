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
import io.airlift.units.DataSize;

import java.util.function.Function;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class PropertyMetadata<T>
{
    private final String name;
    private final String description;
    private final Type sqlType;
    private final Class<T> javaType;
    private final T defaultValue;
    private final boolean hidden;
    private final Function<Object, T> decoder;
    private final Function<T, Object> encoder;

    public PropertyMetadata(
            String name,
            String description,
            Type sqlType,
            Class<T> javaType,
            T defaultValue,
            boolean hidden,
            Function<Object, T> decoder,
            Function<T, Object> encoder)
    {
        requireNonNull(name, "name is null");
        requireNonNull(description, "description is null");
        requireNonNull(sqlType, "type is null");
        requireNonNull(javaType, "javaType is null");
        requireNonNull(decoder, "decoder is null");
        requireNonNull(encoder, "encoder is null");

        if (name.isEmpty() || !name.trim().toLowerCase(ENGLISH).equals(name)) {
            throw new IllegalArgumentException(format("Invalid property name '%s'", name));
        }
        if (description.isEmpty() || !description.trim().equals(description)) {
            throw new IllegalArgumentException(format("Invalid property description '%s'", description));
        }

        this.name = name;
        this.description = description;
        this.javaType = javaType;
        this.sqlType = sqlType;
        this.defaultValue = defaultValue;
        this.hidden = hidden;
        this.decoder = decoder;
        this.encoder = encoder;
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
     * Java type of this property.
     */
    public Class<T> getJavaType()
    {
        return javaType;
    }

    /**
     * Gets the default value for this property.
     */
    public T getDefaultValue()
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

    /**
     * Decodes the SQL type object value to the Java type of the property.
     */
    public T decode(Object value)
    {
        return decoder.apply(value);
    }

    /**
     * Encodes the Java type value to SQL type object value
     */
    public Object encode(T value)
    {
        return encoder.apply(value);
    }

    public static PropertyMetadata<Boolean> booleanProperty(String name, String description, Boolean defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                BOOLEAN,
                Boolean.class,
                defaultValue,
                hidden,
                Boolean.class::cast,
                object -> object);
    }

    public static PropertyMetadata<Integer> integerProperty(String name, String description, Integer defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                INTEGER,
                Integer.class,
                defaultValue,
                hidden,
                value -> ((Number) value).intValue(),
                object -> object);
    }

    public static PropertyMetadata<Long> longProperty(String name, String description, Long defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                BIGINT,
                Long.class,
                defaultValue,
                hidden,
                value -> ((Number) value).longValue(),
                object -> object);
    }

    public static PropertyMetadata<Double> doubleProperty(String name, String description, Double defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                DOUBLE,
                Double.class,
                defaultValue,
                hidden,
                value -> ((Number) value).doubleValue(),
                object -> object);
    }

    public static PropertyMetadata<String> stringProperty(String name, String description, String defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                String.class,
                defaultValue,
                hidden,
                String.class::cast,
                object -> object);
    }

    public static PropertyMetadata<DataSize> dataSizeProperty(String name, String description, DataSize defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                DataSize.class,
                defaultValue,
                hidden,
                value -> DataSize.valueOf((String) value),
                DataSize::toString);
    }
}
