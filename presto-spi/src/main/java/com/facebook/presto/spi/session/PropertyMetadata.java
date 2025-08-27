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

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class PropertyMetadata<T>
{
    private final SessionPropertyMetadata sessionPropertyMetadata;
    private final Class<T> javaType;
    private final Type sqlType;
    private final T defaultValue;
    private final Function<Object, T> decoder;
    private final Function<T, Object> encoder;
    private final List<AdditionalSqlTypeHandler> additionalSqlTypeHandlers = new ArrayList<>();

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
        this.sqlType = requireNonNull(sqlType, "sqlType is null");
        this.sessionPropertyMetadata = new SessionPropertyMetadata(
                name,
                description,
                sqlType.getTypeSignature(),
                defaultValue == null ? "" : defaultValue.toString(),
                hidden);
        this.javaType = requireNonNull(javaType, "javaType is null");
        this.defaultValue = defaultValue;
        this.decoder = requireNonNull(decoder, "decoder is null");
        this.encoder = requireNonNull(encoder, "encoder is null");
    }

    /**
     * Name of the property.  This must be a valid identifier.
     */
    public String getName()
    {
        return sessionPropertyMetadata.getName();
    }

    /**
     * Description for the end user.
     */
    public String getDescription()
    {
        return sessionPropertyMetadata.getDescription();
    }

    /**
     * SQL type of the property.
     */
    public Type getSqlType()
    {
        return sqlType;
    }

    public List<AdditionalSqlTypeHandler> getAdditionalSqlTypeHandlers()
    {
        return additionalSqlTypeHandlers;
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
        return sessionPropertyMetadata.isHidden();
    }

    /**
     * Decodes the SQL type object value to the Java type of the property.
     */
    public T decode(Object value)
    {
        return decoder.apply(value);
    }

    /**
     * Decodes the SQL type object value to the Java type of the property using a specified decoder.
     */
    public T decode(Object value, Function<Object, T> decoder)
    {
        return decoder.apply(value);
    }

    public Function<Object, T> getDecoder()
    {
        return decoder;
    }

    /**
     * Encodes the Java type value to SQL type object value
     */
    public Object encode(T value)
    {
        return encoder.apply(value);
    }

    public Function<T, Object> getEncoder()
    {
        return encoder;
    }

    public PropertyMetadata<?> withAdditionalTypeHandler(Type additionalSupportedType, Function<Object, T> decoder)
    {
        this.additionalSqlTypeHandlers.add(new AdditionalSqlTypeHandler(additionalSupportedType, decoder));
        return this;
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

    public static PropertyMetadata<Byte> tinyIntProperty(String name, String description, Byte defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                TINYINT,
                Byte.class,
                defaultValue,
                hidden,
                value -> ((Number) value).byteValue(),
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

    public static PropertyMetadata<Duration> durationProperty(String name, String description, Duration defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                Duration.class,
                defaultValue,
                hidden,
                value -> Duration.valueOf((String) value),
                Duration::toString);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PropertyMetadata)) {
            return false;
        }

        PropertyMetadata<?> that = (PropertyMetadata<?>) o;

        boolean isSessionPropertyMetadataEqual = this.sessionPropertyMetadata.equals(that.sessionPropertyMetadata);

        boolean isJavaTypeEqual = this.javaType.equals(that.javaType);
        boolean isDefaultValueEqual = (this.defaultValue == null && that.defaultValue == null)
                || (this.defaultValue != null && this.defaultValue.equals(that.defaultValue));

        return isSessionPropertyMetadataEqual && isJavaTypeEqual && isDefaultValueEqual;
    }

    @Override
    public int hashCode()
    {
        int result = sessionPropertyMetadata.hashCode();

        result = 31 * result + Objects.hashCode(javaType);
        result = 31 * result + Objects.hashCode(defaultValue);

        return result;
    }

    public class AdditionalSqlTypeHandler
    {
        Type sqlType;
        Function<Object, T> decoder;

        public AdditionalSqlTypeHandler(Type sqlType, Function<Object, T> decoder)
        {
            this.sqlType = sqlType;
            this.decoder = decoder;
        }

        public Function<Object, T> getDecoder()
        {
            return decoder;
        }

        public Type getSqlType()
        {
            return sqlType;
        }
    }
}
