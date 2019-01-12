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
package io.prestosql.jdbc;

import java.io.File;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

abstract class AbstractConnectionProperty<T>
        implements ConnectionProperty<T>
{
    private final String key;
    private final Optional<String> defaultValue;
    private final Predicate<Properties> isRequired;
    private final Predicate<Properties> isAllowed;
    private final Converter<T> converter;

    protected AbstractConnectionProperty(
            String key,
            Optional<String> defaultValue,
            Predicate<Properties> isRequired,
            Predicate<Properties> isAllowed,
            Converter<T> converter)
    {
        this.key = requireNonNull(key, "key is null");
        this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
        this.isRequired = requireNonNull(isRequired, "isRequired is null");
        this.isAllowed = requireNonNull(isAllowed, "isAllowed is null");
        this.converter = requireNonNull(converter, "converter is null");
    }

    protected AbstractConnectionProperty(
            String key,
            Predicate<Properties> required,
            Predicate<Properties> allowed,
            Converter<T> converter)
    {
        this(key, Optional.empty(), required, allowed, converter);
    }

    @Override
    public String getKey()
    {
        return key;
    }

    @Override
    public Optional<String> getDefault()
    {
        return defaultValue;
    }

    @Override
    public DriverPropertyInfo getDriverPropertyInfo(Properties mergedProperties)
    {
        String currentValue = mergedProperties.getProperty(key);
        DriverPropertyInfo result = new DriverPropertyInfo(key, currentValue);
        result.required = isRequired.test(mergedProperties);
        return result;
    }

    @Override
    public boolean isRequired(Properties properties)
    {
        return isRequired.test(properties);
    }

    @Override
    public boolean isAllowed(Properties properties)
    {
        return !properties.containsKey(key) || isAllowed.test(properties);
    }

    @Override
    public Optional<T> getValue(Properties properties)
            throws SQLException
    {
        String value = properties.getProperty(key);
        if (value == null) {
            if (isRequired(properties)) {
                throw new SQLException(format("Connection property '%s' is required", key));
            }
            return Optional.empty();
        }

        try {
            return Optional.of(converter.convert(value));
        }
        catch (RuntimeException e) {
            if (value.isEmpty()) {
                throw new SQLException(format("Connection property '%s' value is empty", key), e);
            }
            throw new SQLException(format("Connection property '%s' value is invalid: %s", key, value), e);
        }
    }

    @Override
    public void validate(Properties properties)
            throws SQLException
    {
        if (!isAllowed(properties)) {
            throw new SQLException(format("Connection property '%s' is not allowed", key));
        }

        getValue(properties);
    }

    protected static final Predicate<Properties> REQUIRED = properties -> true;
    protected static final Predicate<Properties> NOT_REQUIRED = properties -> false;

    protected static final Predicate<Properties> ALLOWED = properties -> true;

    interface Converter<T>
    {
        T convert(String value);
    }

    protected static final Converter<String> STRING_CONVERTER = value -> value;

    protected static final Converter<String> NON_EMPTY_STRING_CONVERTER = value -> {
        checkArgument(!value.isEmpty(), "value is empty");
        return value;
    };

    protected static final Converter<File> FILE_CONVERTER = File::new;

    protected static final Converter<Boolean> BOOLEAN_CONVERTER = value -> {
        switch (value.toLowerCase(ENGLISH)) {
            case "true":
                return true;
            case "false":
                return false;
        }
        throw new IllegalArgumentException("value must be 'true' or 'false'");
    };

    protected interface CheckedPredicate<T>
    {
        boolean test(T t)
                throws SQLException;
    }

    protected static <T> Predicate<T> checkedPredicate(CheckedPredicate<T> predicate)
    {
        return t -> {
            try {
                return predicate.test(t);
            }
            catch (SQLException e) {
                return false;
            }
        };
    }
}
