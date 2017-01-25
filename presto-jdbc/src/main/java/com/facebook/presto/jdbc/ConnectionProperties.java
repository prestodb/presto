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
package com.facebook.presto.jdbc;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class ConnectionProperties
{
    public static final ConnectionProperty<String> USER = new User();
    public static final ConnectionProperty<String> PASSWORD = new Password();
    public static final ConnectionProperty<Boolean> SECURE = new Secure();

    private static final Set<ConnectionProperty> ALL_OF = ImmutableSet.of(
            USER,
            PASSWORD,
            SECURE);

    private static final Map<String, ConnectionProperty> KEY_LOOKUP = unmodifiableMap(allOf().stream()
            .collect(toMap(ConnectionProperty::getKey, identity())));

    private static final Map<String, String> DEFAULTS;

    static {
        ImmutableMap.Builder<String, String> defaults = ImmutableMap.builder();
        for (ConnectionProperty property : ALL_OF) {
            Optional<String> value = property.getDefault();

            if (value.isPresent()) {
                defaults.put(property.getKey(), value.get());
            }
        }
        DEFAULTS = defaults.build();
    }

    private ConnectionProperties() {}

    public static ConnectionProperty forKey(String propertiesKey)
    {
        return KEY_LOOKUP.get(propertiesKey);
    }

    public static Set<ConnectionProperty> allOf()
    {
        return ALL_OF;
    }

    public static Map<String, String> getDefaults()
    {
        return DEFAULTS;
    }

    private static class User
            extends AbstractConnectionProperty<String>
    {
        private User()
        {
            super("user", REQUIRED, STRING_CONVERTER);
        }
    }

    private static class Password
            extends AbstractConnectionProperty<String>
    {
        private Password()
        {
            super("password", NOT_REQUIRED, STRING_CONVERTER);
        }
    }

    public static final boolean SSL_DISABLED = false;
    public static final boolean SSL_ENABLED = true;

    private static class Secure
            extends AbstractConnectionProperty<Boolean>
    {
        private static final Set<Boolean> allowed = ImmutableSet.of(SSL_DISABLED, SSL_ENABLED);

        private Secure()
        {
            super("secure", Optional.of(Boolean.toString(SSL_DISABLED)), NOT_REQUIRED, BOOLEAN_CONVERTER);
        }

        @Override
        public void validate(Properties properties)
                throws SQLException
        {
            Optional<Boolean> flag = getValue(properties);
            if (!flag.isPresent()) {
                return;
            }

            if (!allowed.contains(flag.get())) {
                throw new SQLException(format("The value of %s must be one of %s", getKey(), Joiner.on(", ").join(allowed)));
            }
        }
    }
}
