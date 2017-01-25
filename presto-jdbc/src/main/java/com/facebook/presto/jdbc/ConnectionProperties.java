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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;

import static com.facebook.presto.jdbc.AbstractConnectionProperty.BOOLEAN_CONVERTER;
import static com.facebook.presto.jdbc.AbstractConnectionProperty.INTEGER_CONVERTER;
import static com.google.common.base.Joiner.on;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class ConnectionProperties
{
    public static final ConnectionProperty<String> USER = new User();
    public static final ConnectionProperty<String> PASSWORD = new Password();
    public static final ConnectionProperty<Boolean> SSL = new Ssl();
    public static final ConnectionProperty<String> SSL_TRUST_STORE_PATH = new SslTrustStorePath();
    public static final ConnectionProperty<String> SSL_TRUST_STORE_PASSWORD = new SslTrustStorePassword();
    public static final ConnectionProperty<String> SSL_TRUST_STORE_PWD = new SslTrustStorePwd();

    private static final Set<ConnectionProperty> ALL_OF = ImmutableSet.of(
            USER,
            PASSWORD,
            SSL,
            SSL_TRUST_STORE_PATH,
            SSL_TRUST_STORE_PASSWORD,
            SSL_TRUST_STORE_PWD);

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
            super("user", REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class Password
            extends AbstractConnectionProperty<String>
    {
        private Password()
        {
            super("password", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    public static final boolean SSL_DISABLED = false;
    public static final boolean SSL_ENABLED = true;

    private static class SslEnabledConverter
            implements AbstractConnectionProperty.Converter<Boolean>
    {
        private static final Set<String> ALLOWED_BOOLEANS = ImmutableSet.of(
                Boolean.toString(SSL_DISABLED),
                Boolean.toString(SSL_ENABLED));

        @Override
        public Boolean convert(String value)
                throws Exception
        {
            /*
             * Boolean.parseBoolean will return false for any string that isn't true, ignoring case.
             * This is not what we want; we want to convert ints to booleans too following the
             * usual convention that any non-zero value is true, and zero is false.
             */
            if (ALLOWED_BOOLEANS.contains(value.toLowerCase())) {
                return BOOLEAN_CONVERTER.convert(value);
            }

            return 0 != INTEGER_CONVERTER.convert(value);
        }
    }

    private static class Ssl
            extends AbstractConnectionProperty<Boolean>
    {
        private static Set<String> allowed = ImmutableSet.of(
                Boolean.toString(SSL_DISABLED),
                Boolean.toString(SSL_ENABLED),
                // For compatibility with the Teradata JDBC driver, limit allowed integer values to 0, 1
                Integer.toString(0),
                Integer.toString(1));

        private Ssl()
        {
            super("SSL", Optional.of(Boolean.toString(SSL_DISABLED)), NOT_REQUIRED, ALLOWED, new SslEnabledConverter());
        }

        @Override
        public void validate(Properties properties)
                throws SQLException
        {
            Optional<Boolean> flag = getValue(properties);
            if (!flag.isPresent()) {
                return;
            }

            if (!allowed.contains(properties.getProperty(getKey()))) {
                throw new SQLException(format("The value of %s must be one of %s", getKey(), on(", ").join(allowed)));
            }
        }
    }

    private static class IfSslEnabled
            implements Predicate<Properties>
    {
        @Override
        public boolean test(Properties properties)
        {
            try {
                Optional<Boolean> sslEnabled = SSL.getValue(properties);
                return sslEnabled.isPresent() && sslEnabled.get() == SSL_ENABLED;
            }
            catch (SQLException e) {
                return false;
            }
        }
    }

    public static class SslTrustStorePath
            extends AbstractConnectionProperty<String>
    {
        private SslTrustStorePath()
        {
            super("SSLTrustStorePath", new IfSslEnabled(), new IfSslEnabled(), STRING_CONVERTER);
        }
    }

    private static final String SSL_TRUST_STORE_PASSWORD_KEY = "SSLTrustStorePassword";
    private static final String SSL_TRUST_STORE_PWD_KEY = "SSLTrustStorePwd";

    public static class SslTrustStorePassword
            extends AbstractConnectionProperty<String>
    {
        private SslTrustStorePassword()
        {
            super(SSL_TRUST_STORE_PASSWORD_KEY,
                    new IfSslEnabled().and(noneOf(ImmutableSet.of(SSL_TRUST_STORE_PWD_KEY))),
                    new IfSslEnabled().and(noneOf(ImmutableSet.of(SSL_TRUST_STORE_PWD_KEY))),
                    STRING_CONVERTER);
        }
    }

    public static class SslTrustStorePwd
            extends AbstractConnectionProperty<String>
    {
        private SslTrustStorePwd()
        {
            super(
                    SSL_TRUST_STORE_PWD_KEY,
                    new IfSslEnabled().and(noneOf(ImmutableSet.of(SSL_TRUST_STORE_PASSWORD_KEY))),
                    new IfSslEnabled().and(noneOf(ImmutableSet.of(SSL_TRUST_STORE_PASSWORD_KEY))),
                    STRING_CONVERTER);
        }
    }
}
