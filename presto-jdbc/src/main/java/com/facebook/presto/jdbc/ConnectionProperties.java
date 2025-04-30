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

import com.facebook.presto.client.auth.external.ExternalRedirectStrategy;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import okhttp3.Protocol;

import java.io.File;
import java.security.KeyStore;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static com.facebook.presto.jdbc.AbstractConnectionProperty.ClassListConverter.CLASS_LIST_CONVERTER;
import static com.facebook.presto.jdbc.AbstractConnectionProperty.HttpProtocolConverter.HTTP_PROTOCOL_CONVERTER;
import static com.facebook.presto.jdbc.AbstractConnectionProperty.ListValidateConvertor.LIST_VALIDATE_CONVERTOR;
import static com.facebook.presto.jdbc.AbstractConnectionProperty.StringMapConverter.STRING_MAP_CONVERTER;
import static com.facebook.presto.jdbc.AbstractConnectionProperty.checkedPredicate;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

final class ConnectionProperties
{
    public static final ConnectionProperty<String> USER = new User();
    public static final ConnectionProperty<String> PASSWORD = new Password();
    public static final ConnectionProperty<HostAndPort> SOCKS_PROXY = new SocksProxy();
    public static final ConnectionProperty<HostAndPort> HTTP_PROXY = new HttpProxy();
    public static final ConnectionProperty<String> APPLICATION_NAME_PREFIX = new ApplicationNamePrefix();
    public static final ConnectionProperty<Boolean> DISABLE_COMPRESSION = new DisableCompression();
    public static final ConnectionProperty<Boolean> SSL = new Ssl();
    public static final ConnectionProperty<String> SSL_KEY_STORE_PATH = new SslKeyStorePath();

    public static final ConnectionProperty<String> SSL_KEY_STORE_PASSWORD = new SslKeyStorePassword();
    public static final ConnectionProperty<String> SSL_TRUST_STORE_PATH = new SslTrustStorePath();
    public static final ConnectionProperty<String> SSL_TRUST_STORE_PASSWORD = new SslTrustStorePassword();
    public static final ConnectionProperty<String> KERBEROS_REMOTE_SERVICE_NAME = new KerberosRemoteServiceName();
    public static final ConnectionProperty<Boolean> KERBEROS_USE_CANONICAL_HOSTNAME = new KerberosUseCanonicalHostname();
    public static final ConnectionProperty<String> KERBEROS_PRINCIPAL = new KerberosPrincipal();
    public static final ConnectionProperty<File> KERBEROS_CONFIG_PATH = new KerberosConfigPath();
    public static final ConnectionProperty<File> KERBEROS_KEYTAB_PATH = new KerberosKeytabPath();
    public static final ConnectionProperty<File> KERBEROS_CREDENTIAL_CACHE_PATH = new KerberosCredentialCachePath();
    public static final ConnectionProperty<String> ACCESS_TOKEN = new AccessToken();
    public static final ConnectionProperty<String> TIMEZONE_ID = new TimeZoneId();
    public static final ConnectionProperty<Map<String, String>> EXTRA_CREDENTIALS = new ExtraCredentials();
    public static final ConnectionProperty<Map<String, String>> CUSTOM_HEADERS = new CustomHeaders();
    public static final ConnectionProperty<String> CLIENT_TAGS = new ClientTags();
    public static final ConnectionProperty<Map<String, String>> SESSION_PROPERTIES = new SessionProperties();
    public static final ConnectionProperty<List<Protocol>> HTTP_PROTOCOLS = new HttpProtocols();
    public static final ConnectionProperty<List<QueryInterceptor>> QUERY_INTERCEPTORS = new QueryInterceptors();
    public static final ConnectionProperty<Boolean> VALIDATE_NEXTURI_SOURCE = new ValidateNextUriSource();
    public static final ConnectionProperty<Boolean> FOLLOW_REDIRECTS = new FollowRedirects();
    public static final ConnectionProperty<String> SSL_KEY_STORE_TYPE = new SSLKeyStoreType();
    public static final ConnectionProperty<String> SSL_TRUST_STORE_TYPE = new SSLTrustStoreType();
    public static final ConnectionProperty<Boolean> EXTERNAL_AUTHENTICATION = new ExternalAuthentication();
    public static final ConnectionProperty<Duration> EXTERNAL_AUTHENTICATION_TIMEOUT = new ExternalAuthenticationTimeout();
    public static final ConnectionProperty<KnownTokenCache> EXTERNAL_AUTHENTICATION_TOKEN_CACHE = new ExternalAuthenticationTokenCache();
    public static final ConnectionProperty<List<ExternalRedirectStrategy>> EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS = new ExternalAuthenticationRedirectHandlers();

    private static final Set<ConnectionProperty<?>> ALL_PROPERTIES = ImmutableSet.<ConnectionProperty<?>>builder()
            .add(USER)
            .add(PASSWORD)
            .add(SOCKS_PROXY)
            .add(HTTP_PROXY)
            .add(APPLICATION_NAME_PREFIX)
            .add(DISABLE_COMPRESSION)
            .add(SSL)
            .add(SSL_KEY_STORE_PATH)
            .add(SSL_KEY_STORE_PASSWORD)
            .add(SSL_KEY_STORE_TYPE)
            .add(SSL_TRUST_STORE_PATH)
            .add(SSL_TRUST_STORE_PASSWORD)
            .add(SSL_TRUST_STORE_TYPE)
            .add(KERBEROS_REMOTE_SERVICE_NAME)
            .add(KERBEROS_USE_CANONICAL_HOSTNAME)
            .add(KERBEROS_PRINCIPAL)
            .add(KERBEROS_CONFIG_PATH)
            .add(KERBEROS_KEYTAB_PATH)
            .add(KERBEROS_CREDENTIAL_CACHE_PATH)
            .add(ACCESS_TOKEN)
            .add(TIMEZONE_ID)
            .add(EXTRA_CREDENTIALS)
            .add(CUSTOM_HEADERS)
            .add(CLIENT_TAGS)
            .add(SESSION_PROPERTIES)
            .add(HTTP_PROTOCOLS)
            .add(QUERY_INTERCEPTORS)
            .add(VALIDATE_NEXTURI_SOURCE)
            .add(FOLLOW_REDIRECTS)
            .add(EXTERNAL_AUTHENTICATION)
            .add(EXTERNAL_AUTHENTICATION_TIMEOUT)
            .add(EXTERNAL_AUTHENTICATION_TOKEN_CACHE)
            .add(EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS)
            .build();

    private static final Map<String, ConnectionProperty<?>> KEY_LOOKUP = unmodifiableMap(ALL_PROPERTIES.stream()
            .collect(toMap(ConnectionProperty::getKey, identity())));

    private static final Map<String, String> DEFAULTS;

    static {
        ImmutableMap.Builder<String, String> defaults = ImmutableMap.builder();
        for (ConnectionProperty<?> property : ALL_PROPERTIES) {
            property.getDefault().ifPresent(value -> defaults.put(property.getKey(), value));
        }
        DEFAULTS = defaults.build();
    }

    private ConnectionProperties() {}

    public static ConnectionProperty<?> forKey(String propertiesKey)
    {
        return KEY_LOOKUP.get(propertiesKey);
    }

    public static Set<ConnectionProperty<?>> allProperties()
    {
        return ALL_PROPERTIES;
    }

    public static Map<String, String> getDefaults()
    {
        return DEFAULTS;
    }

    private static class User
            extends AbstractConnectionProperty<String>
    {
        public User()
        {
            super("user", REQUIRED, ALLOWED, NON_EMPTY_STRING_CONVERTER);
        }
    }

    private static class Password
            extends AbstractConnectionProperty<String>
    {
        public Password()
        {
            super("password", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class SocksProxy
            extends AbstractConnectionProperty<HostAndPort>
    {
        private static final Predicate<Properties> NO_HTTP_PROXY =
                checkedPredicate(properties -> !HTTP_PROXY.getValue(properties).isPresent());

        public SocksProxy()
        {
            super("socksProxy", NOT_REQUIRED, NO_HTTP_PROXY, HostAndPort::fromString);
        }
    }

    private static class HttpProxy
            extends AbstractConnectionProperty<HostAndPort>
    {
        private static final Predicate<Properties> NO_SOCKS_PROXY =
                checkedPredicate(properties -> !SOCKS_PROXY.getValue(properties).isPresent());

        public HttpProxy()
        {
            super("httpProxy", NOT_REQUIRED, NO_SOCKS_PROXY, HostAndPort::fromString);
        }
    }

    private static class ApplicationNamePrefix
            extends AbstractConnectionProperty<String>
    {
        public ApplicationNamePrefix()
        {
            super("applicationNamePrefix", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class ClientTags
            extends AbstractConnectionProperty<String>
    {
        public ClientTags()
        {
            super("clientTags", NOT_REQUIRED, ALLOWED, LIST_VALIDATE_CONVERTOR);
        }
    }

    private static class DisableCompression
            extends AbstractConnectionProperty<Boolean>
    {
        public DisableCompression()
        {
            super("disableCompression", NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class Ssl
            extends AbstractConnectionProperty<Boolean>
    {
        public Ssl()
        {
            super("SSL", NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class SslKeyStorePath
            extends AbstractConnectionProperty<String>
    {
        private static final Predicate<Properties> IF_SSL_ENABLED =
                checkedPredicate(properties -> SSL.getValue(properties).orElse(false));

        public SslKeyStorePath()
        {
            super("SSLKeyStorePath", NOT_REQUIRED, IF_SSL_ENABLED, STRING_CONVERTER);
        }
    }

    private static class SslKeyStorePassword
            extends AbstractConnectionProperty<String>
    {
        private static final Predicate<Properties> IF_KEY_STORE =
                checkedPredicate(properties -> SSL_KEY_STORE_PATH.getValue(properties).isPresent());

        public SslKeyStorePassword()
        {
            super("SSLKeyStorePassword", NOT_REQUIRED, IF_KEY_STORE, STRING_CONVERTER);
        }
    }

    private static class SslTrustStorePath
            extends AbstractConnectionProperty<String>
    {
        private static final Predicate<Properties> IF_SSL_ENABLED =
                checkedPredicate(properties -> SSL.getValue(properties).orElse(false));

        public SslTrustStorePath()
        {
            super("SSLTrustStorePath", NOT_REQUIRED, IF_SSL_ENABLED, STRING_CONVERTER);
        }
    }

    private static class SslTrustStorePassword
            extends AbstractConnectionProperty<String>
    {
        private static final Predicate<Properties> IF_TRUST_STORE =
                checkedPredicate(properties -> SSL_TRUST_STORE_PATH.getValue(properties).isPresent());

        public SslTrustStorePassword()
        {
            super("SSLTrustStorePassword", NOT_REQUIRED, IF_TRUST_STORE, STRING_CONVERTER);
        }
    }

    private static class KerberosRemoteServiceName
            extends AbstractConnectionProperty<String>
    {
        public KerberosRemoteServiceName()
        {
            super("KerberosRemoteServiceName", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static Predicate<Properties> isKerberosEnabled()
    {
        return checkedPredicate(properties -> KERBEROS_REMOTE_SERVICE_NAME.getValue(properties).isPresent());
    }

    private static class KerberosPrincipal
            extends AbstractConnectionProperty<String>
    {
        public KerberosPrincipal()
        {
            super("KerberosPrincipal", NOT_REQUIRED, isKerberosEnabled(), STRING_CONVERTER);
        }
    }

    private static class KerberosUseCanonicalHostname
            extends AbstractConnectionProperty<Boolean>
    {
        public KerberosUseCanonicalHostname()
        {
            super("KerberosUseCanonicalHostname", Optional.of("true"), isKerberosEnabled(), ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class KerberosConfigPath
            extends AbstractConnectionProperty<File>
    {
        public KerberosConfigPath()
        {
            super("KerberosConfigPath", NOT_REQUIRED, isKerberosEnabled(), FILE_CONVERTER);
        }
    }

    private static class KerberosKeytabPath
            extends AbstractConnectionProperty<File>
    {
        public KerberosKeytabPath()
        {
            super("KerberosKeytabPath", NOT_REQUIRED, isKerberosEnabled(), FILE_CONVERTER);
        }
    }

    private static class KerberosCredentialCachePath
            extends AbstractConnectionProperty<File>
    {
        public KerberosCredentialCachePath()
        {
            super("KerberosCredentialCachePath", NOT_REQUIRED, isKerberosEnabled(), FILE_CONVERTER);
        }
    }

    private static class AccessToken
            extends AbstractConnectionProperty<String>
    {
        public AccessToken()
        {
            super("accessToken", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class TimeZoneId
            extends AbstractConnectionProperty<String>
    {
        public TimeZoneId()
        {
            super("timeZoneId", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class ExtraCredentials
            extends AbstractConnectionProperty<Map<String, String>>
    {
        public ExtraCredentials()
        {
            super("extraCredentials", NOT_REQUIRED, ALLOWED, STRING_MAP_CONVERTER);
        }
    }

    private static class CustomHeaders
            extends AbstractConnectionProperty<Map<String, String>>
    {
        public CustomHeaders()
        {
            super("customHeaders", NOT_REQUIRED, ALLOWED, STRING_MAP_CONVERTER);
        }
    }
    private static class SessionProperties
            extends AbstractConnectionProperty<Map<String, String>>
    {
        public SessionProperties()
        {
            super("sessionProperties", NOT_REQUIRED, ALLOWED, STRING_MAP_CONVERTER);
        }
    }

    private static class HttpProtocols
            extends AbstractConnectionProperty<List<Protocol>>
    {
        public HttpProtocols()
        {
            super("protocols", NOT_REQUIRED, ALLOWED, HTTP_PROTOCOL_CONVERTER);
        }
    }

    private static class QueryInterceptors
            extends AbstractConnectionProperty<List<QueryInterceptor>>
    {
        public QueryInterceptors()
        {
            super("queryInterceptors", NOT_REQUIRED, ALLOWED, CLASS_LIST_CONVERTER);
        }
    }

    private static class ValidateNextUriSource
            extends AbstractConnectionProperty<Boolean>
    {
        public ValidateNextUriSource()
        {
            super("validateNextUriSource", Optional.of("false"), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class FollowRedirects
            extends AbstractConnectionProperty<Boolean>
    {
        public FollowRedirects()
        {
            super("followRedirects", Optional.of("true"), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }
    private static class SSLTrustStoreType
            extends AbstractConnectionProperty<String>
    {
        public SSLTrustStoreType()
        {
            super("SSLTrustStoreType", Optional.of(KeyStore.getDefaultType()), NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class SSLKeyStoreType
            extends AbstractConnectionProperty<String>
    {
        public SSLKeyStoreType()
        {
            super("SSLKeyStoreType", Optional.of(KeyStore.getDefaultType()), NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static Predicate<Properties> isExternalAuthEnabled()
    {
        return checkedPredicate(properties -> EXTERNAL_AUTHENTICATION.getValue(properties).isPresent());
    }

    private static class ExternalAuthentication
            extends AbstractConnectionProperty<Boolean>
    {
        public ExternalAuthentication()
        {
            super("externalAuthentication", Optional.of("false"), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class ExternalAuthenticationTimeout
            extends AbstractConnectionProperty<Duration>
    {
        public ExternalAuthenticationTimeout()
        {
            super("externalAuthenticationTimeout", NOT_REQUIRED, isExternalAuthEnabled(), Duration::valueOf);
        }
    }

    private static class ExternalAuthenticationTokenCache
            extends AbstractConnectionProperty<KnownTokenCache>
    {
        public ExternalAuthenticationTokenCache()
        {
            super("externalAuthenticationTokenCache", Optional.of(KnownTokenCache.NONE.name()), NOT_REQUIRED, ALLOWED, KnownTokenCache::valueOf);
        }
    }

    private static class ExternalAuthenticationRedirectHandlers
            extends AbstractConnectionProperty<List<ExternalRedirectStrategy>>
    {
        private static final Splitter ENUM_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

        public ExternalAuthenticationRedirectHandlers()
        {
            super("externalAuthenticationRedirectHandlers", Optional.of("OPEN"), NOT_REQUIRED, ALLOWED, ExternalAuthenticationRedirectHandlers::parse);
        }

        public static List<ExternalRedirectStrategy> parse(String value)
        {
            return StreamSupport.stream(ENUM_SPLITTER.split(value).spliterator(), false)
                    .map(ExternalRedirectStrategy::valueOf)
                    .collect(toImmutableList());
        }
    }
}
