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
package com.facebook.presto.hive.metastore.hms.http;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class HttpHiveMetastoreConfig
{
    private HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType httpHiveMetastoreClientAuthenticationType = HttpHiveMetastoreClientAuthenticationType.NONE;
    private Duration httpReadTimeout = new Duration(60, TimeUnit.SECONDS);
    private Map<String, String> httpAdditionalHeaders = ImmutableMap.of();
    private boolean httpMetastoreTlsEnabled;
    private File httpMetastoreTlsKeystorePath;
    private String httpMetastoreTlsKeystorePassword;
    private File httpMetastoreTlsTruststorePath;
    private String httpMetastoreTlsTruststorePassword;
    private Optional<String> httpMetastoreBasicUsername = Optional.empty();
    private Optional<String> httpMetastoreBasicPassword = Optional.empty();
    private Optional<String> httpBearerToken = Optional.empty();

    public enum HttpHiveMetastoreClientAuthenticationType
    {
        NONE,
        BASIC,
        BEARER
    }

    public Optional<String> getHttpBearerToken()
    {
        return httpBearerToken;
    }

    @Config("hive.metastore.http.client.bearer-token")
    @ConfigSecuritySensitive
    @ConfigDescription("Bearer token to authenticate with a HTTP transport based metastore service")
    public HttpHiveMetastoreConfig setHttpBearerToken(String httpBearerToken)
    {
        this.httpBearerToken = Optional.ofNullable(httpBearerToken);
        return this;
    }

    @NotNull
    public Duration getHttpReadTimeout()
    {
        return httpReadTimeout;
    }

    @Config("hive.metastore.http.client.read-timeout")
    @ConfigDescription("Socket read timeout for metastore client")
    public HttpHiveMetastoreConfig setHttpReadTimeout(Duration readTimeout)
    {
        this.httpReadTimeout = readTimeout;
        return this;
    }

    public Map<String, String> getHttpAdditionalHeaders()
    {
        return httpAdditionalHeaders;
    }

    @Config("hive.metastore.http.client.additional-headers")
    @ConfigDescription("Comma separated key:value pairs to be send to metastore as additional headers")
    public HttpHiveMetastoreConfig setHttpAdditionalHeaders(String httpHeaders)
    {
        try {
            // we allow escaping the delimiters like , and : using back-slash.
            // To support that we create a negative lookbehind of , and : which
            // are not preceded by a back-slash.
            String headersDelim = "(?<!\\\\),";
            String kvDelim = "(?<!\\\\):";
            Map<String, String> temp = new HashMap<>();
            if (httpHeaders != null) {
                for (String kv : httpHeaders.split(headersDelim)) {
                    String key = kv.split(kvDelim, 2)[0].trim();
                    String val = kv.split(kvDelim, 2)[1].trim();
                    temp.put(key, val);
                }
                this.httpAdditionalHeaders = ImmutableMap.copyOf(temp);
            }
        }
        catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(format("Invalid format for 'hive.metastore.http.client.additional-headers'. " +
                    "Value provided is %s", httpHeaders), e);
        }
        return this;
    }

    @Config("hive.metastore.http.client.tls.enabled")
    @ConfigDescription("Enable TLS security for HMS")
    public HttpHiveMetastoreConfig setHttpMetastoreTlsEnabled(boolean enabled)
    {
        this.httpMetastoreTlsEnabled = enabled;
        return this;
    }

    public boolean getHttpMetastoreTlsEnabled()
    {
        return httpMetastoreTlsEnabled;
    }

    @Config("hive.metastore.http.client.tls.keystore-path")
    @ConfigDescription("Path to JKS or PEM key store")
    public HttpHiveMetastoreConfig setHttpMetastoreTlsKeystorePath(File keystorePath)
    {
        this.httpMetastoreTlsKeystorePath = keystorePath;
        return this;
    }

    public File getHttpMetastoreTlsKeystorePath()
    {
        return httpMetastoreTlsKeystorePath;
    }

    @Config("hive.metastore.http.client.tls.keystore-password")
    @ConfigDescription("Password to key store")
    @ConfigSecuritySensitive
    public HttpHiveMetastoreConfig setHttpMetastoreTlsKeystorePassword(String keystorePassword)
    {
        this.httpMetastoreTlsKeystorePassword = keystorePassword;
        return this;
    }

    public String getHttpMetastoreTlsKeystorePassword()
    {
        return httpMetastoreTlsKeystorePassword;
    }

    @Config("hive.metastore.http.client.tls.truststore-path")
    @ConfigDescription("Path to JKS or PEM trust store")
    public HttpHiveMetastoreConfig setHttpMetastoreTlsTruststorePath(File truststorePath)
    {
        this.httpMetastoreTlsTruststorePath = truststorePath;
        return this;
    }

    public File getHttpMetastoreTlsTruststorePath()
    {
        return httpMetastoreTlsTruststorePath;
    }

    @Config("hive.metastore.http.client.tls.truststore-password")
    @ConfigDescription("Path to trust store")
    public HttpHiveMetastoreConfig setHttpMetastoreTlsTruststorePassword(String truststorePassword)
    {
        this.httpMetastoreTlsTruststorePassword = truststorePassword;
        return this;
    }

    public String getHttpMetastoreTlsTruststorePassword()
    {
        return httpMetastoreTlsTruststorePassword;
    }

    @Config("hive.metastore.http.client.auth.basic.password")
    @ConfigDescription("Hive metastore http client authentication basic password")
    public HttpHiveMetastoreConfig setHttpMetastoreBasicPassword(String httpMetastoreBasicPassword)
    {
        this.httpMetastoreBasicPassword = Optional.ofNullable(httpMetastoreBasicPassword);
        return this;
    }

    public Optional<String> getHttpMetastoreBasicPassword()
    {
        return this.httpMetastoreBasicPassword;
    }

    @Config("hive.metastore.http.client.auth.basic.username")
    @ConfigDescription("Hive metastore http client authentication basic username")
    public HttpHiveMetastoreConfig setHttpMetastoreBasicUsername(String httpMetastoreBasicUsername)
    {
        this.httpMetastoreBasicUsername = Optional.ofNullable(httpMetastoreBasicUsername);
        return this;
    }

    public Optional<String> getHttpMetastoreBasicUsername()
    {
        return this.httpMetastoreBasicUsername;
    }

    @Config("hive.metastore.http.client.authentication.type")
    @ConfigDescription("Hive metastore http client authentication type")
    public HttpHiveMetastoreConfig setHttpHiveMetastoreClientAuthenticationType(HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType httpHiveMetastoreClientAuthenticationType)
    {
        this.httpHiveMetastoreClientAuthenticationType = httpHiveMetastoreClientAuthenticationType;
        return this;
    }

    @NotNull
    public HttpHiveMetastoreConfig.HttpHiveMetastoreClientAuthenticationType getHttpHiveMetastoreClientAuthenticationType()
    {
        return httpHiveMetastoreClientAuthenticationType;
    }
}
