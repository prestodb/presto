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
package com.facebook.presto.plugin.jdbc;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.ConfigurationException;
import com.google.inject.spi.Message;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.NotNull;

import java.util.Set;

import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Base configuration class for JDBC connectors.
 *
 * This class is provided for convenience and contains common configuration properties
 * that many JDBC connectors may need. However, core JDBC functionality should not
 * depend on this class, as JDBC connectors may choose to use their own mechanisms
 * for connection management, authentication, and other configuration needs.
 *
 * Connectors are free to implement their own configuration classes and connection
 * strategies without extending or using this base configuration.
 */
public class BaseJdbcConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String userCredentialName;
    private String passwordCredentialName;
    private boolean caseInsensitiveNameMatching;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);
    private Set<String> listSchemasIgnoredSchemas = ImmutableSet.of("information_schema");
    private boolean caseSensitiveNameMatchingEnabled;

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public BaseJdbcConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @Nullable
    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("connection-user")
    public BaseJdbcConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @Nullable
    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("connection-password")
    @ConfigSecuritySensitive
    public BaseJdbcConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    @Nullable
    public String getUserCredentialName()
    {
        return userCredentialName;
    }

    @Config("user-credential-name")
    public BaseJdbcConfig setUserCredentialName(String userCredentialName)
    {
        this.userCredentialName = userCredentialName;
        return this;
    }

    @Nullable
    public String getPasswordCredentialName()
    {
        return passwordCredentialName;
    }

    @Config("password-credential-name")
    public BaseJdbcConfig setPasswordCredentialName(String passwordCredentialName)
    {
        this.passwordCredentialName = passwordCredentialName;
        return this;
    }

    @Deprecated
    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Deprecated
    @Config("case-insensitive-name-matching")
    @ConfigDescription("Deprecated: This will be removed in future releases. Use 'case-sensitive-name-matching=true' instead for mysql. " +
            "This configuration setting converts all schema/table names to lowercase. " +
            "If your source database contains names differing only by case (e.g., 'Testdb' and 'testdb'), " +
            "this setting can lead to conflicts and query failures.")
    public BaseJdbcConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl;
    }

    @Config("case-insensitive-name-matching.cache-ttl")
    public BaseJdbcConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }

    public Set<String> getlistSchemasIgnoredSchemas()
    {
        return listSchemasIgnoredSchemas;
    }

    @Config("list-schemas-ignored-schemas")
    public BaseJdbcConfig setlistSchemasIgnoredSchemas(String listSchemasIgnoredSchemas)
    {
        this.listSchemasIgnoredSchemas = ImmutableSet.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(listSchemasIgnoredSchemas.toLowerCase(ENGLISH)));
        return this;
    }

    public boolean isCaseSensitiveNameMatching()
    {
        return caseSensitiveNameMatchingEnabled;
    }

    @Config("case-sensitive-name-matching")
    @ConfigDescription("Enable case-sensitive matching of schema, table names across the connector. " +
            "When disabled, names are matched case-insensitively using lowercase normalization.")
    public BaseJdbcConfig setCaseSensitiveNameMatching(boolean caseSensitiveNameMatchingEnabled)
    {
        this.caseSensitiveNameMatchingEnabled = caseSensitiveNameMatchingEnabled;
        return this;
    }

    @PostConstruct
    public void validateConfig()
    {
        if (isCaseInsensitiveNameMatching() && isCaseSensitiveNameMatching()) {
            throw new ConfigurationException(ImmutableList.of(new Message("Only one of 'case-insensitive-name-matching=true' or 'case-sensitive-name-matching=true' can be set. " +
                    "These options are mutually exclusive.")));
        }

        if (connectionUrl == null) {
            throw new ConfigurationException(ImmutableList.of(new Message("connection-url is required but was not provided")));
        }
    }
}
