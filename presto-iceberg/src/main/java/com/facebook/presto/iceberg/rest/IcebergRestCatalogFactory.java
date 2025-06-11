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
package com.facebook.presto.iceberg.rest;

import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.azure.AzureConfigurationInitializer;
import com.facebook.presto.hive.gcs.GcsConfigurationInitializer;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.jsonwebtoken.Jwts;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;

import javax.inject.Inject;

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.iceberg.rest.AuthenticationType.OAUTH2;
import static com.facebook.presto.iceberg.rest.SessionType.USER;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogUtil.configureHadoopConf;
import static org.apache.iceberg.rest.auth.OAuth2Properties.CREDENTIAL;
import static org.apache.iceberg.rest.auth.OAuth2Properties.JWT_TOKEN_TYPE;
import static org.apache.iceberg.rest.auth.OAuth2Properties.OAUTH2_SERVER_URI;
import static org.apache.iceberg.rest.auth.OAuth2Properties.SCOPE;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;

public class IcebergRestCatalogFactory
        extends IcebergNativeCatalogFactory
{
    private final IcebergRestConfig catalogConfig;
    private final NodeVersion nodeVersion;
    private final String catalogName;
    private final boolean nestedNamespaceEnabled;

    @Inject
    public IcebergRestCatalogFactory(
            IcebergConfig config,
            IcebergRestConfig catalogConfig,
            IcebergCatalogName catalogName,
            S3ConfigurationUpdater s3ConfigurationUpdater,
            GcsConfigurationInitializer gcsConfigurationInitialize,
            AzureConfigurationInitializer azureConfigurationInitialize,
            NodeVersion nodeVersion)
    {
        super(config, catalogName, s3ConfigurationUpdater, gcsConfigurationInitialize, azureConfigurationInitialize);
        this.catalogConfig = requireNonNull(catalogConfig, "catalogConfig is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null").getCatalogName();
        this.nestedNamespaceEnabled = catalogConfig.isNestedNamespaceEnabled();
    }

    @Override
    public Catalog getCatalog(ConnectorSession session)
    {
        try {
            return catalogCache.get(getCacheKey(session), () -> {
                RESTCatalog catalog = new RESTCatalog(
                        convertSession(session),
                        config -> HTTPClient.builder(config).uri(config.get(URI)).build());

                configureHadoopConf(catalog, getHadoopConfiguration());
                catalog.initialize(catalogName, getProperties(session));
                return catalog;
            });
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throwIfUnchecked(e);
            throw new UncheckedExecutionException(e);
        }
    }

    @Override
    protected Optional<String> getCatalogCacheKey(ConnectorSession session)
    {
        StringBuilder sb = new StringBuilder();
        catalogConfig.getSessionType().filter(type -> type.equals(USER))
                .ifPresent(type -> sb.append(session.getUser()));
        return Optional.of(sb.toString());
    }

    @Override
    protected Map<String, String> getCatalogProperties(ConnectorSession session)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        properties.put(URI, catalogConfig.getServerUri().orElseThrow(
                () -> new IllegalStateException("iceberg.rest.uri must be set for REST catalog")));

        catalogConfig.getAuthenticationType().ifPresent(type -> {
            if (type == OAUTH2) {
                // The oauth2/tokens endpoint of the REST catalog spec has been deprecated and will
                // be removed in Iceberg 2.0 (https://github.com/apache/iceberg/pull/10603)
                // TODO auth server URI will eventually need to be made a required property
                catalogConfig.getAuthenticationServerUri().ifPresent(authServerUri -> properties.put(OAUTH2_SERVER_URI, authServerUri));

                if (!catalogConfig.credentialOrTokenExists()) {
                    throw new IllegalStateException("iceberg.rest.auth.oauth2 requires either a credential or a token");
                }
                catalogConfig.getCredential().ifPresent(credential -> properties.put(CREDENTIAL, credential));
                catalogConfig.getToken().ifPresent(token -> properties.put(TOKEN, token));
                catalogConfig.getScope().ifPresent(scope -> properties.put(SCOPE, scope));
            }
        });

        catalogConfig.getSessionType().filter(type -> type.equals(USER))
                .ifPresent(type -> properties.put(CatalogProperties.USER, session.getUser()));

        return properties.build();
    }

    @Override
    public boolean isNestedNamespaceEnabled()
    {
        return this.nestedNamespaceEnabled;
    }

    protected SessionContext convertSession(ConnectorSession session)
    {
        RestSessionBuilder sessionContextBuilder = catalogConfig.getSessionType()
                .filter(type -> type.equals(USER))
                .map(type -> {
                    String sessionId = format("%s-%s", session.getUser(), session.getSource().orElse("default"));
                    Map<String, String> properties = ImmutableMap.of(
                            "user", session.getUser(),
                            "source", session.getSource().orElse(""),
                            "version", nodeVersion.toString());

                    String jwt = Jwts.builder()
                            .setSubject(session.getUser())
                            .setIssuer(nodeVersion.toString())
                            .setIssuedAt(new Date())
                            .claim("user", session.getUser())
                            .claim("source", session.getSource().orElse(""))
                            .compact();

                    ImmutableMap.Builder<String, String> credentials = ImmutableMap.builder();
                    credentials.put(JWT_TOKEN_TYPE, jwt).putAll(session.getIdentity().getExtraCredentials());

                    return builder(session).setSessionId(sessionId)
                            .setIdentity(session.getUser())
                            .setCredentials(credentials.build())
                            .setProperties(properties);
                }).orElseGet(() -> builder(session).setSessionId(randomUUID().toString()));
        return sessionContextBuilder.build();
    }

    protected static class RestSessionBuilder
    {
        private String sessionId;
        private String identity;
        private Map<String, String> properties;
        private Map<String, String> credentials;
        private final ConnectorIdentity wrappedIdentity;

        private RestSessionBuilder(ConnectorSession session)
        {
            sessionId = null;
            identity = null;
            credentials = null;
            properties = ImmutableMap.of();
            wrappedIdentity = session.getIdentity();
        }

        protected RestSessionBuilder setSessionId(String sessionId)
        {
            this.sessionId = sessionId;
            return this;
        }

        protected RestSessionBuilder setIdentity(String identity)
        {
            this.identity = identity;
            return this;
        }

        protected RestSessionBuilder setCredentials(Map<String, String> credentials)
        {
            this.credentials = credentials;
            return this;
        }

        protected RestSessionBuilder setProperties(Map<String, String> properties)
        {
            this.properties = properties;
            return this;
        }

        protected SessionContext build()
        {
            return new SessionContext(
                    sessionId,
                    identity,
                    credentials,
                    properties,
                    wrappedIdentity);
        }
    }

    protected static RestSessionBuilder builder(ConnectorSession session)
    {
        return new RestSessionBuilder(session);
    }
}
