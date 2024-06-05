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
import com.facebook.presto.hive.gcs.GcsConfigurationInitializer;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableMap;
import io.jsonwebtoken.Jwts;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import javax.inject.Inject;

import java.util.Date;
import java.util.Map;

import static com.facebook.presto.iceberg.rest.AuthenticationType.OAUTH2;
import static com.facebook.presto.iceberg.rest.SessionType.USER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.rest.auth.OAuth2Properties.CREDENTIAL;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;

public class IcebergRestCatalogFactory
        extends IcebergNativeCatalogFactory
{
    private final IcebergRestConfig catalogConfig;
    private final NodeVersion nodeVersion;

    @Inject
    public IcebergRestCatalogFactory(
            IcebergConfig config,
            IcebergRestConfig catalogConfig,
            IcebergCatalogName catalogName,
            S3ConfigurationUpdater s3ConfigurationUpdater,
            GcsConfigurationInitializer gcsConfigurationInitialize,
            NodeVersion nodeVersion)
    {
        super(config, catalogName, s3ConfigurationUpdater, gcsConfigurationInitialize);
        this.catalogConfig = requireNonNull(catalogConfig, "catalogConfig is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
    }

    @Override
    protected Map<String, String> getCatalogProperties(ConnectorSession session)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        properties.put(URI, catalogConfig.getServerUri().orElseThrow(
                () -> new IllegalStateException("iceberg.rest.uri must be set for REST catalog")));

        catalogConfig.getAuthenticationType().ifPresent(type -> {
            if (type == OAUTH2) {
                if (!catalogConfig.credentialOrTokenExists()) {
                    throw new IllegalStateException("iceberg.rest.auth.oauth2 requires either a credential or a token");
                }
                catalogConfig.getCredential().ifPresent(credential -> properties.put(CREDENTIAL, credential));
                catalogConfig.getToken().ifPresent(token -> properties.put(TOKEN, token));
            }
        });

        catalogConfig.getSessionType().ifPresent(type -> {
            if (type == USER) {
                properties.putAll(session.getIdentity().getExtraCredentials());

                String sessionId = format("%s-%s", session.getUser(), session.getSource().orElse("default"));
                String jwt = Jwts.builder()
                        .setId(sessionId)
                        .setSubject(session.getUser())
                        .setIssuedAt(new Date())
                        .setIssuer(nodeVersion.toString())
                        .claim("user", session.getUser())
                        .claim("source", session.getSource().orElse(""))
                        .compact();

                properties.put(OAuth2Properties.JWT_TOKEN_TYPE, jwt);
            }
        });

        return properties.build();
    }
}
