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
package com.facebook.presto.iceberg.nessie;

import com.facebook.presto.hive.azure.AzureConfigurationInitializer;
import com.facebook.presto.hive.gcs.GcsConfigurationInitializer;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.iceberg.IcebergSessionProperties.getNessieReferenceHash;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getNessieReferenceName;
import static com.facebook.presto.iceberg.nessie.AuthenticationType.BASIC;
import static com.facebook.presto.iceberg.nessie.AuthenticationType.BEARER;
import static java.util.Objects.requireNonNull;

public class IcebergNessieCatalogFactory
        extends IcebergNativeCatalogFactory
{
    private final IcebergNessieConfig catalogConfig;

    @Inject
    public IcebergNessieCatalogFactory(
            IcebergConfig config,
            IcebergNessieConfig catalogConfig,
            IcebergCatalogName catalogName,
            S3ConfigurationUpdater s3ConfigurationUpdater,
            GcsConfigurationInitializer gcsConfigurationInitialize,
            AzureConfigurationInitializer azureConfigurationInitialize)
    {
        super(config, catalogName, s3ConfigurationUpdater, gcsConfigurationInitialize, azureConfigurationInitialize);
        this.catalogConfig = requireNonNull(catalogConfig, "catalogConfig is null");
    }

    @Override
    protected Map<String, String> getCatalogProperties(ConnectorSession session)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        properties.put("ref", getNessieReferenceName(session));
        properties.put("uri", catalogConfig.getServerUri().orElseThrow(() -> new IllegalStateException("iceberg.nessie.uri must be set for Nessie")));
        String hash = getNessieReferenceHash(session);
        if (hash != null) {
            properties.put("ref.hash", hash);
        }
        catalogConfig.getReadTimeoutMillis().ifPresent(val -> properties.put("transport.read-timeout", val.toString()));
        catalogConfig.getConnectTimeoutMillis().ifPresent(val -> properties.put("transport.connect-timeout", val.toString()));
        catalogConfig.getClientBuilderImpl().ifPresent(val -> properties.put("client-builder-impl", val));
        catalogConfig.getAuthenticationType().ifPresent(type -> {
            if (type == BASIC) {
                properties.put("authentication.username", catalogConfig.getUsername()
                        .orElseThrow(() -> new IllegalStateException("iceberg.nessie.auth.basic.username must be set with BASIC authentication")));
                properties.put("authentication.password", catalogConfig.getPassword()
                        .orElseThrow(() -> new IllegalStateException("iceberg.nessie.auth.basic.password must be set with BASIC authentication")));
            }
            else if (type == BEARER) {
                properties.put("authentication.token", catalogConfig.getBearerToken()
                        .orElseThrow(() -> new IllegalStateException("iceberg.nessie.auth.bearer.token must be set with BEARER authentication")));
            }
        });
        if (!catalogConfig.isCompressionEnabled()) {
            properties.put("transport.disable-compression", "true");
        }
        return properties.build();
    }

    @Override
    protected Optional<String> getCatalogCacheKey(ConnectorSession session)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(getNessieReferenceName(session));
        sb.append("@");
        sb.append(getNessieReferenceHash(session));
        return Optional.of(sb.toString());
    }
}
