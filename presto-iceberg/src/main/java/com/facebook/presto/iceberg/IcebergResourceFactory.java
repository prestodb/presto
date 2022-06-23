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
package com.facebook.presto.iceberg;

import com.facebook.presto.iceberg.nessie.NessieConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.iceberg.CatalogType.NESSIE;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getNessieReferenceHash;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getNessieReferenceName;
import static com.facebook.presto.iceberg.nessie.AuthenticationType.BASIC;
import static com.facebook.presto.iceberg.nessie.AuthenticationType.BEARER;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;

/**
 * Factory for loading Iceberg resources such as Catalog.
 */
public class IcebergResourceFactory
{
    private final Cache<String, Catalog> catalogCache;

    private final String catalogName;
    private final CatalogType catalogType;
    private final String catalogWarehouse;
    private final List<String> hadoopConfigResources;
    private final NessieConfig nessieConfig;

    @Inject
    public IcebergResourceFactory(IcebergConfig config, IcebergCatalogName catalogName, NessieConfig nessieConfig)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null").getCatalogName();
        requireNonNull(config, "config is null");
        this.catalogType = config.getCatalogType();
        this.catalogWarehouse = config.getCatalogWarehouse();
        this.hadoopConfigResources = config.getHadoopConfigResources();
        this.nessieConfig = requireNonNull(nessieConfig, "nessieConfig is null");
        catalogCache = CacheBuilder.newBuilder()
                .maximumSize(config.getCatalogCacheSize())
                .build();
    }

    public Catalog getCatalog(ConnectorSession session)
    {
        try {
            return catalogCache.get(getCatalogCacheKey(session), () -> CatalogUtil.loadCatalog(
                    catalogType.getCatalogImpl(), catalogName, getCatalogProperties(session), getHadoopConfiguration(session)));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throwIfUnchecked(e);
            throw new UncheckedExecutionException(e);
        }
    }

    public SupportsNamespaces getNamespaces(ConnectorSession session)
    {
        Catalog catalog = getCatalog(session);
        if (catalog instanceof SupportsNamespaces) {
            return (SupportsNamespaces) catalog;
        }
        throw new PrestoException(NOT_SUPPORTED, "Iceberg catalog of type " + catalogType + " does not support namespace operations");
    }

    private String getCatalogCacheKey(ConnectorSession session)
    {
        StringBuilder sb = new StringBuilder();
        ConnectorIdentity identity = session.getIdentity();
        sb.append("User:");
        sb.append(identity.getUser());
        if (identity.getPrincipal().isPresent()) {
            sb.append(",Principle:");
            sb.append(identity.getPrincipal().toString());
        }
        if (identity.getRole().isPresent()) {
            sb.append(",Role:");
            sb.append(identity.getRole());
        }
        if (identity.getExtraCredentials() != null) {
            identity.getExtraCredentials().forEach((key, value) -> {
                sb.append(",");
                sb.append(key);
                sb.append(":");
                sb.append(value);
            });
        }

        if (catalogType == NESSIE) {
            sb.append(getNessieReferenceName(session));
            sb.append("@");
            sb.append(getNessieReferenceHash(session));
        }

        return sb.toString();
    }

    private Configuration getHadoopConfiguration(ConnectorSession session)
    {
        Configuration configuration = new Configuration(false);
        if (hadoopConfigResources.isEmpty()) {
            configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        }
        for (String resourcePath : hadoopConfigResources) {
            Configuration resourceProperties = new Configuration(false);
            resourceProperties.addResource(new Path(resourcePath));
            for (Map.Entry<String, String> entry : resourceProperties) {
                configuration.set(entry.getKey(), entry.getValue());
            }
        }
        return configuration;
    }

    public Map<String, String> getCatalogProperties(ConnectorSession session)
    {
        Map<String, String> properties = new HashMap<>();
        if (catalogWarehouse != null) {
            properties.put(WAREHOUSE_LOCATION, catalogWarehouse);
        }
        if (catalogType == NESSIE) {
            properties.put("ref", getNessieReferenceName(session));
            properties.put("uri", nessieConfig.getServerUri().orElseThrow(() -> new IllegalStateException("iceberg.nessie.uri must be set for Nessie")));
            String hash = getNessieReferenceHash(session);
            if (hash != null) {
                properties.put("ref.hash", hash);
            }
            nessieConfig.getReadTimeoutMillis().ifPresent(val -> properties.put("transport.read-timeout", val.toString()));
            nessieConfig.getConnectTimeoutMillis().ifPresent(val -> properties.put("transport.connect-timeout", val.toString()));
            nessieConfig.getClientBuilderImpl().ifPresent(val -> properties.put("client-builder-impl", val));
            nessieConfig.getAuthenticationType().ifPresent(type -> {
                if (type == BASIC) {
                    properties.put("authentication.username", nessieConfig.getUsername()
                            .orElseThrow(() -> new IllegalStateException("iceberg.nessie.auth.basic.username must be set with BASIC authentication")));
                    properties.put("authentication.password", nessieConfig.getPassword()
                            .orElseThrow(() -> new IllegalStateException("iceberg.nessie.auth.basic.password must be set with BASIC authentication")));
                }
                else if (type == BEARER) {
                    properties.put("authentication.token", nessieConfig.getBearerToken()
                            .orElseThrow(() -> new IllegalStateException("iceberg.nessie.auth.bearer.token must be set with BEARER authentication")));
                }
            });
            if (nessieConfig.isCompressionDisabled()) {
                properties.put("transport.disable-compression", "true");
            }
        }
        return properties;
    }
}
