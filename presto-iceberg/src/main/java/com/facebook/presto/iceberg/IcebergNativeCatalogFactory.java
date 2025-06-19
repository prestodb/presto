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

import com.facebook.presto.hive.azure.AzureConfigurationInitializer;
import com.facebook.presto.hive.gcs.GcsConfigurationInitializer;
import com.facebook.presto.hive.s3.S3ConfigurationUpdater;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
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
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.iceberg.IcebergUtil.loadCachingProperties;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.FILE_IO_IMPL;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;

/**
 * Factory for loading resources related to the Iceberg Catalog.
 */
public class IcebergNativeCatalogFactory
{
    protected final Cache<String, Catalog> catalogCache;
    private final String catalogName;
    protected final CatalogType catalogType;
    private final String catalogWarehouse;
    private final String catalogWarehouseDataDir;
    protected final IcebergConfig icebergConfig;

    private final List<String> hadoopConfigResources;
    private final S3ConfigurationUpdater s3ConfigurationUpdater;
    private final GcsConfigurationInitializer gcsConfigurationInitialize;
    private final AzureConfigurationInitializer azureConfigurationInitialize;

    @Inject
    public IcebergNativeCatalogFactory(
            IcebergConfig config,
            IcebergCatalogName catalogName,
            S3ConfigurationUpdater s3ConfigurationUpdater,
            GcsConfigurationInitializer gcsConfigurationInitialize,
            AzureConfigurationInitializer azureConfigurationInitialize)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null").getCatalogName();
        this.icebergConfig = requireNonNull(config, "config is null");
        this.catalogType = config.getCatalogType();
        this.catalogWarehouse = config.getCatalogWarehouse();
        this.catalogWarehouseDataDir = config.getCatalogWarehouseDataDir();
        this.hadoopConfigResources = icebergConfig.getHadoopConfigResources();
        this.s3ConfigurationUpdater = requireNonNull(s3ConfigurationUpdater, "s3ConfigurationUpdater is null");
        this.gcsConfigurationInitialize = requireNonNull(gcsConfigurationInitialize, "gcsConfigurationInitialize is null");
        this.azureConfigurationInitialize = requireNonNull(azureConfigurationInitialize, "azureConfigurationInitialize is null");
        catalogCache = CacheBuilder.newBuilder()
                .maximumSize(config.getCatalogCacheSize())
                .build();
    }

    public Catalog getCatalog(ConnectorSession session)
    {
        try {
            return catalogCache.get(getCacheKey(session), () -> CatalogUtil.loadCatalog(
                    catalogType.getCatalogImpl(), catalogName, getProperties(session), getHadoopConfiguration()));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throwIfUnchecked(e);
            throw new UncheckedExecutionException(e);
        }
    }

    public String getCatalogWarehouseDataDir()
    {
        return this.catalogWarehouseDataDir;
    }

    public SupportsNamespaces getNamespaces(ConnectorSession session)
    {
        Catalog catalog = getCatalog(session);
        if (catalog instanceof SupportsNamespaces) {
            return (SupportsNamespaces) catalog;
        }
        throw new PrestoException(NOT_SUPPORTED, "Iceberg catalog of type " + catalogType + " does not support namespace operations");
    }

    public boolean isNestedNamespaceEnabled()
    {
        return false;
    }

    protected String getCacheKey(ConnectorSession session)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(catalogName);
        getCatalogCacheKey(session).ifPresent(sb::append);
        return sb.toString();
    }

    protected Optional<String> getCatalogCacheKey(ConnectorSession session)
    {
        return Optional.empty();
    }

    protected Map<String, String> getProperties(ConnectorSession session)
    {
        Map<String, String> properties = new HashMap<>();
        if (icebergConfig.getManifestCachingEnabled()) {
            properties.putAll(loadCachingProperties(icebergConfig));
        }
        if (icebergConfig.getFileIOImpl() != null) {
            properties.put(FILE_IO_IMPL, icebergConfig.getFileIOImpl());
        }
        if (catalogWarehouse != null) {
            properties.put(WAREHOUSE_LOCATION, catalogWarehouse);
        }

        properties.putAll(getCatalogProperties(session));
        return properties;
    }

    protected Map<String, String> getCatalogProperties(ConnectorSession session)
    {
        return ImmutableMap.of();
    }

    protected Configuration getHadoopConfiguration()
    {
        Configuration configuration = new Configuration(false);

        if (hadoopConfigResources.isEmpty()) {
            configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        }

        s3ConfigurationUpdater.updateConfiguration(configuration);
        gcsConfigurationInitialize.updateConfiguration(configuration);
        azureConfigurationInitialize.updateConfiguration(configuration);

        for (String resourcePath : hadoopConfigResources) {
            Configuration resourceProperties = new Configuration(false);
            resourceProperties.addResource(new Path(resourcePath));
            for (Map.Entry<String, String> entry : resourceProperties) {
                configuration.set(entry.getKey(), entry.getValue());
            }
        }
        return configuration;
    }
}
