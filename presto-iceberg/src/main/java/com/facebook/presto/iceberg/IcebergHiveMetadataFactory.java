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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
import com.facebook.presto.spi.ConnectorSystemConfig;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpressionService;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class IcebergHiveMetadataFactory
        implements IcebergMetadataFactory
{
    final IcebergCatalogName catalogName;
    final ExtendedHiveMetastore metastore;
    final HdfsEnvironment hdfsEnvironment;
    final TypeManager typeManager;
    final JsonCodec<CommitTaskData> commitTaskCodec;
    final StandardFunctionResolution functionResolution;
    final RowExpressionService rowExpressionService;
    final NodeVersion nodeVersion;
    final FilterStatsCalculatorService filterStatsCalculatorService;
    final IcebergHiveTableOperationsConfig operationsConfig;
    final StatisticsFileCache statisticsFileCache;
    final ManifestFileCache manifestFileCache;
    final IcebergTableProperties tableProperties;
    final ConnectorSystemConfig connectorSystemConfig;

    @Inject
    public IcebergHiveMetadataFactory(
            IcebergCatalogName catalogName,
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JsonCodec<CommitTaskData> commitTaskCodec,
            NodeVersion nodeVersion,
            FilterStatsCalculatorService filterStatsCalculatorService,
            IcebergHiveTableOperationsConfig operationsConfig,
            StatisticsFileCache statisticsFileCache,
            ManifestFileCache manifestFileCache,
            IcebergTableProperties tableProperties,
            ConnectorSystemConfig connectorSystemConfig)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.filterStatsCalculatorService = requireNonNull(filterStatsCalculatorService, "filterStatsCalculatorService is null");
        this.operationsConfig = requireNonNull(operationsConfig, "operationsConfig is null");
        this.statisticsFileCache = requireNonNull(statisticsFileCache, "statisticsFileCache is null");
        this.manifestFileCache = requireNonNull(manifestFileCache, "manifestFileCache is null");
        this.tableProperties = requireNonNull(tableProperties, "icebergTableProperties is null");
        this.connectorSystemConfig = requireNonNull(connectorSystemConfig, "connectorSystemConfig is null");
    }

    public ConnectorMetadata create()
    {
        return new IcebergHiveMetadata(
                catalogName,
                metastore,
                hdfsEnvironment,
                typeManager,
                functionResolution,
                rowExpressionService,
                commitTaskCodec,
                nodeVersion,
                filterStatsCalculatorService,
                operationsConfig,
                statisticsFileCache,
                manifestFileCache,
                tableProperties,
                connectorSystemConfig);
    }
}
