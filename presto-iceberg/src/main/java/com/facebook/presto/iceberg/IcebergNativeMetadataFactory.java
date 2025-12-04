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
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpressionService;
import jakarta.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class IcebergNativeMetadataFactory
        implements IcebergMetadataFactory
{
    final TypeManager typeManager;
    final JsonCodec<CommitTaskData> commitTaskCodec;
    final JsonCodec<List<MaterializedViewDefinition.ColumnMapping>> columnMappingsCodec;
    final JsonCodec<List<SchemaTableName>> schemaTableNamesCodec;
    final IcebergNativeCatalogFactory catalogFactory;
    final CatalogType catalogType;
    final StandardFunctionResolution functionResolution;
    final RowExpressionService rowExpressionService;
    final NodeVersion nodeVersion;
    final FilterStatsCalculatorService filterStatsCalculatorService;
    final StatisticsFileCache statisticsFileCache;
    final IcebergTableProperties tableProperties;

    @Inject
    public IcebergNativeMetadataFactory(
            IcebergConfig config,
            IcebergNativeCatalogFactory catalogFactory,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JsonCodec<CommitTaskData> commitTaskCodec,
            JsonCodec<List<MaterializedViewDefinition.ColumnMapping>> columnMappingsCodec,
            JsonCodec<List<SchemaTableName>> schemaTableNamesCodec,
            NodeVersion nodeVersion,
            FilterStatsCalculatorService filterStatsCalculatorService,
            StatisticsFileCache statisticsFileCache,
            IcebergTableProperties tableProperties)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.columnMappingsCodec = requireNonNull(columnMappingsCodec, "columnMappingsCodec is null");
        this.schemaTableNamesCodec = requireNonNull(schemaTableNamesCodec, "schemaTableNamesCodec is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.catalogType = config.getCatalogType();
        this.filterStatsCalculatorService = requireNonNull(filterStatsCalculatorService, "filterStatsCalculatorService is null");
        this.statisticsFileCache = requireNonNull(statisticsFileCache, "statisticsFileCache is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
    }

    public ConnectorMetadata create()
    {
        return new IcebergNativeMetadata(catalogFactory, typeManager, functionResolution, rowExpressionService, commitTaskCodec, columnMappingsCodec, schemaTableNamesCodec, catalogType, nodeVersion, filterStatsCalculatorService, statisticsFileCache, tableProperties);
    }
}
