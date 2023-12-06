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
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.RowExpressionService;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class IcebergNativeMetadataFactory
        implements IcebergMetadataFactory
{
    final TypeManager typeManager;
    final JsonCodec<CommitTaskData> commitTaskCodec;
    final IcebergResourceFactory resourceFactory;
    final CatalogType catalogType;
    final StandardFunctionResolution functionResolution;
    final RowExpressionService rowExpressionService;
    final NodeVersion nodeVersion;

    @Inject
    public IcebergNativeMetadataFactory(
            IcebergConfig config,
            IcebergResourceFactory resourceFactory,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JsonCodec<CommitTaskData> commitTaskCodec,
            NodeVersion nodeVersion)
    {
        this.resourceFactory = requireNonNull(resourceFactory, "resourceFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        requireNonNull(config, "config is null");
        this.catalogType = config.getCatalogType();
    }

    public ConnectorMetadata create()
    {
        return new IcebergNativeMetadata(resourceFactory, typeManager, functionResolution, rowExpressionService, commitTaskCodec, catalogType, nodeVersion);
    }
}
