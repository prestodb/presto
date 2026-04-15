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
package com.facebook.presto.iceberg.procedure.context;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergSplitSource;
import com.facebook.presto.iceberg.procedure.splits.RewriteDataFilesIcebergSplitSource;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IcebergRewriteDataFilesProcedureContext
        implements IcebergCommonProcedureContext
{
    final Table table;
    final IcebergAbstractMetadata metadata;
    final Map<String, String> options;

    public IcebergRewriteDataFilesProcedureContext(Table table, IcebergAbstractMetadata metadata)
    {
        this(table, metadata, ImmutableMap.of());
    }

    public IcebergRewriteDataFilesProcedureContext(Table table, IcebergAbstractMetadata metadata, Map<String, String> options)
    {
        this.table = requireNonNull(table, "table is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.options = ImmutableMap.copyOf(requireNonNull(options, "options is null"));
    }

    public Table getTable()
    {
        return table;
    }

    public IcebergAbstractMetadata getMetadata()
    {
        return metadata;
    }

    public Map<String, String> getOptions()
    {
        return options;
    }

    @Override
    public Optional<IcebergSplitSource> customizeSplitSource(ConnectorSession session,
                                                             TableScan tableScan,
                                                             TupleDomain<IcebergColumnHandle> metadataColumnConstraints)
    {
        return Optional.of(new RewriteDataFilesIcebergSplitSource(
                session,
                tableScan,
                metadataColumnConstraints,
                options));
    }
}
