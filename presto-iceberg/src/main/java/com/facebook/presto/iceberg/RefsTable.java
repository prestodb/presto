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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.iceberg.util.PageListBuilder;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Table;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static java.util.Objects.requireNonNull;

public class RefsTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;

    private static final List<ColumnMetadata> COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(ColumnMetadata.builder().setName("name").setType(VARCHAR).build())
            .add(ColumnMetadata.builder().setName("type").setType(VARCHAR).build())
            .add(ColumnMetadata.builder().setName("snapshot_id").setType(BIGINT).build())
            .add(ColumnMetadata.builder().setName("max_reference_age_in_ms").setType(BIGINT).build())
            .add(ColumnMetadata.builder().setName("min_snapshots_to_keep").setType(BIGINT).build())
            .add(ColumnMetadata.builder().setName("max_snapshot_age_in_ms").setType(BIGINT).build())
            .build();

    public RefsTable(SchemaTableName tableName, Table icebergTable)
    {
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), COLUMNS);
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return new FixedPageSource(buildPages(tableMetadata, icebergTable));
    }

    private static void appendValue(Long value, PageListBuilder pagesBuilder)
    {
        if (value == null) {
            pagesBuilder.appendNull();
        }
        else {
            pagesBuilder.appendBigint(value);
        }
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, Table icebergTable)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        icebergTable.refs().forEach((key, value) -> {
            pagesBuilder.beginRow();
            pagesBuilder.appendVarchar(key);
            pagesBuilder.appendVarchar(String.valueOf(value.type()));
            pagesBuilder.appendBigint(value.snapshotId());
            appendValue(value.maxRefAgeMs(), pagesBuilder);
            appendValue(value.minSnapshotsToKeep() != null ? Long.valueOf(value.minSnapshotsToKeep()) : null, pagesBuilder);
            appendValue(value.maxSnapshotAgeMs(), pagesBuilder);
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }
}
