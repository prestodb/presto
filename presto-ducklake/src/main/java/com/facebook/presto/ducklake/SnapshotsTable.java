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
package com.facebook.presto.ducklake;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.ducklake.catalog.DuckLakeCatalog;
import com.facebook.presto.ducklake.catalog.DuckLakeSnapshot;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * The {@code "<table>$snapshots"} system table (spec &sect;4.8), backed by {@code
 * ducklake_snapshot} joined with {@code ducklake_snapshot_changes}. DuckLake snapshots are
 * catalog-wide, not per table, so this lists every snapshot in the catalog regardless of which
 * table name it was addressed through.
 */
public class SnapshotsTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final DuckLakeCatalog catalog;

    public SnapshotsTable(SchemaTableName tableName, DuckLakeCatalog catalog)
    {
        requireNonNull(tableName, "tableName is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.tableMetadata = new ConnectorTableMetadata(
                tableName,
                ImmutableList.of(
                        ColumnMetadata.builder().setName("snapshot_id").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("snapshot_time").setType(TIMESTAMP_WITH_TIME_ZONE).build(),
                        ColumnMetadata.builder().setName("schema_version").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("author").setType(VARCHAR).build(),
                        ColumnMetadata.builder().setName("commit_message").setType(VARCHAR).build(),
                        ColumnMetadata.builder().setName("changes").setType(VARCHAR).build()));
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(tableMetadata);
        for (DuckLakeSnapshot snapshot : catalog.listSnapshots()) {
            table.addRow(
                    snapshot.getSnapshotId(),
                    packDateTimeWithZone(snapshot.getSnapshotTime().toEpochMilli(), UTC_KEY),
                    snapshot.getSchemaVersion(),
                    snapshot.getAuthor().orElse(null),
                    snapshot.getCommitMessage().orElse(null),
                    snapshot.getChangesMade().orElse(null));
        }
        return table.build().cursor();
    }
}
