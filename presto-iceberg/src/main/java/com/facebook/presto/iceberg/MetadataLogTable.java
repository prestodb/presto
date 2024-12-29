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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.util.SnapshotUtil;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class MetadataLogTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;

    private static final List<ColumnMetadata> COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("timestamp", TIMESTAMP_WITH_TIME_ZONE))
            .add(new ColumnMetadata("file", VARCHAR))
            .add(new ColumnMetadata("latest_snapshot_id", BIGINT))
            .add(new ColumnMetadata("latest_schema_id", BIGINT))
            .add(new ColumnMetadata("latest_sequence_number", BIGINT))
            .build();

    public MetadataLogTable(SchemaTableName tableName, Table icebergTable)
    {
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), COLUMNS);
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
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
        InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(COLUMNS);

        TableMetadata metadata = ((org.apache.iceberg.BaseTable) icebergTable).operations().current();
        List<MetadataLogEntry> metadataLogEntries = metadata.previousFiles();

        processMetadataLogEntries(table, session, metadataLogEntries);
        addLatestMetadataEntry(table, session, metadata);

        return table.build().cursor();
    }

    private void processMetadataLogEntries(InMemoryRecordSet.Builder table, ConnectorSession session, List<MetadataLogEntry> metadataLogEntries)
    {
        if (metadataLogEntries.isEmpty()) {
            return;
        }

        for (MetadataLogEntry entry : metadataLogEntries) {
            Long snapshotId = null;
            Snapshot snapshot = null;
            try {
                snapshotId = SnapshotUtil.snapshotIdAsOfTime(icebergTable, entry.timestampMillis());
                snapshot = icebergTable.snapshot(snapshotId);
            }
            catch (IllegalArgumentException ignored) {
                // Implies this metadata file was created during table creation
            }
            addRow(table, session, entry.timestampMillis(), entry.file(), snapshotId, snapshot);
        }
    }

    private void addLatestMetadataEntry(InMemoryRecordSet.Builder table, ConnectorSession session, TableMetadata metadata)
    {
        Snapshot latestSnapshot = icebergTable.currentSnapshot();
        Long latestSnapshotId = (latestSnapshot != null) ? latestSnapshot.snapshotId() : null;

        addRow(table, session, metadata.lastUpdatedMillis(), metadata.metadataFileLocation(), latestSnapshotId, latestSnapshot);
    }

    private void addRow(InMemoryRecordSet.Builder table, ConnectorSession session, long timestampMillis, String fileLocation, Long snapshotId, Snapshot snapshot)
    {
        table.addRow(packDateTimeWithZone(timestampMillis, session.getSqlFunctionProperties().getTimeZoneKey()),
                fileLocation,
                snapshotId,
                snapshot != null ? snapshot.schemaId() : null,
                snapshot != null ? snapshot.sequenceNumber() : null);
    }
}
