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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.util.SnapshotUtil.snapshotIdAsOfTime;

public class MetadataLogTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;

    private static final List<ColumnMetadata> COLUMN_DEFINITIONS = ImmutableList.<ColumnMetadata>builder()
            .add(ColumnMetadata.builder().setName("timestamp").setType(TIMESTAMP_WITH_TIME_ZONE).build())
            .add(ColumnMetadata.builder().setName("file").setType(VARCHAR).build())
            .add(ColumnMetadata.builder().setName("latest_snapshot_id").setType(BIGINT).build())
            .add(ColumnMetadata.builder().setName("latest_schema_id").setType(INTEGER).build())
            .add(ColumnMetadata.builder().setName("latest_sequence_number").setType(BIGINT).build())
            .build();

    private static final List<Type> COLUMN_TYPES = COLUMN_DEFINITIONS.stream()
            .map(ColumnMetadata::getType)
            .collect(Collectors.toList());

    public MetadataLogTable(SchemaTableName tableName, Table icebergTable)
    {
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"), COLUMN_DEFINITIONS);
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
        Iterable<List<?>> rowIterable = () -> new Iterator<List<?>>()
        {
            private final Iterator<MetadataLogEntry> metadataLogEntriesIterator = ((BaseTable) icebergTable).operations().current().previousFiles().iterator();
            private boolean addedLatestEntry;

            @Override
            public boolean hasNext()
            {
                return metadataLogEntriesIterator.hasNext() || !addedLatestEntry;
            }

            @Override
            public List<?> next()
            {
                if (metadataLogEntriesIterator.hasNext()) {
                    return processMetadataLogEntries(session, metadataLogEntriesIterator.next());
                }
                if (!addedLatestEntry) {
                    addedLatestEntry = true;
                    TableMetadata currentMetadata = ((BaseTable) icebergTable).operations().current();
                    return buildLatestMetadataRow(session, currentMetadata);
                }
                throw new NoSuchElementException();
            }
        };
        return new InMemoryRecordSet(COLUMN_TYPES, rowIterable).cursor();
    }

    private List<?> processMetadataLogEntries(ConnectorSession session, MetadataLogEntry metadataLogEntry)
    {
        Long snapshotId = null;
        Snapshot snapshot = null;
        try {
            snapshotId = snapshotIdAsOfTime(icebergTable, metadataLogEntry.timestampMillis());
            snapshot = icebergTable.snapshot(snapshotId);
        }
        catch (IllegalArgumentException ignored) {
            // Implies this metadata file was created during table creation
        }
        return addRow(session, metadataLogEntry.timestampMillis(), metadataLogEntry.file(), snapshotId, snapshot);
    }

    private List<?> buildLatestMetadataRow(ConnectorSession session, TableMetadata metadata)
    {
        Snapshot latestSnapshot = icebergTable.currentSnapshot();
        Long latestSnapshotId = (latestSnapshot != null) ? latestSnapshot.snapshotId() : null;

        return addRow(session, metadata.lastUpdatedMillis(), metadata.metadataFileLocation(), latestSnapshotId, latestSnapshot);
    }

    private List<?> addRow(ConnectorSession session, long timestampMillis, String fileLocation, Long snapshotId, Snapshot snapshot)
    {
        return Arrays.asList(
                packDateTimeWithZone(timestampMillis, session.getSqlFunctionProperties().getTimeZoneKey()),
                fileLocation,
                snapshotId,
                snapshot != null ? snapshot.schemaId() : null,
                snapshot != null ? snapshot.sequenceNumber() : null);
    }
}
