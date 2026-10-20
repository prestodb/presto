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
package com.facebook.presto.iceberg.procedure;

import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.iceberg.transaction.IcebergTransactionMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import jakarta.inject.Inject;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.SnapshotUtil;

import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.moveMainBranch;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class RollbackToTimestampProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle ROLLBACK_TO_TIMESTAMP = methodHandle(
            RollbackToTimestampProcedure.class,
            "rollbackToTimestamp",
            ConnectorSession.class,
            String.class,
            String.class,
            SqlTimestamp.class);

    private final IcebergMetadataFactory metadataFactory;

    @Inject
    public RollbackToTimestampProcedure(IcebergMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "rollback_to_timestamp",
                ImmutableList.of(
                        new Argument("schema", VARCHAR),
                        new Argument("table_name", VARCHAR),
                        new Argument("timestamp", TIMESTAMP)),
                ROLLBACK_TO_TIMESTAMP.bindTo(this));
    }

    public void rollbackToTimestamp(ConnectorSession clientSession, String schema, String tableName, SqlTimestamp timestamp)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRollbackToTimestamp(clientSession, schema, tableName, timestamp);
        }
    }

    private void doRollbackToTimestamp(ConnectorSession clientSession, String schema, String tableName, SqlTimestamp timestamp)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schema, tableName);
        IcebergTransactionMetadata metadata = metadataFactory.create();
        long timestampMillis = timestamp.isLegacyTimestamp() ? timestamp.getMillisUtc() : timestamp.getMillis();
        moveMainBranch(
                getIcebergTable(metadata, clientSession, schemaTableName),
                schemaTableName,
                icebergTable -> latestAncestorOlderThan(icebergTable, timestampMillis),
                ManageSnapshots::rollbackTo);
        metadata.commit();
    }

    /**
     * Picks the same target as {@link ManageSnapshots#rollbackToTime}, and fails with the same message.
     */
    private static long latestAncestorOlderThan(Table icebergTable, long timestampMillis)
    {
        Optional<Snapshot> target = Streams.stream(SnapshotUtil.currentAncestors(icebergTable))
                .filter(snapshot -> snapshot.timestampMillis() < timestampMillis)
                .max(comparingLong(Snapshot::timestampMillis));
        checkArgument(target.isPresent(), "Cannot roll back, no valid snapshot older than: %s", timestampMillis);
        return target.get().snapshotId();
    }
}
