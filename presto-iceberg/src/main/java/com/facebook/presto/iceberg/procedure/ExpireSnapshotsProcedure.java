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
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Table;

import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.IcebergLibUtils.withIncrementalCleanup;

public class ExpireSnapshotsProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle EXPIRE_SNAPSHOTS = methodHandle(
            ExpireSnapshotsProcedure.class,
            "expireSnapshots",
            ConnectorSession.class,
            String.class,
            String.class,
            SqlTimestamp.class,
            Integer.class,
            List.class);
    private final IcebergMetadataFactory metadataFactory;

    @Inject
    public ExpireSnapshotsProcedure(IcebergMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "expire_snapshots",
                ImmutableList.of(
                        new Procedure.Argument("schema", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR),
                        new Procedure.Argument("older_than", TIMESTAMP, false, null),
                        new Procedure.Argument("retain_last", INTEGER, false, null),
                        new Procedure.Argument("snapshot_ids", "array(bigint)", false, null)),
                EXPIRE_SNAPSHOTS.bindTo(this));
    }

    public void expireSnapshots(ConnectorSession clientSession, String schema, String tableName, SqlTimestamp olderThan, Integer retainLast, List<Long> snapshotIds)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doExpireSnapshots(clientSession, schema, tableName, olderThan, retainLast, snapshotIds);
        }
    }

    private void doExpireSnapshots(ConnectorSession clientSession, String schema, String tableName, SqlTimestamp olderThan, Integer retainLast, List<Long> snapshotIds)
    {
        IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) metadataFactory.create();
        SchemaTableName schemaTableName = new SchemaTableName(schema, tableName);
        Table icebergTable = IcebergUtil.getIcebergTable(metadata, clientSession, schemaTableName);

        // Incremental clean up strategy has a bug when expire specified snapshots.
        //  So explicitly use reachable file cleanup strategy here.
        //  Referring to https://github.com/apache/iceberg/issues/10982
        ExpireSnapshots expireSnapshots = withIncrementalCleanup(icebergTable.expireSnapshots(), false);

        if (snapshotIds != null) {
            for (long id : snapshotIds) {
                expireSnapshots = expireSnapshots.expireSnapshotId(id);
            }
        }

        if (olderThan != null) {
            expireSnapshots = expireSnapshots.expireOlderThan(olderThan.isLegacyTimestamp() ? olderThan.getMillisUtc() : olderThan.getMillis());
        }

        if (retainLast != null) {
            expireSnapshots = expireSnapshots.retainLast(retainLast);
        }

        expireSnapshots.cleanExpiredFiles(true)
                .commit();
    }
}
