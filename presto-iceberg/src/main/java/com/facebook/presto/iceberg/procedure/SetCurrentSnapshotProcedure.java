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

import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SetCurrentSnapshotProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle SET_CURRENT_SNAPSHOT = methodHandle(
            SetCurrentSnapshotProcedure.class,
            "setCurrentSnapshot",
            ConnectorSession.class,
            String.class,
            String.class,
            Long.class,
            String.class);

    private final IcebergMetadataFactory metadataFactory;

    @Inject
    public SetCurrentSnapshotProcedure(IcebergMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "set_current_snapshot",
                ImmutableList.of(
                        new Procedure.Argument("schema", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR),
                        new Procedure.Argument("snapshot_id", BIGINT, false, null),
                        new Procedure.Argument("ref", VARCHAR, false, null)),
                SET_CURRENT_SNAPSHOT.bindTo(this));
    }

    public void setCurrentSnapshot(ConnectorSession clientSession, String schema, String table, Long snapshotId, String reference)
    {
        checkState((snapshotId != null && reference == null) || (snapshotId == null && reference != null),
                "Either snapshot_id or reference must be provided, not both");
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        ConnectorMetadata metadata = metadataFactory.create();
        Table icebergTable = getIcebergTable(metadata, clientSession, schemaTableName);
        long targetSnapshotId = snapshotId != null ? snapshotId : getSnapshotIdFromReference(icebergTable, reference);
        icebergTable.manageSnapshots().setCurrentSnapshot(targetSnapshotId).commit();
    }

    private long getSnapshotIdFromReference(Table table, String refName)
    {
        SnapshotRef ref = table.refs().get(refName);
        checkState(ref != null, "Cannot find matching snapshot ID for ref " + refName);
        return ref.snapshotId();
    }
}
