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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Table;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static java.util.Objects.requireNonNull;

public class RollbackToSnapshotProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle ROLLBACK_TO_SNAPSHOT = methodHandle(
            RollbackToSnapshotProcedure.class,
            "rollbackToSnapshot",
            ConnectorSession.class,
            String.class,
            String.class,
            Long.class);

    private final IcebergMetadataFactory metadataFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final IcebergResourceFactory resourceFactory;
    private final boolean nativeCatalogMode;

    @Inject
    public RollbackToSnapshotProcedure(
            IcebergConfig config,
            IcebergMetadataFactory metadataFactory,
            HdfsEnvironment hdfsEnvironment,
            IcebergResourceFactory resourceFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.resourceFactory = requireNonNull(resourceFactory, "resourceFactory is null");
        requireNonNull(config, "config is null");
        this.nativeCatalogMode = config.isNativeMode();
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "rollback_to_snapshot",
                ImmutableList.of(
                        new Procedure.Argument("schema", VARCHAR),
                        new Procedure.Argument("table", VARCHAR),
                        new Procedure.Argument("snapshot_id", BIGINT)),
                ROLLBACK_TO_SNAPSHOT.bindTo(this));
    }

    public void rollbackToSnapshot(ConnectorSession clientSession, String schema, String table, Long snapshotId)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        ConnectorMetadata metadata = metadataFactory.create();
        Table icebergTable;
        if (nativeCatalogMode) {
            icebergTable = resourceFactory.getCatalog(clientSession).loadTable(toIcebergTableIdentifier(schema, table));
        }
        else {
            ExtendedHiveMetastore metastore = ((IcebergMetadata) metadata).getMetastore();
            icebergTable = getIcebergTable(metastore, hdfsEnvironment, clientSession, schemaTableName);
        }
        icebergTable.rollback().toSnapshotId(snapshotId).commit();
    }
}
