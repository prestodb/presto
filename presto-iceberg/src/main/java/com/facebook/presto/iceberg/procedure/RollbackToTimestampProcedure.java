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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
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
                        new Procedure.Argument("schema", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR),
                        new Procedure.Argument("timestamp", TIMESTAMP)),
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
        ConnectorMetadata metadata = metadataFactory.create();
        getIcebergTable(metadata, clientSession, schemaTableName)
                .manageSnapshots()
                .rollbackToTime(timestamp.isLegacyTimestamp() ? timestamp.getMillisUtc() : timestamp.getMillis())
                .commit();
    }
}
