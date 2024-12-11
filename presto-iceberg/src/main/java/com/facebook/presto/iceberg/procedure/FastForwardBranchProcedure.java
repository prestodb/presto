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
import org.apache.iceberg.Table;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static java.util.Objects.requireNonNull;

public class FastForwardBranchProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle FAST_FORWARD = methodHandle(
            FastForwardBranchProcedure.class,
            "fastForwardToBranch",
            ConnectorSession.class,
            String.class,
            String.class,
            String.class,
            String.class);

    private final IcebergMetadataFactory metadataFactory;

    @Inject
    public FastForwardBranchProcedure(IcebergMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "fast_forward",
                ImmutableList.of(
                        new Procedure.Argument("schema", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR),
                        new Procedure.Argument("branch", VARCHAR),
                        new Procedure.Argument("to", VARCHAR)),
                FAST_FORWARD.bindTo(this));
    }

    public void fastForwardToBranch(ConnectorSession clientSession, String schemaName, String tableName, String fromBranch, String targetBranch)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        ConnectorMetadata metadata = metadataFactory.create();
        Table icebergTable = getIcebergTable(metadata, clientSession, schemaTableName);
        icebergTable.manageSnapshots().fastForwardBranch(fromBranch, targetBranch).commit();
    }
}
