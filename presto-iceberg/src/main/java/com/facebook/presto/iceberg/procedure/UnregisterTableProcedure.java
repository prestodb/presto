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

import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static java.util.Objects.requireNonNull;

public class UnregisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle UNREGISTER_TABLE = methodHandle(
            UnregisterTableProcedure.class,
            "unregisterTable",
            ConnectorSession.class,
            String.class,
            String.class);

    private final IcebergMetadataFactory metadataFactory;

    @Inject
    public UnregisterTableProcedure(IcebergMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "unregister_table",
                ImmutableList.of(
                        new Procedure.Argument("schema", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR)),
                UNREGISTER_TABLE.bindTo(this));
    }

    public void unregisterTable(ConnectorSession clientSession, String schema, String table)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doUnregisterTable(clientSession, schema, table);
        }
    }

    private void doUnregisterTable(ConnectorSession clientSession, String schema, String table)
    {
        IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) metadataFactory.create();
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        if (!metadata.schemaExists(clientSession, schemaTableName.getSchemaName())) {
            throw new SchemaNotFoundException(schemaTableName.getSchemaName());
        }

        metadata.unregisterTable(clientSession, schemaTableName);
    }
}
