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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.iceberg.IcebergTableName;
import com.facebook.presto.iceberg.IcebergUtil;
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
import static java.util.Objects.requireNonNull;

public class SetTablePropertyProcedure
        implements Provider<Procedure>
{
    private static final Logger LOG = Logger.get(SetTablePropertyProcedure.class);
    private static final MethodHandle SET_TABLE_PROPERTY = methodHandle(
            SetTablePropertyProcedure.class,
            "setTableProperty",
            ConnectorSession.class,
            String.class,
            String.class,
            String.class,
            String.class);

    private final IcebergConfig config;
    private final IcebergMetadataFactory metadataFactory;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public SetTablePropertyProcedure(
            IcebergConfig config,
            IcebergMetadataFactory metadataFactory,
            HdfsEnvironment hdfsEnvironment)
    {
        this.config = requireNonNull(config);
        this.metadataFactory = requireNonNull(metadataFactory);
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment);
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "set_table_property",
                ImmutableList.of(
                        new Procedure.Argument("schema", VARCHAR),
                        new Procedure.Argument("table_name", VARCHAR),
                        new Procedure.Argument("key", VARCHAR),
                        new Procedure.Argument("value", VARCHAR)),
                SET_TABLE_PROPERTY.bindTo(this));
    }

    /**
     * Sets a key-value string pair inside an Iceberg table's properties
     *
     * @param session the client's session information
     * @param schema the schema where the table exists
     * @param table the name of the table to sample from
     */
    public void setTableProperty(ConnectorSession session, String schema, String table, String key, String value)
    {
        ConnectorMetadata metadata = metadataFactory.create();
        IcebergTableName tableName = IcebergTableName.from(table);
        SchemaTableName schemaTableName = new SchemaTableName(schema, tableName.getTableName());
        Table icebergTable = IcebergUtil.getIcebergTable(metadata, session, schemaTableName);

        icebergTable.updateProperties()
                .set(key, value)
                .commit();
    }
}
