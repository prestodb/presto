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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.iceberg.samples.SampleUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateProperties;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.CatalogType.NESSIE;
import static com.facebook.presto.iceberg.IcebergTableProperties.SAMPLE_TABLE_LAST_SNAPSHOT;
import static com.facebook.presto.iceberg.IcebergTableProperties.SAMPLE_TABLE_PRIMARY_KEY;
import static com.facebook.presto.iceberg.IcebergUtil.getHiveIcebergTable;
import static com.facebook.presto.iceberg.samples.SampleUtil.SAMPLE_TABLE_ID;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static java.util.Objects.requireNonNull;

public class CreateSampleTableProcedure
        implements Provider<Procedure>
{
    private static final Logger LOG = Logger.get(CreateSampleTableProcedure.class);
    private static final MethodHandle CREATE_SAMPLE_TABLE = methodHandle(
            CreateSampleTableProcedure.class,
            "createSampleTable",
            ConnectorSession.class,
            String.class,
            String.class,
            String.class);

    private final IcebergConfig config;
    private final IcebergMetadataFactory metadataFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final IcebergResourceFactory resourceFactory;

    @Inject
    public CreateSampleTableProcedure(
            IcebergConfig config,
            IcebergMetadataFactory metadataFactory,
            HdfsEnvironment hdfsEnvironment,
            IcebergResourceFactory resourceFactory)
    {
        this.config = requireNonNull(config);
        this.metadataFactory = requireNonNull(metadataFactory);
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment);
        this.resourceFactory = requireNonNull(resourceFactory);
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "create_sample_table",
                ImmutableList.of(
                        new Procedure.Argument("schema", VARCHAR),
                        new Procedure.Argument("table", VARCHAR),
                        new Procedure.Argument("primary_key", VARCHAR)),
                CREATE_SAMPLE_TABLE.bindTo(this));
    }

    /**
     * Creates a new table sample alongside the given iceberg table with the
     * given schema and table name.
     *
     * @param clientSession the client's session information
     * @param schema the schema where the table exists
     * @param table the name of the table to sample from
     */
    public void createSampleTable(ConnectorSession clientSession, String schema, String table, String primaryKey)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        ConnectorMetadata metadata = metadataFactory.create();
        Table icebergTable;
        CatalogType catalogType = config.getCatalogType();

        if (catalogType == HADOOP || catalogType == NESSIE) {
            icebergTable = resourceFactory.getCatalog(clientSession).loadTable(toIcebergTableIdentifier(schema, table));
        }
        else {
            ExtendedHiveMetastore metastore = ((IcebergHiveMetadata) metadata).getMetastore();
            icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, clientSession, schemaTableName);
        }
        try (SampleUtil.AutoCloseableCatalog c = SampleUtil.getCatalogForSampleTable(icebergTable, schema, hdfsEnvironment, clientSession)) {
            // create the table for samples and load the table back to make sure it's valid
            c.createTable(SAMPLE_TABLE_ID, icebergTable.schema());
        }
        UpdateProperties properties = icebergTable.updateProperties()
                .set(SAMPLE_TABLE_PRIMARY_KEY, primaryKey);
        if (icebergTable.currentSnapshot() != null) {
            properties.set(SAMPLE_TABLE_LAST_SNAPSHOT, Long.toString(icebergTable.currentSnapshot().snapshotId()));
        }
        properties.commit();
    }
}
