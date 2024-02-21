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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopCatalog;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.CatalogType.NESSIE;
import static com.facebook.presto.iceberg.IcebergUtil.getHiveIcebergTable;
import static com.facebook.presto.iceberg.samples.SampleUtil.SAMPLE_TABLE_ID;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.util.Objects.requireNonNull;

public class CreateSampleTableProcedure
        implements Provider<Procedure>
{
    private static final Logger LOG = Logger.get(CreateSampleTableProcedure.class);
    private static final MethodHandle CREATE_SAMPLE_TABLE = methodHandle(
            CreateSampleTableProcedure.class,
            "createSampleTable",
            ConnectorSession.class,
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
                        new Procedure.Argument("table", VARCHAR)),
                CREATE_SAMPLE_TABLE.bindTo(this));
    }

    /**
     * Creates a new table sample alongside the given iceberg table with the
     * given schema and table name.
     *
     * @param clientSession the client's session information
     * @param table the name of the table to sample from
     */
    public void createSampleTable(ConnectorSession clientSession, String table)
    {
        SchemaTableName schemaTableName = clientSession.getSchema().map(schema -> new SchemaTableName(schema, table)).orElseThrow(() -> new PrestoException(INVALID_ARGUMENTS, "schema must be specified in session. Try executing 'USE <schema>' first."));
        ConnectorMetadata metadata = metadataFactory.create();
        Table icebergTable;
        CatalogType catalogType = config.getCatalogType();

        if (catalogType == HADOOP || catalogType == NESSIE) {
            icebergTable = resourceFactory.getCatalog(clientSession).loadTable(toIcebergTableIdentifier(schemaTableName.getSchemaName(), table));
        }
        else {
            ExtendedHiveMetastore metastore = ((IcebergHiveMetadata) metadata).getMetastore();
            icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, clientSession, schemaTableName);
        }

        try (HadoopCatalog catalog = SampleUtil.getCatalogForSampleTable(icebergTable, schemaTableName.getSchemaName(), hdfsEnvironment, clientSession)) {
            // create the table for samples and load the table back to make sure it's valid
            catalog.createTable(SAMPLE_TABLE_ID, icebergTable.schema());
        }
        catch (IOException e) {
            throw new PrestoException(IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR, e);
        }
    }
}
