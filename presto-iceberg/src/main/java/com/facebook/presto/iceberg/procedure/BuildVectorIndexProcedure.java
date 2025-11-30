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
import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.iceberg.vectors.IcebergVectorIndexBuilder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Procedure for building vector indexes from Iceberg table data.
 */
public class BuildVectorIndexProcedure
        implements Provider<Procedure>
{
    private static final Logger log = Logger.get(BuildVectorIndexProcedure.class);

    private final IcebergTransactionManager transactionManager;
    private final IcebergMetadataFactory metadataFactory;
    private final ConnectorPageSourceProvider pageSourceProvider;

    @Inject
    public BuildVectorIndexProcedure(
            IcebergTransactionManager transactionManager,
            IcebergMetadataFactory metadataFactory,
            ConnectorPageSourceProvider pageSourceProvider)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
    }

    @Override
    public Procedure get()
    {
        // Create the MethodHandle at runtime and bind to this instance so the arity matches
        MethodHandle handle = methodHandle(
                BuildVectorIndexProcedure.class,
                "buildVectorIndex",
                ConnectorSession.class,
                String.class     // columnPath
        ).bindTo(this);

        return new Procedure(
                "system", // Use "system" as the schema name instead of an empty string
                "CREATE_VEC_INDEX",
                ImmutableList.of(
                        new Procedure.Argument("column_path", VARCHAR)),
                handle);
    }

    public void buildVectorIndex(
            ConnectorSession session,
            String columnPath)
    {
        // Parse the column path to extract catalog, schema, table, and column names
        // Format: catalog.schema.table.column or schema.table.column
        String[] parts = columnPath.split("\\.");
        String catalogName = null;
        String schemaName;
        String tableName;
        String columnName;
        if (parts.length == 4) {
            // Format: catalog.schema.table.column
            catalogName = parts[0];
            schemaName = parts[1];
            tableName = parts[2];
            columnName = parts[3];
        }
        else if (parts.length == 3) {
            // Format: schema.table.column
            schemaName = parts[0];
            tableName = parts[1];
            columnName = parts[2];
        }
        else {
            throw new IllegalArgumentException("Invalid column path format. Expected: [catalog.]schema.table.column, got: " + columnPath);
        }
        // Use a generic fixed name for the index
        String indexName = "vector_index";
        log.info("Building vector index for %s.%s.%s", schemaName, tableName, columnName);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        // Use default values for similarity function, m, and ef_construction
        int mValue = 16;
        int efValue = 100;
        String simFunction = "COSINE";

        // Create a new transaction handle for this procedure
        ConnectorTransactionHandle transactionHandle = new HiveTransactionHandle();
        boolean transactionRegistered = false;

        try {
            // Create a new metadata instance and register it with the transaction manager
            ConnectorMetadata metadata = metadataFactory.create();
            transactionManager.put(transactionHandle, metadata);
            transactionRegistered = true;

            Path resultPath = IcebergVectorIndexBuilder.buildAndSaveVectorIndex(
                    metadata,
                    pageSourceProvider,
                    transactionHandle,
                    session,
                    schemaTableName,
                    columnName,
                    indexName,
                    catalogName,  // Pass the catalog name to the builder
                    simFunction,
                    mValue,
                    efValue);
            log.info("Vector index built and saved to %s", resultPath);
        }
        catch (Exception e) {
            log.error(e, "Error building vector index");
            throw new RuntimeException("Error building vector index: " + e.getMessage(), e);
        }
        finally {
            // Only clean up the transaction if it was successfully registered
            if (transactionRegistered) {
                try {
                    transactionManager.remove(transactionHandle);
                }
                catch (Exception e) {
                    log.warn(e, "Failed to remove transaction handle");
                }
            }
        }
    }
}
