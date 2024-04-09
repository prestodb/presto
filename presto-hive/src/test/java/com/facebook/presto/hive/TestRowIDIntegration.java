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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.ROW_ID_COLUMN_NAME;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.TPCH_SCHEMA;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.PART_SUPPLIER;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRowIDIntegration
        extends AbstractTestQueryFramework
{
    private final String catalog;

    @SuppressWarnings("unused")
    public TestRowIDIntegration()
    {
        this(HIVE_CATALOG);
    }

    protected TestRowIDIntegration(String catalog)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(ORDERS, CUSTOMER, LINE_ITEM, PART_SUPPLIER, NATION);
    }

    @Test
    public void testRowIDHiddenColumnORC()
    {
        Session session = getSession();
        testRowIDHiddenColumn(session, ORC);
    }

    @Test
    public void testRowIDHiddenColumnDWRF()
    {
        Session session = getSession();
        testRowIDHiddenColumn(session, DWRF);
    }

    private void testRowIDHiddenColumn(Session session, HiveStorageFormat storageFormat)
    {
        String table = "test_row_id_" + storageFormat.name().toLowerCase(Locale.ENGLISH);
        @Language("SQL") String createTable = "CREATE TABLE " + table + " " +
                "(col0 INTEGER, col1 INTEGER) " +
                "WITH (format = '" + storageFormat + "')";
        assertUpdate(session, createTable);
        assertTrue(getQueryRunner().tableExists(getSession(), table));

        assertUpdate(session, "INSERT INTO " + table + " VALUES (0, 0)", 1);

        try {
            TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, table);

            List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_NAME, ROW_ID_COLUMN_NAME);
            List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
            assertEquals(columnMetadatas.size(), columnNames.size());
            for (int i = 0; i < columnMetadatas.size(); i++) {
                ColumnMetadata columnMetadata = columnMetadatas.get(i);
                assertEquals(columnMetadata.getName(), columnNames.get(i));
                if (columnMetadata.getName().equals(ROW_ID_COLUMN_NAME)) {
                    assertTrue(columnMetadata.isHidden(), "Row ID is not hidden");
                    // TODO assert more properties of $row_id
                }
            }

            MaterializedResult results = computeActual(session, "SELECT \"$row_id\" FROM " + table);
            int rowCount = results.getRowCount();
            assertEquals(rowCount, 1);
            for (int i = 0; i < rowCount; i++) {
                MaterializedRow row = results.getMaterializedRows().get(i);
                byte[] rowID = (byte[]) row.getField(0);

                // TODO assert more properties of $row_id
                // problem could be we don't talk to metastore so we don't get a partition ID; how to mock this?
                // perhaps TestPrismIntegrationSmokeTest but probably still doesn'Pt talk to real metastore
                assertTrue(rowID.length > 0, "Zero length row ID for row " + i);
            }
        }
        finally {
            assertUpdate(session, "DROP TABLE " + table);
            assertFalse(getQueryRunner().tableExists(session, table));
        }
    }

    // TODO push up into superclass to remove duplicate code
    private TableMetadata getTableMetadata(String catalog, String schema, String tableName)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    Optional<TableHandle> tableHandle = metadata.getMetadataResolver(transactionSession).getTableHandle(new QualifiedObjectName(catalog, schema, tableName));
                    assertTrue(tableHandle.isPresent());
                    return metadata.getTableMetadata(transactionSession, tableHandle.get());
                });
    }
}
