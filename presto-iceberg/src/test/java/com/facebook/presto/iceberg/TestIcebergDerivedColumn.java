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

import com.facebook.presto.Session;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.derivedcolumns.DerivedColumnSpec;
import com.facebook.presto.spi.derivedcolumns.DerivedColumnSpecList;
import com.facebook.presto.spi.derivedcolumns.DerivedColumnType;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.QueryRunner.MaterializedResultWithPlan;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergTableProperties.DERIVED_COLUMN_EXPRESSION_SPEC;
import static com.facebook.presto.iceberg.IcebergUtil.DERIVED_COLUMN_SPEC_JSON_CODEC;
import static com.facebook.presto.iceberg.IcebergUtil.DERIVED_COL_EMPTY_SPEC;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsDeep;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestIcebergDerivedColumn
        extends AbstractTestQueryFramework
{
    private static final String ICEBERG_CATALOG = "iceberg";
    private static final String TEST_SCHEMA = "test_schema_derived_col";
    private Session session;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(TEST_SCHEMA)
                .build();

        return IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .setSchemaName(TEST_SCHEMA)
                .setCreateTpchTables(false)
                .build().getQueryRunner();
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate(session, format("CREATE SCHEMA IF NOT EXISTS %s", TEST_SCHEMA));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertUpdate(session, format("DROP SCHEMA IF EXISTS %s", TEST_SCHEMA));
    }

    @Test
    public void testAddColumn()
    {
        try {
            assertUpdate("CREATE TABLE test (c1 BIGINT, c2 VARCHAR)");
            assertUpdate("INSERT INTO test VALUES (123, 'B'), (120, 'C')", 2);
            assertUpdate("ALTER TABLE test ADD COLUMN c2_derived VARCHAR AS lower(c2) PERSISTENT");
            assertUpdate("UPDATE test SET c2_derived = lower(c2)", 2);
            assertTableProperties("test", ImmutableMap.of("c2_derived", "\"lower\"(c2)"));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test");
        }
    }

    @Test
    public void testDeleteColumn()
    {
        try {
            assertUpdate("CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B'), (120, 'C')", 2);
            assertUpdate("ALTER TABLE test2 ADD COLUMN c2_derived VARCHAR AS lower(c2) PERSISTENT");
            assertTableProperties("test2", ImmutableMap.of("c2_derived", "\"lower\"(c2)"));
            // After deleting the derived column,
            assertUpdate("ALTER TABLE test2 DROP COLUMN c2_derived");
            assertTableProperties("test2", ImmutableMap.of());
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test2");
        }
    }

    @Test
    public void testCreateTableWithAddAndDropColumns()
    {
        try {
            assertUpdate("CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR, c2_derived VARCHAR AS lower(c2), " +
                    " c1_derived decimal(19,2) AS (CAST(c1 AS decimal) * DECIMAL '10.5') PERSISTENT)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B', 'b', 1291.5), (120, 'C', 'c', 1260.0)", 2);
            assertTableProperties("test2", ImmutableMap.of("c2_derived", "\"lower\"(c2)", "c1_derived", "(CAST(c1 AS decimal) * DECIMAL '10.5')"));
            // After deleting the derived column,
            assertUpdate("ALTER TABLE test2 DROP COLUMN c2_derived");
            assertTableProperties("test2", ImmutableMap.of("c1_derived", "(CAST(c1 AS decimal) * DECIMAL '10.5')"));
            assertUpdate("ALTER TABLE test2 DROP COLUMN c1_derived");
            assertTableProperties("test2", ImmutableMap.of());
            assertUpdate("ALTER TABLE test2 ADD COLUMN c2_derived VARCHAR GENERATED ALWAYS AS lower(c2) PERSISTENT");
            assertTableProperties("test2", ImmutableMap.of("c2_derived", "\"lower\"(c2)"));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test2");
        }
    }

    @Test
    public void testRenameColumnWithDerivedColumnSpec()
    {
        try {
            assertUpdate("CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR, c2_derived VARCHAR AS lower(c2), " +
                    " c1_derived decimal(19,2) AS (CAST(c1 AS decimal) * DECIMAL '10.5') PERSISTENT)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B', 'b', 1291.5), (120, 'C', 'c', 1260.0)", 2);
            assertTableProperties("test2", ImmutableMap.of("c2_derived", "\"lower\"(c2)", "c1_derived", "(CAST(c1 AS decimal) * DECIMAL '10.5')"));
            assertUpdate("ALTER TABLE test2 RENAME COLUMN c2_derived TO c2_derived2");
            assertTableProperties("test2", ImmutableMap.of("c2_derived2", "\"lower\"(c2)", "c1_derived", "(CAST(c1 AS decimal) * DECIMAL '10.5')"));
            assertUpdate("ALTER TABLE test2 DROP COLUMN c2_derived2");
            assertTableProperties("test2", ImmutableMap.of("c1_derived", "(CAST(c1 AS decimal) * DECIMAL '10.5')"));
            assertUpdate("ALTER TABLE test2 DROP COLUMN c1_derived");
            assertTableProperties("test2", ImmutableMap.of());
            assertUpdate("ALTER TABLE test2 ADD COLUMN c2_derived VARCHAR GENERATED ALWAYS AS lower(c2) PERSISTENT");
            assertTableProperties("test2", ImmutableMap.of("c2_derived", "\"lower\"(c2)"));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test2");
        }
    }

    @Test
    public void testUpdateColumnType()
    {
        try {
            assertUpdate("CREATE TABLE test2 (c1 BIGINT, c2 VARCHAR, c2_derived VARCHAR AS lower(c2), " +
                    " c1_derived decimal(12,2) AS (CAST(c1 AS decimal) * DECIMAL '10.5') PERSISTENT)");
            assertUpdate("INSERT INTO test2 VALUES (123, 'B', 'b', 1291.5), (120, 'C', 'c', 1260.0)", 2);
            assertTableProperties("test2", ImmutableMap.of("c2_derived", "\"lower\"(c2)", "c1_derived", "(CAST(c1 AS decimal) * DECIMAL '10.5')"));
            assertUpdate("ALTER TABLE test2 ALTER COLUMN c1_derived SET DATA TYPE DECIMAL(19,2)");
            assertTableProperties("test2", ImmutableMap.of("c2_derived", "\"lower\"(c2)", "c1_derived", "(CAST(c1 AS decimal) * DECIMAL '10.5')"));
            assertUpdate("ALTER TABLE test2 RENAME COLUMN c1_derived TO c1_derived2");
            assertTableProperties("test2", ImmutableMap.of("c2_derived", "\"lower\"(c2)", "c1_derived2", "(CAST(c1 AS decimal) * DECIMAL '10.5')"));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test2");
        }
    }

    @Test
    public void testShowCreateTableIncludesDerivedColumns()
    {
        try {
            assertUpdate("CREATE TABLE test_ddl (c1 BIGINT, c2 VARCHAR)");
            assertUpdate("ALTER TABLE test_ddl ADD COLUMN c2_derived VARCHAR AS lower(c2) PERSISTENT");

            MaterializedResult result = computeActual("SHOW CREATE TABLE test_ddl");
            String ddl = (String) result.getOnlyValue();

            assertTrue(ddl.contains("\"c2_derived\" varchar AS \"lower\"(c2) PERSISTENT"));
            assertFalse(ddl.contains("presto.derived-columns.spec.json"));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_ddl");
        }
    }

    @Test
    public void testSetDefaultOnDerivedColumnFails()
    {
        try {
            assertUpdate("CREATE TABLE test_set_default (c1 BIGINT, c2 VARCHAR)");
            assertUpdate("ALTER TABLE test_set_default ADD COLUMN c2_derived VARCHAR AS lower(c2) PERSISTENT");

            // Attempt to set a DEFAULT on a derived column; should fail due to verify(columnMetadata.getDerivedColumnSpec().isEmpty(), ...)
            assertQueryFails(
                    "ALTER TABLE test_set_default ALTER COLUMN c2_derived SET DEFAULT 'X'",
                    "SET COLUMN DEFAULT is not supported on derived columns.");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_set_default");
        }
    }

    @Test
    public void testMixedCaseIdentifiersOnDerivedColumn()
    {
        try {
            assertUpdate("CREATE TABLE Test2 (Col1 BIGINT, Col2 VARCHAR, Col1_Derived decimal(13,1) AS (CAST(Col1 AS decimal) * DECIMAL '10.5') PERSISTENT)");
            assertUpdate("INSERT INTO Test2 VALUES (123, 'B', 123 * 10.5), (120, 'C', 120 * 10.5)", 2);
            assertUpdate("ALTER TABLE Test2 ADD COLUMN Col2_derived VARCHAR AS lower(Col2) PERSISTENT");
            assertTableProperties("Test2", ImmutableMap.of("col1_derived", "(CAST(Col1 AS decimal) * DECIMAL '10.5')", "col2_derived", "\"lower\"(Col2)"));
            // After renaming the derived column,
            assertUpdate("ALTER TABLE Test2 RENAME COLUMN Col1_Derived TO c1_Derived2");
            assertTableProperties("Test2", ImmutableMap.of("c1_derived2", "(CAST(Col1 AS decimal) * DECIMAL '10.5')", "col2_derived", "\"lower\"(Col2)"));
            assertUpdate("ALTER TABLE Test2 DROP COLUMN c1_Derived2");
            assertTableProperties("Test2", ImmutableMap.of("col2_derived", "\"lower\"(Col2)"));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS Test2");
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Updating property presto.derived-columns.spec.json is not supported currently.*")
    public void testSetPropertyIsDisallowedForAddDerivedColumn()
    {
        try {
            assertUpdate("CREATE TABLE Test2 (Col1 BIGINT, Col2 VARCHAR, Col1_Derived decimal(13,1) AS (CAST(Col1 AS decimal) * DECIMAL '10.5') PERSISTENT)");
            assertUpdate("INSERT INTO Test2 VALUES (123, 'B', 123 * 10.5), (120, 'C', 120 * 10.5)", 2);
            assertUpdate(format("ALTER TABLE IF EXISTS Test2 SET PROPERTIES (\"%s\" = JSON '%s')", DERIVED_COLUMN_EXPRESSION_SPEC, DERIVED_COL_EMPTY_SPEC));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS Test2");
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*property presto.derived-columns.spec.json is not user configurable.*")
    public void testSetPropertyIsDisallowedForCreateTable()
    {
        try {
            assertUpdate(format("CREATE TABLE Test2 (Col1 BIGINT, Col2 VARCHAR, Col1_Derived VARCHAR) WITH (\"%s\" = JSON '%s')", DERIVED_COLUMN_EXPRESSION_SPEC,
                    DERIVED_COLUMN_SPEC_JSON_CODEC.toJson(new DerivedColumnSpecList(ImmutableList.of(
                            new DerivedColumnSpec(DerivedColumnType.PERSISTENT, "x", "Col1_Derived", -1, "varchar"))))));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS Test2");
        }
    }

    private void assertTableProperties(String tableName, Map<String, String> tableProperties)
    {
        Session txnSession = assertStartTransaction(session, "START TRANSACTION");
        MaterializedResultWithPlan resultWithPlan = getQueryRunner().executeWithPlan(txnSession, format("select * from %s", tableName), WarningCollector.NOOP);
        TableScanNode tableScan = PlanNodeSearcher.searchFrom(resultWithPlan.getQueryPlan().getRoot()).where(planNode -> planNode instanceof TableScanNode).findOnlyElement();
        TableHandle handle = tableScan.getTable();
        ConnectorTableMetadata connectorTableMetadata = getQueryRunner().getMetadata().getTableMetadata(txnSession, handle).getMetadata();
        Map<String, ColumnMetadata> columnMetadataMap = connectorTableMetadata.getColumns().stream().collect(toImmutableMap(ColumnMetadata::getName, y -> y));

        Set<ColumnMetadata> derivedColumnMetadataSet = tableProperties.keySet().stream().map(col -> columnMetadataMap.get(normalizeIdentifier(col, ICEBERG_CATALOG)))
                .collect(toImmutableSet());
        assertEquals(derivedColumnMetadataSet.size(), tableProperties.size());
        for (ColumnMetadata derivedColumnMetadata : derivedColumnMetadataSet) {
            assertTrue(derivedColumnMetadata.getDerivedColumnSpec().isPresent(), format("ColumnMetadata of derived column should have derivedColumnSpec for %s", derivedColumnMetadata.getName()));
            DerivedColumnSpec derivedColumnSpec = derivedColumnMetadata.getDerivedColumnSpec().get();
            assertTrue(tableProperties.containsKey(derivedColumnSpec.getDerivedColumnName()), format("derived column not found in column metadata. %s", derivedColumnSpec.getDerivedColumnName()));
            assertEquals(derivedColumnSpec.getDerivedColumnExpression(), tableProperties.get(derivedColumnSpec.getDerivedColumnName()));
        }
        DerivedColumnSpecList derivedColumnSpecList = IcebergTableProperties.getDerivedColumnSpec(connectorTableMetadata.getProperties());
        assertEquals(derivedColumnSpecList.getDerivedColumnSpecs().size(), tableProperties.size());
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
        for (DerivedColumnSpec derivedColumnSpec : derivedColumnSpecList.getDerivedColumnSpecs()) {
            mapBuilder.put(derivedColumnSpec.getDerivedColumnName(), derivedColumnSpec.getDerivedColumnExpression());
        }
        ImmutableMap<String, String> actualTableProperties = mapBuilder.build();
        assertEqualsDeep(actualTableProperties, tableProperties);
        assertEndTransaction(txnSession, "commit");
    }
}
