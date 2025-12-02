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
package com.facebook.presto.sql.planner.iterative.rule.materializedview;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewDefinition.ColumnMapping;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPassthroughColumnEquivalences
{
    private static final SchemaTableName ORDERS_TABLE = new SchemaTableName("catalog", "orders");
    private static final SchemaTableName CUSTOMER_TABLE = new SchemaTableName("catalog", "customer");
    private static final SchemaTableName MV_DATA_TABLE = new SchemaTableName("catalog", "__mv_storage__test_mv");

    private Metadata metadata;
    private RowExpressionDomainTranslator translator;

    @BeforeClass
    public void setUp()
    {
        metadata = MetadataManager.createTestMetadataManager();
        translator = new RowExpressionDomainTranslator(metadata);
    }

    @Test
    public void testBasicEquivalence()
    {
        // Single column mapping: orders.orderdate -> mv.orderdate
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        // Both columns should have equivalence
        assertTrue(equivalences.hasEquivalence(new TableColumn(MV_DATA_TABLE, "orderdate")));
        assertTrue(equivalences.hasEquivalence(new TableColumn(ORDERS_TABLE, "orderdate")));

        // Non-mapped column should not have equivalence
        assertFalse(equivalences.hasEquivalence(new TableColumn(ORDERS_TABLE, "orderkey")));
        assertFalse(equivalences.hasEquivalence(new TableColumn(MV_DATA_TABLE, "orderkey")));
    }

    @Test
    public void testMultipleColumnMappings()
    {
        // Multiple column mappings
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true),
                        createColumnMapping("orderstatus", ORDERS_TABLE, "orderstatus", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        // Both mappings should create equivalences
        assertTrue(equivalences.hasEquivalence(new TableColumn(MV_DATA_TABLE, "orderdate")));
        assertTrue(equivalences.hasEquivalence(new TableColumn(ORDERS_TABLE, "orderdate")));
        assertTrue(equivalences.hasEquivalence(new TableColumn(MV_DATA_TABLE, "orderstatus")));
        assertTrue(equivalences.hasEquivalence(new TableColumn(ORDERS_TABLE, "orderstatus")));
    }

    @Test
    public void testDirectMappedFilterTrue()
    {
        // Direct mapped column should be in equivalence class
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        assertTrue(equivalences.hasEquivalence(new TableColumn(ORDERS_TABLE, "orderdate")));
    }

    @Test
    public void testDirectMappedFilterFalse()
    {
        // Non-direct mapped column should NOT be in equivalence class
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", false)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        // MV column exists but base table column is not direct mapped
        // Since there's only one column in the equivalence class, no equivalence is created
        assertFalse(equivalences.hasEquivalence(new TableColumn(ORDERS_TABLE, "orderdate")));
        assertFalse(equivalences.hasEquivalence(new TableColumn(MV_DATA_TABLE, "orderdate")));
    }

    @Test
    public void testJoinEquivalences()
    {
        // Join condition: orders.dt = customer.dt both map to mv.dt
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE, CUSTOMER_TABLE),
                ImmutableList.of(
                        createColumnMappingWithMultipleSources("dt",
                                ImmutableList.of(
                                        new TableColumn(ORDERS_TABLE, "dt", Optional.of(true)),
                                        new TableColumn(CUSTOMER_TABLE, "dt", Optional.of(true))))));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        // All three columns should be equivalent
        assertTrue(equivalences.hasEquivalence(new TableColumn(MV_DATA_TABLE, "dt")));
        assertTrue(equivalences.hasEquivalence(new TableColumn(ORDERS_TABLE, "dt")));
        assertTrue(equivalences.hasEquivalence(new TableColumn(CUSTOMER_TABLE, "dt")));
    }

    @Test
    public void testGetEquivalentPredicatesFromBaseTable()
    {
        // Predicate on base table should translate to MV data table
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        TupleDomain<String> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of("orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))));

        Map<SchemaTableName, TupleDomain<String>> result = equivalences.getEquivalentPredicates(ORDERS_TABLE, predicate);

        // Should translate to MV data table
        assertEquals(result.size(), 1);
        assertTrue(result.containsKey(MV_DATA_TABLE));
        assertTrue(result.get(MV_DATA_TABLE).getDomains().isPresent());
        assertTrue(result.get(MV_DATA_TABLE).getDomains().get().containsKey("orderdate"));
    }

    @Test
    public void testGetEquivalentPredicatesFromMvDataTable()
    {
        // Predicate on MV data table should translate to base tables
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        TupleDomain<String> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of("orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))));

        Map<SchemaTableName, TupleDomain<String>> result = equivalences.getEquivalentPredicates(MV_DATA_TABLE, predicate);

        // Should translate to base table
        assertEquals(result.size(), 1);
        assertTrue(result.containsKey(ORDERS_TABLE));
        assertTrue(result.get(ORDERS_TABLE).getDomains().isPresent());
        assertTrue(result.get(ORDERS_TABLE).getDomains().get().containsKey("orderdate"));
    }

    @Test
    public void testGetEquivalentPredicatesWithJoin()
    {
        // Join equivalence: predicate on orders.dt should translate to both mv.dt and customer.dt
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE, CUSTOMER_TABLE),
                ImmutableList.of(
                        createColumnMappingWithMultipleSources("dt",
                                ImmutableList.of(
                                        new TableColumn(ORDERS_TABLE, "dt", Optional.of(true)),
                                        new TableColumn(CUSTOMER_TABLE, "dt", Optional.of(true))))));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        TupleDomain<String> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of("dt", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01"))));

        Map<SchemaTableName, TupleDomain<String>> result = equivalences.getEquivalentPredicates(ORDERS_TABLE, predicate);

        // Should translate to both MV data table and customer table
        assertEquals(result.size(), 2);
        assertTrue(result.containsKey(MV_DATA_TABLE));
        assertTrue(result.containsKey(CUSTOMER_TABLE));
    }

    @Test
    public void testGetEquivalentPredicatesColumnWithoutEquivalence()
    {
        // Column without equivalence should be skipped
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        // Predicate on non-mapped column
        TupleDomain<String> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of("orderkey", Domain.singleValue(BIGINT, 123L)));

        Map<SchemaTableName, TupleDomain<String>> result = equivalences.getEquivalentPredicates(ORDERS_TABLE, predicate);

        // No equivalents found
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetEquivalentPredicatesEmptyDomains()
    {
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        // Predicate with no domains
        TupleDomain<String> predicate = TupleDomain.none();

        Map<SchemaTableName, TupleDomain<String>> result = equivalences.getEquivalentPredicates(ORDERS_TABLE, predicate);

        assertTrue(result.isEmpty());
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Unknown table.*")
    public void testGetEquivalentPredicatesUnknownTable()
    {
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        SchemaTableName unknownTable = new SchemaTableName("catalog", "unknown_table");
        TupleDomain<String> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of("col", Domain.singleValue(VARCHAR, utf8Slice("value"))));

        equivalences.getEquivalentPredicates(unknownTable, predicate);
    }

    @Test
    public void testTranslatePredicatesToVariablesEmpty()
    {
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        List<RowExpression> result = equivalences.translatePredicatesToVariables(
                ORDERS_TABLE,
                ImmutableList.of(),
                ImmutableMap.of(),
                translator);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testTranslatePredicatesToVariables()
    {
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        // Create variable mapping for MV data table column
        VariableReferenceExpression orderdateVar = new VariableReferenceExpression(Optional.empty(), "orderdate", VARCHAR);
        Map<TableColumn, VariableReferenceExpression> columnToVariable = ImmutableMap.of(
                new TableColumn(MV_DATA_TABLE, "orderdate"), orderdateVar);

        List<TupleDomain<String>> stalePredicates = ImmutableList.of(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of("orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01")))));

        List<RowExpression> result = equivalences.translatePredicatesToVariables(
                ORDERS_TABLE,
                stalePredicates,
                columnToVariable,
                translator);

        assertNotNull(result);
        assertEquals(result.size(), 1);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "Cannot map stale predicates.*")
    public void testTranslatePredicatesToVariablesNoMapping()
    {
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        // No variable mapping provided - should throw
        List<TupleDomain<String>> stalePredicates = ImmutableList.of(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of("orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01")))));

        equivalences.translatePredicatesToVariables(
                ORDERS_TABLE,
                stalePredicates,
                ImmutableMap.of(),  // Empty mapping
                translator);
    }

    @Test
    public void testTranslatePredicatesToVariablesMultiplePredicates()
    {
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMapping("orderdate", ORDERS_TABLE, "orderdate", true)));

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        VariableReferenceExpression orderdateVar = new VariableReferenceExpression(Optional.empty(), "orderdate", VARCHAR);
        Map<TableColumn, VariableReferenceExpression> columnToVariable = ImmutableMap.of(
                new TableColumn(MV_DATA_TABLE, "orderdate"), orderdateVar);

        // Multiple stale predicates (disjuncts)
        List<TupleDomain<String>> stalePredicates = ImmutableList.of(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of("orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-01")))),
                TupleDomain.withColumnDomains(
                        ImmutableMap.of("orderdate", Domain.singleValue(VARCHAR, utf8Slice("2024-01-02")))));

        List<RowExpression> result = equivalences.translatePredicatesToVariables(
                ORDERS_TABLE,
                stalePredicates,
                columnToVariable,
                translator);

        // Should produce one expression per predicate
        assertEquals(result.size(), 2);
    }

    @Test
    public void testDirectMappedDefaultsToTrue()
    {
        // When isDirectMapped is not specified (empty Optional), it defaults to true
        MaterializedViewDefinition mvDefinition = createMvDefinition(
                ImmutableList.of(ORDERS_TABLE),
                ImmutableList.of(
                        createColumnMappingWithMultipleSources("orderdate",
                                ImmutableList.of(
                                        new TableColumn(ORDERS_TABLE, "orderdate", Optional.empty())))));  // No isDirectMapped specified

        PassthroughColumnEquivalences equivalences = new PassthroughColumnEquivalences(mvDefinition, MV_DATA_TABLE);

        // Should default to direct mapped = true
        assertTrue(equivalences.hasEquivalence(new TableColumn(ORDERS_TABLE, "orderdate")));
        assertTrue(equivalences.hasEquivalence(new TableColumn(MV_DATA_TABLE, "orderdate")));
    }

    // Helper methods

    private MaterializedViewDefinition createMvDefinition(
            List<SchemaTableName> baseTables,
            List<ColumnMapping> columnMappings)
    {
        return new MaterializedViewDefinition(
                "SELECT * FROM test",
                MV_DATA_TABLE.getSchemaName(),
                MV_DATA_TABLE.getTableName(),
                baseTables,
                Optional.empty(),
                Optional.empty(),
                columnMappings,
                ImmutableList.of(),
                Optional.empty());
    }

    private ColumnMapping createColumnMapping(
            String viewColumnName,
            SchemaTableName baseTable,
            String baseColumnName,
            boolean isDirectMapped)
    {
        return new ColumnMapping(
                new TableColumn(MV_DATA_TABLE, viewColumnName),
                ImmutableList.of(new TableColumn(baseTable, baseColumnName, Optional.of(isDirectMapped))));
    }

    private ColumnMapping createColumnMappingWithMultipleSources(
            String viewColumnName,
            List<TableColumn> baseColumns)
    {
        return new ColumnMapping(
                new TableColumn(MV_DATA_TABLE, viewColumnName),
                baseColumns);
    }
}
