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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewDefinition.ColumnMapping;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.optimizations.MaterializedViewStitchingUtils.ColumnEquivalences;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.sql.planner.optimizations.MaterializedViewStitchingUtils.canPerformPartitionLevelRefresh;
import static com.facebook.presto.sql.planner.optimizations.MaterializedViewStitchingUtils.filterPredicatesToMappedColumns;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMaterializedViewStitchingUtils
{
    private FunctionAndTypeManager functionAndTypeManager;
    private RowExpressionDeterminismEvaluator determinismEvaluator;

    @BeforeClass
    public void setUp()
    {
        functionAndTypeManager = createTestMetadataManager().getFunctionAndTypeManager();
        determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    @Test
    public void testCanPerformPartitionLevelRefreshWithEmptyConstraints()
    {
        PlanNode plan = createSimpleValuesNode();
        Map<SchemaTableName, MaterializedDataPredicates> constraints = ImmutableMap.of();

        assertFalse(canPerformPartitionLevelRefresh(plan, constraints, determinismEvaluator, noLookup()));
    }

    @Test
    public void testCanPerformPartitionLevelRefreshWithInnerJoin()
    {
        PlanNode plan = createJoinNode(INNER);
        Map<SchemaTableName, MaterializedDataPredicates> constraints = createNonEmptyConstraints();

        assertTrue(canPerformPartitionLevelRefresh(plan, constraints, determinismEvaluator, noLookup()));
    }

    @Test
    public void testCanPerformPartitionLevelRefreshReturnsFalseForLeftJoin()
    {
        PlanNode plan = createJoinNode(LEFT);
        Map<SchemaTableName, MaterializedDataPredicates> constraints = createNonEmptyConstraints();

        assertFalse(canPerformPartitionLevelRefresh(plan, constraints, determinismEvaluator, noLookup()));
    }

    @Test
    public void testCanPerformPartitionLevelRefreshReturnsFalseForRightJoin()
    {
        PlanNode plan = createJoinNode(RIGHT);
        Map<SchemaTableName, MaterializedDataPredicates> constraints = createNonEmptyConstraints();

        assertFalse(canPerformPartitionLevelRefresh(plan, constraints, determinismEvaluator, noLookup()));
    }

    @Test
    public void testCanPerformPartitionLevelRefreshReturnsFalseForFullJoin()
    {
        PlanNode plan = createJoinNode(FULL);
        Map<SchemaTableName, MaterializedDataPredicates> constraints = createNonEmptyConstraints();

        assertFalse(canPerformPartitionLevelRefresh(plan, constraints, determinismEvaluator, noLookup()));
    }

    @Test
    public void testCanPerformPartitionLevelRefreshReturnsFalseForNonDeterministicFunction()
    {
        PlanNode plan = createPlanWithNonDeterministicFunction();
        Map<SchemaTableName, MaterializedDataPredicates> constraints = createNonEmptyConstraints();

        assertFalse(canPerformPartitionLevelRefresh(plan, constraints, determinismEvaluator, noLookup()));
    }

    @Test
    public void testCanPerformPartitionLevelRefreshReturnsTrueForDeterministicPlan()
    {
        PlanNode plan = createPlanWithDeterministicFunction();
        Map<SchemaTableName, MaterializedDataPredicates> constraints = createNonEmptyConstraints();

        assertTrue(canPerformPartitionLevelRefresh(plan, constraints, determinismEvaluator, noLookup()));
    }

    @Test
    public void testColumnEquivalencesSimplePassthrough()
    {
        SchemaTableName baseTable = new SchemaTableName("schema", "base_table");
        SchemaTableName dataTable = new SchemaTableName("schema", "mv_data");

        MaterializedViewDefinition mvDef = createMaterializedViewDefinition(
                dataTable,
                ImmutableList.of(baseTable),
                ImmutableList.of(
                        createColumnMapping("dt", baseTable, "dt", true)));

        ColumnEquivalences columnEquivalences = new ColumnEquivalences(mvDef, dataTable);

        assertTrue(columnEquivalences.hasEquivalence(new TableColumn(baseTable, "dt")));
        assertTrue(columnEquivalences.hasEquivalence(new TableColumn(dataTable, "dt")));
        assertFalse(columnEquivalences.hasEquivalence(new TableColumn(baseTable, "other_column")));
    }

    @Test
    public void testColumnEquivalencesWithJoinEquality()
    {
        SchemaTableName tableA = new SchemaTableName("schema", "table_a");
        SchemaTableName tableB = new SchemaTableName("schema", "table_b");
        SchemaTableName dataTable = new SchemaTableName("schema", "mv_data");

        MaterializedViewDefinition mvDef = createMaterializedViewDefinition(
                dataTable,
                ImmutableList.of(tableA, tableB),
                ImmutableList.of(
                        createColumnMapping("dt", ImmutableList.of(
                                new TableColumn(tableA, "dt", Optional.of(true)),
                                new TableColumn(tableB, "dt", Optional.of(true))))));

        ColumnEquivalences columnEquivalences = new ColumnEquivalences(mvDef, dataTable);

        assertTrue(columnEquivalences.hasEquivalence(new TableColumn(tableA, "dt")));
        assertTrue(columnEquivalences.hasEquivalence(new TableColumn(tableB, "dt")));
        assertTrue(columnEquivalences.hasEquivalence(new TableColumn(dataTable, "dt")));
    }

    @Test
    public void testColumnEquivalencesGetEquivalentPredicates()
    {
        SchemaTableName tableA = new SchemaTableName("schema", "table_a");
        SchemaTableName tableB = new SchemaTableName("schema", "table_b");
        SchemaTableName dataTable = new SchemaTableName("schema", "mv_data");

        MaterializedViewDefinition mvDef = createMaterializedViewDefinition(
                dataTable,
                ImmutableList.of(tableA, tableB),
                ImmutableList.of(
                        createColumnMapping("dt", ImmutableList.of(
                                new TableColumn(tableA, "dt", Optional.of(true)),
                                new TableColumn(tableB, "dt", Optional.of(true))))));

        ColumnEquivalences columnEquivalences = new ColumnEquivalences(mvDef, dataTable);

        TupleDomain<String> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of("dt", Domain.singleValue(VARCHAR, io.airlift.slice.Slices.utf8Slice("2024-01-01"))));

        Map<SchemaTableName, TupleDomain<String>> equivalents = columnEquivalences.getEquivalentPredicates(tableA, predicate);

        assertTrue(equivalents.containsKey(tableB));
        assertTrue(equivalents.containsKey(dataTable));
        assertFalse(equivalents.containsKey(tableA));
    }

    @Test
    public void testColumnEquivalencesNonDirectMappedColumnsExcluded()
    {
        SchemaTableName baseTable = new SchemaTableName("schema", "base_table");
        SchemaTableName dataTable = new SchemaTableName("schema", "mv_data");

        MaterializedViewDefinition mvDef = createMaterializedViewDefinition(
                dataTable,
                ImmutableList.of(baseTable),
                ImmutableList.of(
                        createColumnMapping("computed", baseTable, "source", false)));

        ColumnEquivalences columnEquivalences = new ColumnEquivalences(mvDef, dataTable);

        assertFalse(columnEquivalences.hasEquivalence(new TableColumn(baseTable, "source")));
    }

    @Test
    public void testFilterPredicatesToMappedColumnsWithEmptyConstraints()
    {
        ColumnEquivalences columnEquivalences = createSimpleColumnEquivalences();
        Map<SchemaTableName, MaterializedDataPredicates> constraints = ImmutableMap.of();

        Map<SchemaTableName, List<TupleDomain<String>>> result = filterPredicatesToMappedColumns(constraints, columnEquivalences);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testFilterPredicatesToMappedColumnsWithMappedColumn()
    {
        SchemaTableName table = new SchemaTableName("schema", "base_table");
        SchemaTableName dataTable = new SchemaTableName("schema", "mv_data");

        MaterializedViewDefinition mvDef = createMaterializedViewDefinition(
                dataTable,
                ImmutableList.of(table),
                ImmutableList.of(
                        createColumnMapping("dt", table, "dt", true)));

        ColumnEquivalences columnEquivalences = new ColumnEquivalences(mvDef, dataTable);

        List<TupleDomain<String>> predicates = ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "dt", Domain.singleValue(VARCHAR, io.airlift.slice.Slices.utf8Slice("2024-01-01")))));

        Map<SchemaTableName, MaterializedDataPredicates> constraints = ImmutableMap.of(
                table, new MaterializedDataPredicates(predicates, ImmutableList.of("dt")));

        Map<SchemaTableName, List<TupleDomain<String>>> result = filterPredicatesToMappedColumns(constraints, columnEquivalences);

        assertEquals(result.get(table).size(), 1);
        assertTrue(result.get(table).get(0).getDomains().get().containsKey("dt"));
    }

    @Test
    public void testFilterPredicatesToMappedColumnsWithUnmappedColumn()
    {
        SchemaTableName table = new SchemaTableName("schema", "base_table");
        SchemaTableName dataTable = new SchemaTableName("schema", "mv_data");

        MaterializedViewDefinition mvDef = createMaterializedViewDefinition(
                dataTable,
                ImmutableList.of(table),
                ImmutableList.of(
                        createColumnMapping("dt", table, "dt", true)));

        ColumnEquivalences columnEquivalences = new ColumnEquivalences(mvDef, dataTable);

        List<TupleDomain<String>> predicates = ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "other_column", Domain.singleValue(VARCHAR, io.airlift.slice.Slices.utf8Slice("value")))));

        Map<SchemaTableName, MaterializedDataPredicates> constraints = ImmutableMap.of(
                table, new MaterializedDataPredicates(predicates, ImmutableList.of("other_column")));

        Map<SchemaTableName, List<TupleDomain<String>>> result = filterPredicatesToMappedColumns(constraints, columnEquivalences);

        assertTrue(result.get(table).isEmpty());
    }

    @Test
    public void testFilterPredicatesToMappedColumnsWithMixedColumns()
    {
        SchemaTableName table = new SchemaTableName("schema", "base_table");
        SchemaTableName dataTable = new SchemaTableName("schema", "mv_data");

        MaterializedViewDefinition mvDef = createMaterializedViewDefinition(
                dataTable,
                ImmutableList.of(table),
                ImmutableList.of(
                        createColumnMapping("dt", table, "dt", true)));

        ColumnEquivalences columnEquivalences = new ColumnEquivalences(mvDef, dataTable);

        List<TupleDomain<String>> predicates = ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "dt", Domain.singleValue(VARCHAR, io.airlift.slice.Slices.utf8Slice("2024-01-01")),
                        "unmapped", Domain.singleValue(BIGINT, 100L))));

        Map<SchemaTableName, MaterializedDataPredicates> constraints = ImmutableMap.of(
                table, new MaterializedDataPredicates(predicates, ImmutableList.of("dt", "unmapped")));

        Map<SchemaTableName, List<TupleDomain<String>>> result = filterPredicatesToMappedColumns(constraints, columnEquivalences);

        assertEquals(result.get(table).size(), 1);
        assertTrue(result.get(table).get(0).getDomains().get().containsKey("dt"));
        assertFalse(result.get(table).get(0).getDomains().get().containsKey("unmapped"));
    }

    @Test
    public void testFilterPredicatesToMappedColumnsWithMultiplePredicates()
    {
        SchemaTableName table = new SchemaTableName("schema", "base_table");
        SchemaTableName dataTable = new SchemaTableName("schema", "mv_data");

        MaterializedViewDefinition mvDef = createMaterializedViewDefinition(
                dataTable,
                ImmutableList.of(table),
                ImmutableList.of(
                        createColumnMapping("dt", table, "dt", true)));

        ColumnEquivalences columnEquivalences = new ColumnEquivalences(mvDef, dataTable);

        List<TupleDomain<String>> predicates = ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "dt", Domain.singleValue(VARCHAR, io.airlift.slice.Slices.utf8Slice("2024-01-01")))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "dt", Domain.singleValue(VARCHAR, io.airlift.slice.Slices.utf8Slice("2024-01-02")))));

        Map<SchemaTableName, MaterializedDataPredicates> constraints = ImmutableMap.of(
                table, new MaterializedDataPredicates(predicates, ImmutableList.of("dt")));

        Map<SchemaTableName, List<TupleDomain<String>>> result = filterPredicatesToMappedColumns(constraints, columnEquivalences);

        assertEquals(result.get(table).size(), 2);
    }

    @Test
    public void testFilterPredicatesToMappedColumnsWithMultipleTables()
    {
        SchemaTableName tableA = new SchemaTableName("schema", "table_a");
        SchemaTableName tableB = new SchemaTableName("schema", "table_b");
        SchemaTableName dataTable = new SchemaTableName("schema", "mv_data");

        MaterializedViewDefinition mvDef = createMaterializedViewDefinition(
                dataTable,
                ImmutableList.of(tableA, tableB),
                ImmutableList.of(
                        createColumnMapping("dt", ImmutableList.of(
                                new TableColumn(tableA, "dt", Optional.of(true)),
                                new TableColumn(tableB, "dt", Optional.of(true))))));

        ColumnEquivalences columnEquivalences = new ColumnEquivalences(mvDef, dataTable);

        Map<SchemaTableName, MaterializedDataPredicates> constraints = ImmutableMap.of(
                tableA, new MaterializedDataPredicates(
                        ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                                "dt", Domain.singleValue(VARCHAR, io.airlift.slice.Slices.utf8Slice("2024-01-01"))))),
                        ImmutableList.of("dt")),
                tableB, new MaterializedDataPredicates(
                        ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                                "dt", Domain.singleValue(VARCHAR, io.airlift.slice.Slices.utf8Slice("2024-01-02"))))),
                        ImmutableList.of("dt")));

        Map<SchemaTableName, List<TupleDomain<String>>> result = filterPredicatesToMappedColumns(constraints, columnEquivalences);

        assertEquals(result.size(), 2);
        assertEquals(result.get(tableA).size(), 1);
        assertEquals(result.get(tableB).size(), 1);
    }

    // ===========================================
    // Helper methods
    // ===========================================

    private PlanNode createSimpleValuesNode()
    {
        return new ValuesNode(
                Optional.empty(),
                new PlanNodeId("values"),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());
    }

    private PlanNode createJoinNode(JoinType joinType)
    {
        ValuesNode left = new ValuesNode(
                Optional.empty(),
                new PlanNodeId("left"),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());

        ValuesNode right = new ValuesNode(
                Optional.empty(),
                new PlanNodeId("right"),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());

        return new JoinNode(
                Optional.empty(),
                new PlanNodeId("join"),
                joinType,
                left,
                right,
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    private PlanNode createPlanWithNonDeterministicFunction()
    {
        CallExpression randomCall = new CallExpression(
                "random",
                functionAndTypeManager.lookupFunction("random", fromTypes(BIGINT)),
                BIGINT,
                singletonList(constant(10L, BIGINT)));

        ValuesNode source = new ValuesNode(
                Optional.empty(),
                new PlanNodeId("values"),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());

        return new FilterNode(
                Optional.empty(),
                new PlanNodeId("filter"),
                source,
                randomCall);
    }

    private PlanNode createPlanWithDeterministicFunction()
    {
        ValuesNode source = new ValuesNode(
                Optional.empty(),
                new PlanNodeId("values"),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());

        RowExpression deterministicExpression = constant(true, BOOLEAN);

        return new FilterNode(
                Optional.empty(),
                new PlanNodeId("filter"),
                source,
                deterministicExpression);
    }

    private Map<SchemaTableName, MaterializedDataPredicates> createNonEmptyConstraints()
    {
        return ImmutableMap.of(
                new SchemaTableName("schema", "table"),
                new MaterializedDataPredicates(
                        ImmutableList.of(TupleDomain.all()),
                        ImmutableList.of("dt")));
    }

    private MaterializedViewDefinition createMaterializedViewDefinition(
            SchemaTableName dataTable,
            List<SchemaTableName> baseTables,
            List<ColumnMapping> columnMappings)
    {
        return new MaterializedViewDefinition(
                "SELECT * FROM base_table",
                dataTable.getSchemaName(),
                dataTable.getTableName(),
                baseTables,
                Optional.of("owner"),
                Optional.empty(),
                columnMappings,
                ImmutableList.of(),
                Optional.empty());
    }

    private ColumnMapping createColumnMapping(String viewColumn, SchemaTableName baseTable, String baseColumn, boolean directMapped)
    {
        return new ColumnMapping(
                new TableColumn(new SchemaTableName("schema", "mv"), viewColumn),
                ImmutableList.of(new TableColumn(baseTable, baseColumn, Optional.of(directMapped))));
    }

    private ColumnMapping createColumnMapping(String viewColumn, List<TableColumn> baseColumns)
    {
        return new ColumnMapping(
                new TableColumn(new SchemaTableName("schema", "mv"), viewColumn),
                baseColumns);
    }

    private ColumnEquivalences createSimpleColumnEquivalences()
    {
        SchemaTableName baseTable = new SchemaTableName("schema", "base_table");
        SchemaTableName dataTable = new SchemaTableName("schema", "mv_data");

        MaterializedViewDefinition mvDef = createMaterializedViewDefinition(
                dataTable,
                ImmutableList.of(baseTable),
                ImmutableList.of(
                        createColumnMapping("dt", baseTable, "dt", true)));

        return new ColumnEquivalences(mvDef, dataTable);
    }
}
