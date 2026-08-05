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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMaterializedViewColumnMappingExtractor
        extends AbstractAnalyzerTest
{
    private static final SchemaTableName T1 = new SchemaTableName("s1", "t1");

    private MaterializedViewColumnMappingExtractor extract(@Language("SQL") String sql)
    {
        return transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(CLIENT_SESSION, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata, WarningCollector.NOOP, Optional.empty(), sql);
                    CreateMaterializedView statement = (CreateMaterializedView) SQL_PARSER.createStatement(sql);
                    Analysis analysis = analyzer.analyzeSemantic(statement, false);
                    return new MaterializedViewColumnMappingExtractor(analysis, session, metadata);
                });
    }

    private static Set<String> baseColumnNames(List<TableColumn> columns)
    {
        return columns.stream().map(TableColumn::getColumnName).collect(Collectors.toSet());
    }

    private static Set<String> baseColumns(List<TableColumn> columns)
    {
        return columns.stream()
                .map(c -> c.getTableName().getTableName() + "." + c.getColumnName())
                .collect(Collectors.toSet());
    }

    @Test
    public void testDerivedColumnMapsToAllValueCarryingSources()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT a + b AS sum_ab, a AS plain_a FROM t1");

        Map<String, List<TableColumn>> derived = extractor.getMaterializedViewDerivedColumnMappings();

        // a + b -> {t1.a, t1.b}; both from the same base table.
        assertTrue(derived.containsKey("sum_ab"));
        assertEquals(baseColumnNames(derived.get("sum_ab")), ImmutableSet.of("a", "b"));
        derived.get("sum_ab").forEach(col -> {
            assertEquals(col.getTableName(), T1);
            assertFalse(col.isDirectMapped().orElse(true));
        });

        // The plain 1:1 column stays in the origin-based mapping, not the derived one.
        assertFalse(derived.containsKey("plain_a"));
        assertTrue(extractor.getMaterializedViewColumnMappings().containsKey("plain_a"));
        assertFalse(extractor.getMaterializedViewColumnMappings().containsKey("sum_ab"));
    }

    @Test
    public void testAggregatedColumnMapsToSource()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT b, SUM(a) AS total FROM t1 GROUP BY b");

        Map<String, List<TableColumn>> derived = extractor.getMaterializedViewDerivedColumnMappings();

        assertEquals(baseColumnNames(derived.get("total")), ImmutableSet.of("a"));
        // GROUP BY key b is a 1:1 column, not derived.
        assertFalse(derived.containsKey("b"));
    }

    @Test
    public void testConstantColumnHasNoLineage()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT a, 2021 AS c0 FROM t1");

        // A constant column carries no base-table lineage (create-time warning case).
        assertFalse(extractor.getMaterializedViewDerivedColumnMappings().containsKey("c0"));
        assertFalse(extractor.getMaterializedViewColumnMappings().containsKey("c0"));
        assertTrue(extractor.getMaterializedViewColumnMappings().containsKey("a"));
    }

    @Test
    public void testFilterOnlyColumnIsNotPropagated()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT a + 1 AS a1 FROM t1 WHERE b > 0");

        Map<String, List<TableColumn>> derived = extractor.getMaterializedViewDerivedColumnMappings();

        // Only value-carrying (DIRECT) sources propagate; b appears only in WHERE (INDIRECT).
        assertEquals(baseColumnNames(derived.get("a1")), ImmutableSet.of("a"));
    }

    @Test
    public void testUnnestColumnMapsToSourceCollection()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT a AS ka, x AS xx FROM t7 CROSS JOIN UNNEST(c) AS u(x)");

        // The unnested column is derived from its source collection column.
        assertEquals(baseColumns(extractor.getMaterializedViewDerivedColumnMappings().get("xx")), ImmutableSet.of("t7.c"));
        assertTrue(extractor.getMaterializedViewColumnMappings().containsKey("ka"));
    }

    @Test
    public void testUnionDerivedColumnMapsToAllBranches()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT a + b AS s FROM t1 UNION ALL SELECT a + b AS s FROM t2");

        Map<String, List<TableColumn>> derived = extractor.getMaterializedViewDerivedColumnMappings();

        // Derived UNION column should carry lineage from BOTH branches.
        assertEquals(baseColumns(derived.get("s")), ImmutableSet.of("t1.a", "t1.b", "t2.a", "t2.b"));
    }

    @Test
    public void testOuterJoinDerivedColumnMapsToReferencedColumn()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT t1.a AS ka, t2.b + 1 AS x FROM t1 LEFT JOIN t2 ON t1.a = t2.a");

        Map<String, List<TableColumn>> derived = extractor.getMaterializedViewDerivedColumnMappings();

        // Derived column over the outer (nullable) side maps to the referenced column.
        assertEquals(baseColumns(derived.get("x")), ImmutableSet.of("t2.b"));
    }

    @Test
    public void testOuterJoinMappedAndDerivedAreDisjointAndCoverLineageColumns()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT t1.a AS ka, t2.b AS kb, t2.b + 1 AS x, 42 AS c FROM t1 LEFT JOIN t2 ON t1.a = t2.a");

        // Nullable-side 1:1 columns stay in the 1:1 map, the expression is derived, and the
        // constant is in neither, so mapped + derived < output columns (the no-lineage case).
        assertEquals(extractor.getMaterializedViewColumnMappings().keySet(), ImmutableSet.of("ka", "kb"));
        assertEquals(extractor.getMaterializedViewDerivedColumnMappings().keySet(), ImmutableSet.of("x"));
    }
}
