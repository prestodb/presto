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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.TransformationSubtype;
import com.facebook.presto.spi.MaterializedViewDefinition.ColumnProvenance;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.spi.function.FunctionImplementationType.JAVA;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

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

    @Test
    public void testPartitionTransformCarriesMetadata()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT TEST_PARTITION_BUCKET(b, 'many') AS bucket, SUM(a) AS total FROM t6 GROUP BY 1");

        List<TableColumn> sources = extractor.getMaterializedViewDerivedColumnMappings().get("bucket");
        assertEquals(sources.size(), 1);
        assertEquals(sources.get(0).getColumnName(), "b");
        assertEquals(
                sources.get(0).getColumnMappingInfo(),
                ImmutableMap.of("transform", "test_partition_bucket", "mode", "many"));
    }

    @Test
    public void testOrdinaryDerivedColumnCarriesNoTransformMetadata()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT b, SUM(a) AS total FROM t6 GROUP BY b");

        List<TableColumn> columns = extractor.getMaterializedViewDerivedColumnMappings().get("total");
        assertEquals(columns.size(), 1);
        assertTrue(columns.get(0).getColumnMappingInfo().isEmpty());
    }

    @Test
    public void testMixedCaseAliasPreservesTransformMetadataAndProvenance()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT TEST_PARTITION_BUCKET(b, 'many') AS BucketValue FROM t6 GROUP BY 1");

        Map<String, List<TableColumn>> mappings = extractor.getMaterializedViewDerivedColumnMappings();
        assertEquals(mappings.keySet(), ImmutableSet.of("BucketValue"));
        assertEquals(
                mappings.get("BucketValue").get(0).getColumnMappingInfo(),
                ImmutableMap.of("transform", "test_partition_bucket", "mode", "many"));

        Map<String, ColumnProvenance> provenance = extractor.getMaterializedViewColumnProvenance();
        assertEquals(provenance.keySet(), ImmutableSet.of("BucketValue"));
        assertEquals(provenance.get("BucketValue").getFunctionName(), Optional.of("test_partition_bucket"));
        assertEquals(provenance.get("BucketValue").getReturnType(), Optional.of("varchar"));
    }

    @Test
    public void testProvenanceRecordsAggregatesWithNoLineage()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT b, COUNT(*) AS n FROM t6 GROUP BY b");

        Map<String, ColumnProvenance> provenance = extractor.getMaterializedViewColumnProvenance();

        // COUNT(*) has no source column, so it never appears in either lineage map. Recording it
        // here is what lets the next tier reject COUNT(*) as a merge partner for COUNT.
        assertFalse(extractor.getMaterializedViewDerivedColumnMappings().containsKey("n"));
        assertFalse(extractor.getMaterializedViewColumnMappings().containsKey("n"));
        assertEquals(provenance.get("n").getFunctionName(), Optional.of("count"));
        assertTrue(provenance.get("n").isAggregation());

        assertEquals(provenance.get("b").getFunctionName(), Optional.empty());
        assertEquals(provenance.get("b").getSubtype(), TransformationSubtype.IDENTITY);
    }

    @Test
    public void testProvenanceRecordsDistinct()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT COUNT(DISTINCT a) AS distinct_count, COUNT(a) AS ordinary_count FROM t6");

        Map<String, ColumnProvenance> provenance = extractor.getMaterializedViewColumnProvenance();
        assertTrue(provenance.get("distinct_count").isDistinct());
        assertFalse(provenance.get("ordinary_count").isDistinct());
    }

    @Test
    public void testSelectStarRecordsIdentityProvenance()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t1");

        Map<String, ColumnProvenance> provenance = extractor.getMaterializedViewColumnProvenance();
        assertEquals(provenance.keySet(), ImmutableSet.of("a", "b", "c", "d"));
        provenance.values().forEach(entry -> {
            assertEquals(entry.getSubtype(), TransformationSubtype.IDENTITY);
            assertEquals(entry.getFunctionName(), Optional.empty());
            assertEquals(entry.getReturnType(), Optional.of("bigint"));
            assertFalse(entry.isDistinct());
        });
    }

    @Test
    public void testCustomAggregateNamedSumIsNotRecordedAsBuiltIn()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT unittest.memory.sum(a) AS total FROM t6");

        ColumnProvenance provenance = extractor.getMaterializedViewColumnProvenance().get("total");
        assertEquals(provenance.getFunctionName(), Optional.empty());
    }

    @Test
    public void testOnlyBuiltInFunctionHandlesAreRecordedAsBuiltIn()
    {
        FunctionHandle customSum = transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(CLIENT_SESSION, session -> {
                    return metadata.getFunctionAndTypeManager().resolveFunction(
                            Optional.empty(),
                            Optional.of(session.getRequiredTransactionId()),
                            QualifiedObjectName.valueOf("unittest", "memory", "sum"),
                            fromTypes(BIGINT));
                });
        FunctionHandle builtInSum = metadata.getFunctionAndTypeManager().lookupFunction("sum", fromTypes(BIGINT));

        assertEquals(MaterializedViewColumnMappingExtractor.getCanonicalBuiltInFunctionName(customSum), Optional.empty());
        assertEquals(MaterializedViewColumnMappingExtractor.getCanonicalBuiltInFunctionName(builtInSum), Optional.of("sum"));
    }

    @Test
    public void testProvenanceDistinguishesAggregatesFromPartitionTransforms()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT TEST_PARTITION_BUCKET(b, 'many') AS bucket, SUM(a) AS total FROM t6 GROUP BY 1");

        Map<String, ColumnProvenance> provenance = extractor.getMaterializedViewColumnProvenance();

        assertEquals(provenance.get("bucket").getFunctionName(), Optional.of("test_partition_bucket"));
        assertEquals(provenance.get("bucket").getSubtype(), TransformationSubtype.TRANSFORMATION);

        assertEquals(provenance.get("total").getFunctionName(), Optional.of("sum"));
        assertTrue(provenance.get("total").isAggregation());
    }

    @Test
    public void testProvenanceIsVersioned()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT b, SUM(a) AS total FROM t6 GROUP BY b");

        Map<String, ColumnProvenance> provenance = extractor.getMaterializedViewColumnProvenance();
        assertEquals(provenance.keySet(), ImmutableSet.of("b", "total"));
        provenance.values()
                .forEach(entry -> assertEquals(entry.getVersion(), ColumnProvenance.CURRENT_VERSION));
    }

    @Test
    public void testPartitionTransformWithNonLiteralConfigurationIsRejected()
    {
        try {
            extract("CREATE MATERIALIZED VIEW mv AS SELECT TEST_PARTITION_BUCKET(b, b) AS bucket FROM t6 GROUP BY 1")
                    .getMaterializedViewDerivedColumnMappings();
            fail("expected a non-constant transform argument to be rejected");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("one source column and one constant mode"), e.getMessage());
        }
    }

    @Test
    public void testPartitionTransformOverMultipleSourceColumnsIsRejected()
    {
        try {
            extract("CREATE MATERIALIZED VIEW mv AS SELECT TEST_PARTITION_BUCKET(CONCAT(b, CAST(a AS VARCHAR)), 'many') AS bucket FROM t6 GROUP BY 1")
                    .getMaterializedViewDerivedColumnMappings();
            fail("expected a transform over multiple source columns to be rejected");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("requires one direct source column"), e.getMessage());
        }
    }

    @Test
    public void testPartitionTransformOverSingleSourceColumnIsAccepted()
    {
        Map<String, List<TableColumn>> mappings =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT TEST_PARTITION_BUCKET(t6.b, 'many') AS bucket FROM t6 GROUP BY 1")
                        .getMaterializedViewDerivedColumnMappings();
        assertEquals(mappings.get("bucket").size(), 1);
        assertEquals(
                mappings.get("bucket").get(0).getColumnMappingInfo(),
                ImmutableMap.of("transform", "test_partition_bucket", "mode", "many"));
    }

    @Test
    public void testPartitionTransformOverConstantIsRejected()
    {
        try {
            extract("CREATE MATERIALIZED VIEW mv AS SELECT TEST_PARTITION_BUCKET('source', 'many') AS bucket, SUM(a) AS total FROM t6 GROUP BY 1")
                    .getMaterializedViewDerivedColumnMappings();
            fail("expected a transform over no source column to be rejected");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("requires one source column"), e.getMessage());
        }
    }

    @Test
    public void testPartitionTransformOverWrappedSourceColumnIsRejected()
    {
        try {
            extract("CREATE MATERIALIZED VIEW mv AS SELECT TEST_PARTITION_BUCKET(CONCAT(b, b), 'many') AS bucket FROM t6 GROUP BY 1")
                    .getMaterializedViewDerivedColumnMappings();
            fail("expected a transform over a wrapped source column to be rejected");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("requires one direct source column"), e.getMessage());
        }
    }

    @Test
    public void testWrappedPartitionTransformIsRejected()
    {
        try {
            extract("CREATE MATERIALIZED VIEW mv AS SELECT CAST(TEST_PARTITION_BUCKET(b, 'many') AS VARCHAR) AS bucket FROM t6 GROUP BY 1")
                    .getMaterializedViewDerivedColumnMappings();
            fail("expected a wrapped partition transform to be rejected");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("must be selected directly"), e.getMessage());
        }
    }

    @Test
    public void testPartitionTransformWithUnsupportedConfigurationIsRejected()
    {
        try {
            extract("CREATE MATERIALIZED VIEW mv AS SELECT TEST_PARTITION_BUCKET(b, 'unsupported') AS bucket FROM t6 GROUP BY 1")
                    .getMaterializedViewDerivedColumnMappings();
            fail("expected unsupported transform configuration to be rejected");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("Unsupported test partition transform mode"), e.getMessage());
        }
    }

    @Test
    public void testInvalidPartitionTransformProviderIsRejectedAsSemanticError()
    {
        MaterializedViewColumnMappingExtractor extractor =
                extract("CREATE MATERIALIZED VIEW mv AS SELECT TEST_PARTITION_BUCKET(b, 'many') AS bucket FROM t6 GROUP BY 1");
        FunctionCall functionCall = new FunctionCall(QualifiedName.of(TestingMaterializedViewPartitionTransformProvider.FUNCTION_NAME), ImmutableList.of());
        FunctionMetadata nondeterministicFunction = new FunctionMetadata(
                QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, TestingMaterializedViewPartitionTransformProvider.FUNCTION_NAME),
                ImmutableList.of(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()),
                VARCHAR.getTypeSignature(),
                SCALAR,
                JAVA,
                false,
                false);

        SemanticException exception = expectThrows(
                SemanticException.class,
                () -> extractor.getPartitionTransformProvider(functionCall, nondeterministicFunction));
        assertTrue(exception.getMessage().contains("must be deterministic"), exception.getMessage());
    }

    @Test
    public void testPartitionTransformInSetOperationIsRejected()
    {
        try {
            extract("CREATE MATERIALIZED VIEW mv AS " +
                    "SELECT TEST_PARTITION_BUCKET(b, 'many') AS bucket FROM t6 " +
                    "UNION ALL " +
                    "SELECT TEST_PARTITION_BUCKET(CAST(b AS VARCHAR), 'many') AS bucket FROM t1")
                    .getMaterializedViewDerivedColumnMappings();
            fail("expected a partition transform in a set operation to be rejected");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("Partition transforms are not supported in set-operation materialized views"), e.getMessage());
        }
    }
}
