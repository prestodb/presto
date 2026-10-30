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

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.TransformationSubtype;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewDefinition.ColumnProvenance;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.SystemSessionProperties.MATERIALIZED_VIEW_ON_MATERIALIZED_VIEW_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.spi.function.FunctionImplementationType.JAVA;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
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

    @BeforeClass
    public void setupMaterializedViewSourceChain()
    {
        SchemaTableName source = T1;
        for (int depth = 1; depth <= 4; depth++) {
            SchemaTableName target = new SchemaTableName("s1", "mv_source_depth_" + depth);
            MaterializedViewDefinition definition = new MaterializedViewDefinition(
                    "SELECT 1 AS a FROM " + source.getTableName(),
                    target.getSchemaName(),
                    target.getTableName(),
                    ImmutableList.of(source),
                    Optional.of(ImmutableList.of(TPCH_CATALOG)),
                    Optional.of("user"),
                    Optional.empty(),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            ConnectorTableMetadata viewMetadata = new ConnectorTableMetadata(
                    target,
                    ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));
            runInSetupTransaction(session -> metadata.createMaterializedView(session, TPCH_CATALOG, viewMetadata, definition, false));
            source = target;
        }
    }

    @BeforeClass
    public void setupMaterializedViewSourcesForMergeChecks()
    {
        createMaterializedViewSource(
                new SchemaTableName("s1", "mv_without_provenance"),
                ImmutableList.of(column("b", VARCHAR), column("total", BIGINT)),
                ImmutableMap.of());

        createMaterializedViewSource(
                THIRD_CATALOG,
                new SchemaTableName("s1", "mv_without_connector_capability"),
                ImmutableList.of(column("b", VARCHAR), column("total", BIGINT)),
                ImmutableMap.of(
                        "b", new ColumnProvenance(Optional.empty(), TransformationSubtype.IDENTITY, Optional.of("varchar")),
                        "total", new ColumnProvenance(Optional.of("sum"), TransformationSubtype.AGGREGATION, Optional.of("bigint"))));

        createMaterializedViewSource(
                new SchemaTableName("s1", "mv_provenance_cases"),
                ImmutableList.of(
                        column("b", VARCHAR),
                        column("total", BIGINT),
                        column("peak", BIGINT),
                        column("distinct_count", BIGINT),
                        column("distinct_total", BIGINT),
                        column("distinct_peak", BIGINT),
                        column("wrapped", BIGINT),
                        column("future", BIGINT)),
                ImmutableMap.of(
                        "B", new ColumnProvenance(Optional.empty(), TransformationSubtype.IDENTITY, Optional.of("varchar")),
                        "Total", new ColumnProvenance(Optional.of("sum"), TransformationSubtype.AGGREGATION, Optional.of("bigint")),
                        "peak", new ColumnProvenance(Optional.of("max"), TransformationSubtype.AGGREGATION, Optional.of("bigint")),
                        "distinct_count", new ColumnProvenance(Optional.of("count"), TransformationSubtype.AGGREGATION, Optional.of("bigint"), true),
                        "distinct_total", new ColumnProvenance(Optional.of("sum"), TransformationSubtype.AGGREGATION, Optional.of("bigint"), true),
                        "distinct_peak", new ColumnProvenance(Optional.of("max"), TransformationSubtype.AGGREGATION, Optional.of("bigint"), true),
                        "wrapped", new ColumnProvenance(Optional.empty(), TransformationSubtype.AGGREGATION, Optional.of("bigint")),
                        "future", new ColumnProvenance(ColumnProvenance.CURRENT_VERSION + 1, Optional.of("sum"), TransformationSubtype.AGGREGATION, Optional.of("bigint"))));

        String viewData = jsonCodec(ViewDefinition.class).toJson(new ViewDefinition(
                "SELECT b, total FROM mv_rollup_source",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ViewDefinition.ViewColumn("b", VARCHAR), new ViewDefinition.ViewColumn("total", BIGINT)),
                Optional.of("user"),
                false));
        ConnectorTableMetadata viewMetadata = new ConnectorTableMetadata(
                new SchemaTableName("s1", "v_over_rollup_source"),
                ImmutableList.of(column("b", VARCHAR), column("total", BIGINT)));
        runInSetupTransaction(session -> metadata.createView(session, TPCH_CATALOG, viewMetadata, viewData, false));

        ConnectorTableMetadata sameNameInSecondCatalog = new ConnectorTableMetadata(
                new SchemaTableName("s1", "mv_rollup_source"),
                ImmutableList.of(column("b", VARCHAR), column("a", BIGINT)));
        runInSetupTransaction(session -> metadata.createTable(session, SECOND_CATALOG, sameNameInSecondCatalog, false));

        SchemaTableName malformedCatalogs = new SchemaTableName("s1", "mv_malformed_catalogs");
        MaterializedViewDefinition malformedCatalogDefinition = new MaterializedViewDefinition(
                "SELECT b FROM t1 JOIN t2 ON t1.a = t2.a",
                malformedCatalogs.getSchemaName(),
                malformedCatalogs.getTableName(),
                ImmutableList.of(T1, new SchemaTableName("s1", "t2")),
                Optional.of(ImmutableList.of(TPCH_CATALOG)),
                Optional.of("user"),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("b", new ColumnProvenance(Optional.empty(), TransformationSubtype.IDENTITY, Optional.of("bigint"))));
        runInSetupTransaction(session -> metadata.createMaterializedView(
                session,
                TPCH_CATALOG,
                new ConnectorTableMetadata(malformedCatalogs, ImmutableList.of(column("b", BIGINT))),
                malformedCatalogDefinition,
                false));

        SchemaTableName missingCatalogs = new SchemaTableName("s1", "mv_missing_catalogs");
        MaterializedViewDefinition missingCatalogDefinition = new MaterializedViewDefinition(
                "SELECT b, SUM(a) AS total FROM t6 GROUP BY b",
                missingCatalogs.getSchemaName(),
                missingCatalogs.getTableName(),
                ImmutableList.of(new SchemaTableName("s1", "t6")),
                Optional.of("user"),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.of(
                        "b", new ColumnProvenance(Optional.empty(), TransformationSubtype.IDENTITY, Optional.of("varchar")),
                        "total", new ColumnProvenance(Optional.of("sum"), TransformationSubtype.AGGREGATION, Optional.of("bigint"))));
        runInSetupTransaction(session -> metadata.createMaterializedView(
                session,
                TPCH_CATALOG,
                new ConnectorTableMetadata(missingCatalogs, ImmutableList.of(column("b", VARCHAR), column("total", BIGINT))),
                missingCatalogDefinition,
                false));
    }

    private void createMaterializedViewSource(SchemaTableName name, List<ColumnMetadata> columns, Map<String, ColumnProvenance> provenance)
    {
        createMaterializedViewSource(TPCH_CATALOG, name, columns, provenance);
    }

    private void createMaterializedViewSource(String catalog, SchemaTableName name, List<ColumnMetadata> columns, Map<String, ColumnProvenance> provenance)
    {
        MaterializedViewDefinition definition = new MaterializedViewDefinition(
                "SELECT b, SUM(a) AS total FROM t6 GROUP BY b",
                name.getSchemaName(),
                name.getTableName(),
                ImmutableList.of(new SchemaTableName("s1", "t6")),
                Optional.of(ImmutableList.of(catalog)),
                Optional.of("user"),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                provenance);
        runInSetupTransaction(session -> metadata.createMaterializedView(session, catalog, new ConnectorTableMetadata(name, columns), definition, false));
    }

    private static ColumnMetadata column(String name, Type type)
    {
        return ColumnMetadata.builder().setName(name).setType(type).build();
    }

    private void runInSetupTransaction(Consumer<Session> consumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, consumer);
    }

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

    /**
     * Merge-partner validation resolves the source view's definition through the metadata layer,
     * so it has to run inside the transaction rather than on a returned extractor.
     */
    private void overMaterializedViewSource(@Language("SQL") String sql, Consumer<MaterializedViewColumnMappingExtractor> assertions)
    {
        Session session = testSessionBuilder()
                .setCatalog(TPCH_CATALOG)
                .setSchema("s1")
                .setSystemProperty(MATERIALIZED_VIEW_ON_MATERIALIZED_VIEW_ENABLED, "true")
                .build();

        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(session, transactionSession -> {
                    Analyzer analyzer = createAnalyzer(transactionSession, metadata, WarningCollector.NOOP, Optional.empty(), sql);
                    CreateMaterializedView statement = (CreateMaterializedView) SQL_PARSER.createStatement(sql);
                    Analysis analysis = analyzer.analyzeSemantic(statement, false);
                    assertions.accept(new MaterializedViewColumnMappingExtractor(analysis, transactionSession, metadata));
                });
    }

    private void validateMergePartnersOverMaterializedViewSource(@Language("SQL") String sql)
    {
        overMaterializedViewSource(sql, MaterializedViewColumnMappingExtractor::validateAggregateMergePartners);
    }

    private void assertMergePartnersRejected(@Language("SQL") String sql, String expectedMessage)
    {
        try {
            validateMergePartnersOverMaterializedViewSource(sql);
            fail("expected the merge-partner check to reject: " + sql);
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
        }
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
    public void testMaterializedViewOnMaterializedViewRejectedByDefault()
    {
        try {
            extract("CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(total) AS total FROM mv_rollup_source GROUP BY b");
            fail("expected a materialized view over a materialized view to be rejected by default");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("on a materialized view"), e.getMessage());
        }
    }

    @Test
    public void testMaterializedViewOnMaterializedViewAllowedBySessionProperty()
    {
        overMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(total) AS total FROM mv_rollup_source GROUP BY b",
                extractor -> {
                    extractor.validateAggregateMergePartners();
                    assertEquals(
                            baseColumnNames(extractor.getMaterializedViewDerivedColumnMappings().get("total")),
                            ImmutableSet.of("total"));
                });
    }

    @Test
    public void testMaterializedViewSourceConnectorMustSupportMaterializedViewsOverMaterializedViews()
    {
        try {
            overMaterializedViewSource(
                    "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(total) AS total FROM c3.s1.mv_without_connector_capability GROUP BY b",
                    ignored -> {});
            fail("expected source connector capability validation to fail");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("Catalog c3 does not support materialized views over materialized views"), e.getMessage());
        }
    }

    @Test
    public void testMaterializedViewTargetConnectorMustSupportMaterializedViewsOverMaterializedViews()
    {
        try {
            overMaterializedViewSource(
                    "CREATE MATERIALIZED VIEW c3.s1.mv2 AS SELECT b, SUM(total) AS total FROM mv_rollup_source GROUP BY b",
                    ignored -> {});
            fail("expected target connector capability validation to fail");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("Catalog c3 does not support materialized views over materialized views"), e.getMessage());
        }
    }

    @Test
    public void testMaterializedViewSourceDepthLimit()
    {
        overMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv_depth_allowed AS SELECT 1 AS a FROM mv_source_depth_3",
                ignored -> {});

        try {
            overMaterializedViewSource(
                    "CREATE MATERIALIZED VIEW mv_depth_rejected AS SELECT 1 AS a FROM mv_source_depth_4",
                    ignored -> {});
            fail("expected a materialized view source chain above the maximum depth to be rejected");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("exceeds the maximum supported depth of 4"), e.getMessage());
        }
    }

    @Test
    public void testRegisteredMergeIsAcceptedOverPartial()
    {
        validateMergePartnersOverMaterializedViewSource("CREATE MATERIALIZED VIEW mv2 AS SELECT b, TEST_MERGE(m) AS m FROM mv_rollup_source GROUP BY b");
    }

    @Test
    public void testCountOverACountColumnIsRejected()
    {
        // The silent case: BIGINT to BIGINT either way, but this counts groups rather than rows.
        try {
            validateMergePartnersOverMaterializedViewSource("CREATE MATERIALIZED VIEW mv2 AS SELECT b, COUNT(n) AS n FROM mv_rollup_source GROUP BY b");
            fail("expected count over a count column to be rejected");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("was produced by count, so rolling it up requires sum, not count"), e.getMessage());
        }
    }

    @Test
    public void testWrongMergePartnerOverSumIsRejected()
    {
        try {
            validateMergePartnersOverMaterializedViewSource("CREATE MATERIALIZED VIEW mv2 AS SELECT b, MIN(total) AS total FROM mv_rollup_source GROUP BY b");
            fail("expected min over a sum column to be rejected");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("requires sum, not min"), e.getMessage());
        }
    }

    @Test
    public void testCountStarOverAMaterializedViewIsRejected()
    {
        // COUNT(*) consumes no source column, so there is nothing to check it against. Requiring
        // exactly one source column is what makes it rejectable at all.
        try {
            validateMergePartnersOverMaterializedViewSource("CREATE MATERIALIZED VIEW mv2 AS SELECT b, COUNT(*) AS n FROM mv_rollup_source GROUP BY b");
            fail("expected count(*) over a materialized view to be rejected");
        }
        catch (SemanticException e) {
            assertTrue(e.getMessage().contains("reads no column of the source materialized view"), e.getMessage());
        }
    }

    @Test
    public void testAggregatingAMaterializedViewDimensionIsAllowed()
    {
        // b is an identity column of the source view, so no merge-partner constraint applies.
        validateMergePartnersOverMaterializedViewSource("CREATE MATERIALIZED VIEW mv2 AS SELECT MIN(b) AS b FROM mv_rollup_source");
    }

    @Test
    public void testMixedCaseAliasOverMaterializedViewIsAccepted()
    {
        validateMergePartnersOverMaterializedViewSource("CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(n) AS Total FROM mv_rollup_source GROUP BY b");
    }

    @Test
    public void testDuplicateSensitiveAggregateOverADimensionIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT COUNT(b) AS n FROM mv_rollup_source",
                "reads column b of s1.mv_rollup_source, which is not an aggregate");
    }

    @Test
    public void testDistinctAggregateOverADimensionIsAllowed()
    {
        validateMergePartnersOverMaterializedViewSource("CREATE MATERIALIZED VIEW mv2 AS SELECT COUNT(DISTINCT b) AS n FROM mv_rollup_source");
    }

    @Test
    public void testDuplicateInsensitiveAggregateOverAJoinedTableIsAllowed()
    {
        validateMergePartnersOverMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT s.b, MAX(t.a) AS a FROM mv_rollup_source s JOIN t6 t ON s.b = t.b GROUP BY s.b");
    }

    @Test
    public void testDuplicateSensitiveAggregateOverAJoinedTableIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT s.b, COUNT(t.a) AS a FROM mv_rollup_source s JOIN t6 t ON s.b = t.b GROUP BY s.b",
                "reads only columns of tables that are not materialized views");
    }

    @Test
    public void testOnlyDuplicateInsensitiveRollupsAreAllowedInAJoin()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT s.b, SUM(s.total) AS total FROM mv_rollup_source s JOIN t6 t ON s.b = t.b GROUP BY s.b",
                "rolls up column total of s1.mv_rollup_source in a query with a join");
        validateMergePartnersOverMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT s.b, MAX(s.peak) AS peak FROM mv_provenance_cases s JOIN t6 t ON s.b = t.b GROUP BY s.b");
    }

    @Test
    public void testRollupOverAnExpressionIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(total * 2) AS total FROM mv_rollup_source GROUP BY b",
                "must read column total of s1.mv_rollup_source directly or through a widening CAST");
    }

    @Test
    public void testRollupThroughAWideningCastIsAccepted()
    {
        validateMergePartnersOverMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(CAST(total AS DECIMAL(38, 0))) AS total FROM mv_rollup_source GROUP BY b");
    }

    @Test
    public void testRollupThroughALossyCastIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(CAST(total AS DOUBLE)) AS total FROM mv_rollup_source GROUP BY b",
                "directly or through a widening CAST");
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(CAST(total AS INTEGER)) AS total FROM mv_rollup_source GROUP BY b",
                "directly or through a widening CAST");
    }

    @Test
    public void testDistinctRollupIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(DISTINCT total) AS total FROM mv_rollup_source GROUP BY b",
                "uses DISTINCT");
    }

    @Test
    public void testDistinctSourceAggregatesAreValidated()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT SUM(distinct_count) AS n FROM mv_provenance_cases",
                "was produced by DISTINCT count");
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT SUM(distinct_total) AS total FROM mv_provenance_cases",
                "was produced by DISTINCT sum");
        validateMergePartnersOverMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT MAX(distinct_peak) AS peak FROM mv_provenance_cases");
    }

    @Test
    public void testPassThroughPreservesAggregateProvenance()
    {
        overMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, total FROM mv_rollup_source",
                extractor -> {
                    ColumnProvenance total = extractor.getMaterializedViewColumnProvenance().get("total");
                    assertTrue(total.isAggregation());
                    assertEquals(total.getFunctionName(), Optional.of("sum"));
                });
    }

    @Test
    public void testSelectStarPreservesAggregateProvenance()
    {
        overMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM mv_rollup_source",
                extractor -> {
                    Map<String, ColumnProvenance> provenance = extractor.getMaterializedViewColumnProvenance();
                    assertEquals(provenance.keySet(), ImmutableSet.of("b", "n", "total", "m"));
                    assertEquals(provenance.get("n").getFunctionName(), Optional.of("count"));
                    assertEquals(provenance.get("total").getFunctionName(), Optional.of("sum"));
                });
    }

    @Test
    public void testJoinPassThroughBecomesNonMergeable()
    {
        overMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT s.total FROM mv_rollup_source s JOIN t6 t ON s.b = t.b",
                extractor -> {
                    ColumnProvenance total = extractor.getMaterializedViewColumnProvenance().get("total");
                    assertTrue(total.isAggregation());
                    assertEquals(total.getFunctionName(), Optional.empty());
                });
    }

    @Test
    public void testExpressionOverAggregateSourceBecomesNonMergeable()
    {
        overMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT total * 2 AS scaled FROM mv_rollup_source",
                extractor -> {
                    ColumnProvenance scaled = extractor.getMaterializedViewColumnProvenance().get("scaled");
                    assertTrue(scaled.isAggregation());
                    assertEquals(scaled.getFunctionName(), Optional.empty());
                });
    }

    @Test
    public void testAggregateSourceColumnsAreRejectedInUnsafeClauses()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(total) AS total FROM mv_rollup_source WHERE total > 0 GROUP BY b",
                "WHERE clause reads aggregated column total");
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT total FROM mv_rollup_source GROUP BY total",
                "GROUP BY clause reads aggregated column total");
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT s.total FROM mv_rollup_source s JOIN t6 t ON s.total = t.a",
                "JOIN clause reads aggregated column total");
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT DISTINCT total FROM mv_rollup_source",
                "SELECT DISTINCT clause reads aggregated column total");
    }

    @Test
    public void testHavingAggregatesUseMergePartnerValidation()
    {
        validateMergePartnersOverMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b FROM mv_rollup_source GROUP BY b HAVING SUM(total) > 0");
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b FROM mv_rollup_source GROUP BY b HAVING COUNT(total) > 1",
                "was produced by sum, so rolling it up requires sum, not count");
    }

    @Test
    public void testConstantFilterHasNoSourceColumns()
    {
        validateMergePartnersOverMaterializedViewSource(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(total) AS total FROM mv_rollup_source WHERE 1 = 1 GROUP BY b");
    }

    @Test
    public void testPartialBaseTableCatalogListIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b FROM mv_malformed_catalogs",
                "has 2 base tables but 1 base-table catalogs");
    }

    @Test
    public void testMissingBaseTableCatalogListIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(total) AS total FROM mv_missing_catalogs GROUP BY b",
                "has no base-table catalog metadata");
    }

    @Test
    public void testFilteredRollupIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(total) FILTER (WHERE total > 0) AS total FROM mv_rollup_source GROUP BY b",
                "has a FILTER clause");
    }

    @Test
    public void testWrappedRollupIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, CAST(SUM(total) AS DOUBLE) AS total FROM mv_rollup_source GROUP BY b",
                "Column total wraps an aggregate in another expression");
    }

    @Test
    public void testSourceColumnWithoutProvenanceIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(total) AS total FROM mv_without_provenance GROUP BY b",
                "Column total of s1.mv_without_provenance has no recorded provenance");
    }

    @Test
    public void testSourceProvenanceLookupIgnoresCase()
    {
        validateMergePartnersOverMaterializedViewSource("CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(total) AS total FROM mv_provenance_cases GROUP BY b");
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, COUNT(total) AS total FROM mv_provenance_cases GROUP BY b",
                "was produced by sum, so rolling it up requires sum, not count");
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT COUNT(b) AS n FROM mv_provenance_cases",
                "which is not an aggregate");
    }

    @Test
    public void testSourceAggregateWrappedInAnExpressionIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT SUM(wrapped) AS wrapped FROM mv_provenance_cases",
                "Column wrapped of s1.mv_provenance_cases was produced by an aggregate wrapped in another expression");
    }

    @Test
    public void testUnsupportedSourceProvenanceVersionIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT SUM(future) AS future FROM mv_provenance_cases",
                "has provenance version 2, but only version 1 is supported");
    }

    @Test
    public void testQueryShapesThatHideAggregatesAreRejected()
    {
        String expected = "must be a single SELECT over tables and joins";
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS (SELECT b, COUNT(n) AS n FROM mv_rollup_source GROUP BY b)",
                expected);
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, n FROM (SELECT b, COUNT(n) AS n FROM mv_rollup_source GROUP BY b) t",
                expected);
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS WITH t AS (SELECT b, COUNT(n) AS n FROM mv_rollup_source GROUP BY b) SELECT b, n FROM t",
                expected);
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, total FROM mv_rollup_source UNION ALL SELECT b, a FROM t6",
                expected);
    }

    @Test
    public void testMaterializedViewReadThroughAViewIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, SUM(total) AS total FROM v_over_rollup_source GROUP BY b",
                "Materialized view s1.mv_rollup_source is read through a view");
    }

    @Test
    public void testWindowFunctionOverMaterializedViewIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT b, COUNT(n) OVER (PARTITION BY b) AS n FROM mv_rollup_source",
                "is not supported in a materialized view over a materialized view");
    }

    @Test
    public void testSourceSharingItsNameWithATableInAnotherCatalogIsRejected()
    {
        assertMergePartnersRejected(
                "CREATE MATERIALIZED VIEW mv2 AS SELECT s.b, MAX(x.a) AS a FROM mv_rollup_source s JOIN c2.s1.mv_rollup_source x ON s.b = x.b GROUP BY s.b",
                "has the same schema and table name as a table in another catalog");
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
