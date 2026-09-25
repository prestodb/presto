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
package com.facebook.presto.sql;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.Union;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.MaterializedViewUtils.buildMaterializedViewScanFilter;
import static com.facebook.presto.sql.MaterializedViewUtils.buildPartitionsToRecomputeFilter;
import static com.facebook.presto.sql.MaterializedViewUtils.generateBaseTablePredicates;
import static com.facebook.presto.sql.MaterializedViewUtils.generateFalsePredicates;
import static com.facebook.presto.sql.MaterializedViewUtils.requiresOuterFilter;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.SqlFormatterUtil.getFormattedSql;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.AND;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.OR;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestMaterializedViewUtils
{
    private final Metadata metadata = createTestMetadataManager();
    private static final SchemaTableName BASE_A = new SchemaTableName("s", "base_a");
    private static final SchemaTableName BASE_B = new SchemaTableName("s", "base_b");

    @Test
    public void testEmptyReturnsNoFilter()
    {
        assertFalse(buildMaterializedViewScanFilter(Optional.empty(), metadata).isPresent());
        assertFalse(buildMaterializedViewScanFilter(Optional.of(predicates(ImmutableList.of(), "ds")), metadata).isPresent());
    }

    @Test
    public void testSinglePartitionNegated()
    {
        Optional<Expression> filter = buildMaterializedViewScanFilter(
                Optional.of(predicates(ImmutableList.of(partition("ds", "2026-05-25")), "ds")), metadata);

        assertTrue(innerOf(filter) instanceof ComparisonExpression);
    }

    @Test
    public void testMultiplePartitionsNegatedDisjunction()
    {
        Optional<Expression> filter = buildMaterializedViewScanFilter(
                Optional.of(predicates(ImmutableList.of(partition("ds", "2026-05-25"), partition("ds", "2026-05-26")), "ds")), metadata);

        Expression inner = innerOf(filter);
        assertTrue(inner instanceof LogicalBinaryExpression);
        assertEquals(((LogicalBinaryExpression) inner).getOperator(), LogicalBinaryExpression.Operator.OR);
    }

    @Test
    public void testMultiColumnPartitionNegatedConjunction()
    {
        Optional<Expression> filter = buildMaterializedViewScanFilter(
                Optional.of(predicates(ImmutableList.of(partition2("ds", "2026-05-25", "region", "us")), "ds", "region")), metadata);

        Expression inner = innerOf(filter);
        assertTrue(inner instanceof LogicalBinaryExpression);
        assertEquals(((LogicalBinaryExpression) inner).getOperator(), LogicalBinaryExpression.Operator.AND);
    }

    @Test
    public void testPartialColumnMappingBaseTablePredicate()
    {
        // Collapsed disjuncts must render without a ts predicate (buildPartitionPredicate skips absent columns).
        SchemaTableName baseTable = new SchemaTableName("schema", "base");
        MaterializedDataPredicates predicatesInfo = predicates(
                ImmutableList.of(
                        partition3("ds", "d", "ts", "t1", "c", "c2"),
                        partition3("ds", "d", "ts", "t1", "c", "c3"),
                        partition3("ds", "d", "ts", "t2", "c", "c2"),
                        partition2("ds", "d", "c", "c1")),
                "ds", "ts", "c");

        Expression predicate = generateBaseTablePredicates(ImmutableMap.of(baseTable, predicatesInfo), metadata).get(baseTable);

        Set<String> renderedDisjuncts = flatten(predicate, OR).stream()
                .map(TestMaterializedViewUtils::renderConjunction)
                .collect(toImmutableSet());

        assertEquals(renderedDisjuncts, ImmutableSet.of(
                "c=c2 AND ds=d AND ts=t1",
                "c=c3 AND ds=d AND ts=t1",
                "c=c2 AND ds=d AND ts=t2",
                "c=c1 AND ds=d"));
    }

    private static String renderConjunction(Expression conjunction)
    {
        return flatten(conjunction, AND).stream()
                .map(conjunct -> {
                    ComparisonExpression comparison = (ComparisonExpression) conjunct;
                    return ((Identifier) comparison.getLeft()).getValue() + "=" + valueOf(comparison.getRight());
                })
                .sorted()
                .collect(joining(" AND "));
    }

    private static String valueOf(Expression expression)
    {
        if (expression instanceof Cast) {
            expression = ((Cast) expression).getExpression();
        }
        return ((StringLiteral) expression).getValue();
    }

    private static List<Expression> flatten(Expression expression, LogicalBinaryExpression.Operator operator)
    {
        if (expression instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) expression).getOperator() == operator) {
            LogicalBinaryExpression binary = (LogicalBinaryExpression) expression;
            return ImmutableList.<Expression>builder()
                    .addAll(flatten(binary.getLeft(), operator))
                    .addAll(flatten(binary.getRight(), operator))
                    .build();
        }
        return ImmutableList.of(expression);
    }

    @Test
    public void testGenerateBaseTablePredicatesRendersMissing()
    {
        SchemaTableName base1 = new SchemaTableName("s", "base1");
        Map<SchemaTableName, MaterializedDataPredicates> missing = ImmutableMap.of(
                base1, predicates(ImmutableList.of(partition("ds", "2026-08-02")), "ds"));
        assertEquals(renderConjunction(generateBaseTablePredicates(missing, metadata).get(base1)), "ds=2026-08-02");
    }

    @Test
    public void testBuildPartitionsToRecomputeFilterIsPositiveFullTuple()
    {
        // Positive full tuple; the view-scan exclusion is exactly NOT(COALESCE(<this>, false)).
        MaterializedDataPredicates toRecompute = predicates(ImmutableList.of(partition2("ds", "2026-06-10", "country", "us")), "ds", "country");
        assertEquals(renderConjunction(buildPartitionsToRecomputeFilter(Optional.of(toRecompute), metadata).get()), "country=us AND ds=2026-06-10");
        assertEquals(renderConjunction(innerOf(buildMaterializedViewScanFilter(Optional.of(toRecompute), metadata))), "country=us AND ds=2026-06-10");
        assertFalse(buildPartitionsToRecomputeFilter(Optional.empty(), metadata).isPresent());
    }

    @Test
    public void testScanFilterIsNullSafeForFreshNullPartition()
    {
        // COALESCE(<predicate>, false) keeps fresh NULL-partition rows: bare NOT(ds='...') is UNKNOWN for NULL ds and drops them.
        Optional<Expression> filter = buildMaterializedViewScanFilter(
                Optional.of(predicates(ImmutableList.of(partition("ds", "2026-05-25")), "ds")), metadata);
        assertTrue(filter.isPresent());
        assertTrue(filter.get() instanceof NotExpression);
        Expression coalesce = ((NotExpression) filter.get()).getValue();
        assertTrue(coalesce instanceof CoalesceExpression, "expected NULL-safe COALESCE(<predicate>, false) wrapper");
        List<Expression> operands = ((CoalesceExpression) coalesce).getOperands();
        assertEquals(operands.size(), 2);
        assertEquals(operands.get(1), FALSE_LITERAL);
        assertTrue(operands.get(0) instanceof ComparisonExpression);
    }

    @Test
    public void testScanFilterWithNullAndNonNullPartitions()
    {
        // A genuinely NULL partition plus a non-null one renders (ds IS NULL) OR (ds = '2026-05-25').
        MaterializedDataPredicates toRecompute = predicates(ImmutableList.of(nullPartition("ds"), partition("ds", "2026-05-25")), "ds");
        Set<String> kinds = flatten(innerOf(buildMaterializedViewScanFilter(Optional.of(toRecompute), metadata)), OR).stream()
                .map(disjunct -> disjunct.getClass().getSimpleName())
                .collect(toImmutableSet());
        assertEquals(kinds, ImmutableSet.of("IsNullPredicate", "ComparisonExpression"));
    }

    @Test
    public void testGenerateBaseTablePredicatesNoMissingForBaseRendersFalse()
    {
        // No missing partitions -> FALSE so PredicateStitcher prunes the base scan (an absent base is left unfiltered).
        Map<SchemaTableName, MaterializedDataPredicates> noMissing = ImmutableMap.of(BASE_A, predicates(ImmutableList.of(), "ds"));
        assertEquals(generateBaseTablePredicates(noMissing, metadata).get(BASE_A), FALSE_LITERAL);
    }

    @Test
    public void testGenerateFalsePredicatesForFullyMaterialized()
    {
        // Fully materialized: every base stitched FALSE so the query is served entirely from the view scan.
        Map<SchemaTableName, Expression> predicates = generateFalsePredicates(ImmutableList.of(BASE_A, BASE_B));
        assertEquals(predicates.get(BASE_A), FALSE_LITERAL);
        assertEquals(predicates.get(BASE_B), FALSE_LITERAL);
    }

    @Test
    public void testRequiresOuterFilter()
    {
        Optional<MaterializedDataPredicates> toRecompute = Optional.of(predicates(ImmutableList.of(partition("ds", "2026-06-10")), "ds"));
        Map<String, Map<SchemaTableName, String>> dsToA = ImmutableMap.of("ds", ImmutableMap.of(BASE_A, "ds"));

        // Single base, column maps -> no outer filter (foldable fast path).
        assertFalse(requiresOuterFilter(toRecompute, dsToA, ImmutableList.of(BASE_A)));
        // Inner join, shared key maps to every base -> no outer filter (foldable fast path).
        Map<String, Map<SchemaTableName, String>> dsToBoth = ImmutableMap.of("ds", ImmutableMap.of(BASE_A, "ds", BASE_B, "ds"));
        assertFalse(requiresOuterFilter(toRecompute, dsToBoth, ImmutableList.of(BASE_A, BASE_B)));
        // ds maps to BASE_A but not BASE_B -> outer filter.
        assertTrue(requiresOuterFilter(toRecompute, dsToA, ImmutableList.of(BASE_A, BASE_B)));
        // Join with partition columns from different base tables (an->A, bn->B) -> outer filter.
        Optional<MaterializedDataPredicates> joinKeys = Optional.of(
                predicates(ImmutableList.of(partition2("an", "US", "bn", "FR")), "an", "bn"));
        Map<String, Map<SchemaTableName, String>> anToAbnToB = ImmutableMap.of(
                "an", ImmutableMap.of(BASE_A, "an"),
                "bn", ImmutableMap.of(BASE_B, "bn"));
        assertTrue(requiresOuterFilter(joinKeys, anToAbnToB, ImmutableList.of(BASE_A, BASE_B)));
        // UNION constant: a "source" column unmapped to any base -> outer filter.
        Optional<MaterializedDataPredicates> recomputeWithConstant = Optional.of(
                predicates(ImmutableList.of(partition2("ds", "2026-06-10", "source", "app")), "ds", "source"));
        assertTrue(requiresOuterFilter(recomputeWithConstant, dsToA, ImmutableList.of(BASE_A)));
        // Empty -> no outer filter.
        assertFalse(requiresOuterFilter(Optional.empty(), ImmutableMap.of(), ImmutableList.of(BASE_A)));
    }

    @Test
    public void testUnionArityTwoVsThreeBranches()
    {
        // 2 branches (per-base recompute + view scan), plus a 3rd outer-filter branch iff requiresOuterFilter.
        Optional<MaterializedDataPredicates> toRecompute = Optional.of(predicates(ImmutableList.of(partition("ds", "2026-06-10")), "ds"));
        Map<String, Map<SchemaTableName, String>> dsToA = ImmutableMap.of("ds", ImmutableMap.of(BASE_A, "ds"));

        // Maps to the only base -> 2-way union.
        assertEquals(unionBranches(toRecompute, dsToA, ImmutableList.of(BASE_A)), 2);
        // ds maps to BASE_A but not BASE_B -> outer filter -> 3-way union.
        assertEquals(unionBranches(toRecompute, dsToA, ImmutableList.of(BASE_A, BASE_B)), 3);
        // Empty -> 2-way union (legacy shape).
        assertEquals(unionBranches(Optional.empty(), ImmutableMap.of(), ImmutableList.of(BASE_A)), 2);
    }

    private int unionBranches(Optional<MaterializedDataPredicates> toRecompute,
            Map<String, Map<SchemaTableName, String>> mappings, List<SchemaTableName> baseTables)
    {
        return 2 + (requiresOuterFilter(toRecompute, mappings, baseTables) ? 1 : 0);
    }

    @Test
    public void testFlatThreeWayUnionRequiresFormatSql()
    {
        // The 3-way stitched union is built flat (new Union of three branches). getFormattedSql rejects it because the
        // parser re-associates `a UNION ALL b UNION ALL c` into left-nested binary unions and the structural round-trip
        // check fails; formatSql produces correct SQL (the caller re-parses it). This is why the outer-filter path uses
        // formatSql instead of getFormattedSql.
        SqlParser sqlParser = new SqlParser();
        ParsingOptions options = new ParsingOptions();
        Union flat = new Union(ImmutableList.of(
                queryBody(sqlParser, options, "SELECT 1 AS x"),
                queryBody(sqlParser, options, "SELECT 2 AS x"),
                queryBody(sqlParser, options, "SELECT 3 AS x")), Optional.of(false));
        Query unionQuery = query(flat);

        expectThrows(PrestoException.class, () -> getFormattedSql(unionQuery, sqlParser, Optional.empty()));
        formatSql(unionQuery, Optional.empty());
    }

    private static QueryBody queryBody(SqlParser sqlParser, ParsingOptions options, String sql)
    {
        return ((Query) sqlParser.createStatement(sql, options)).getQueryBody();
    }

    private static Query query(QueryBody queryBody)
    {
        return new Query(Optional.empty(), queryBody, Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static Set<String> disjunctsOf(Expression predicate)
    {
        return flatten(predicate, OR).stream().map(TestMaterializedViewUtils::renderConjunction).collect(toImmutableSet());
    }

    private static TupleDomain<String> nullPartition(String column)
    {
        return TupleDomain.fromFixedValues(ImmutableMap.of(column, NullableValue.asNull(VARCHAR)));
    }

    private static Expression innerOf(Optional<Expression> filter)
    {
        assertTrue(filter.isPresent());
        assertTrue(filter.get() instanceof NotExpression);
        Expression coalesce = ((NotExpression) filter.get()).getValue();
        assertTrue(coalesce instanceof CoalesceExpression, "expected NULL-safe COALESCE(<predicate>, false) wrapper");
        List<Expression> operands = ((CoalesceExpression) coalesce).getOperands();
        assertEquals(operands.size(), 2);
        assertEquals(operands.get(1), FALSE_LITERAL);
        return operands.get(0);
    }

    private static TupleDomain<String> partition(String column, String value)
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.singleValue(VARCHAR, utf8Slice(value))));
    }

    private static TupleDomain<String> partition2(String column1, String value1, String column2, String value2)
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of(
                column1, Domain.singleValue(VARCHAR, utf8Slice(value1)),
                column2, Domain.singleValue(VARCHAR, utf8Slice(value2))));
    }

    private static TupleDomain<String> partition3(String column1, String value1, String column2, String value2, String column3, String value3)
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of(
                column1, Domain.singleValue(VARCHAR, utf8Slice(value1)),
                column2, Domain.singleValue(VARCHAR, utf8Slice(value2)),
                column3, Domain.singleValue(VARCHAR, utf8Slice(value3))));
    }

    private static MaterializedDataPredicates predicates(List<TupleDomain<String>> disjuncts, String... columns)
    {
        return new MaterializedDataPredicates(disjuncts, ImmutableList.copyOf(columns));
    }
}
