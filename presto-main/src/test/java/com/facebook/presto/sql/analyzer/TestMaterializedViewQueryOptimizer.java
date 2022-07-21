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
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestMaterializedViewQueryOptimizer
        extends AbstractAnalyzerTest
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final String BASE_TABLE_1 = "t1";
    private static final String BASE_TABLE_2 = "t2";
    private static final String BASE_TABLE_3 = "t3";
    private static final String BASE_TABLE_6 = "t6";
    private static final String BASE_TABLE_7 = "t7";
    private static final String VIEW_1 = "view_1";
    private static final String VIEW_2 = "view_2";
    private static final String VIEW_3 = "view_3";
    private static final String SESSION_SCHEMA = "s1";

    private RowExpressionDomainTranslator domainTranslator;

    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema(SESSION_SCHEMA)
            .setSystemProperty("parse_decimal_literals_as_double", "true")
            .build();

    @BeforeClass
    public void setupDomainTranslator()
    {
        domainTranslator = new RowExpressionDomainTranslator(metadata);
    }

    @Test
    public void testWithSimpleQuery()
    {
        String originalViewSql = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b FROM %s", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithDistinct()
    {
        String originalViewSql = format("SELECT DISTINCT a, b FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT DISTINCT a, b FROM %s", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT DISTINCT a, b FROM %s", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT DISTINCT a, b FROM %s", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT DISTINCT a, b FROM %s", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT DISTINCT a, b FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithAlias()
    {
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT mv_a as a, b, mv_c as c FROM %s", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a as mv_a, b, c as mv_c, d FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a as result_a, b as result_b, c, d FROM %s", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT mv_a as result_a, b as result_b, mv_c as c, d FROM %s", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a as b, b as a FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b FROM %s", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT b as a, a as b FROM %s", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithAllColumnsSelect()
    {
        String originalViewSql = format("SELECT * FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT * FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithBaseQueryGroupBy()
    {
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT SUM(a * b), MAX(a + b), c FROM %s GROUP BY c", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT SUM(mv_a * b), MAX(mv_a + b), mv_c as c FROM %s GROUP BY mv_c", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithDerivedFields()
    {
        String originalViewSql = format("SELECT SUM(a * b + c) as mv_sum, MAX(a * b + c) as mv_max, d, e FROM %s GROUP BY d, e", BASE_TABLE_1);
        String baseQuerySql = format("SELECT SUM(a * b + c), MAX(a * b + c), d, e FROM %s GROUP BY d, e", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT SUM(mv_sum), MAX(mv_max), d, e FROM %s GROUP BY d, e", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT SUM(a * b + c) as mv_sum, MAX(a * b + c) as mv_max, d as mv_d, e FROM %s GROUP BY d, e", BASE_TABLE_1);
        baseQuerySql = format("SELECT SUM(a * b + c) as sum_of_abc, MAX(a * b + c) as max_of_abc, d, e FROM %s GROUP BY d, e", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT SUM(mv_sum) as sum_of_abc, MAX(mv_max) as max_of_abc, mv_d as d, e FROM %s GROUP BY mv_d, e", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithCount()
    {
        String originalViewSql = format("SELECT COUNT(a) as a_count, COUNT(b, c) as bc_count FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT COUNT(a), COUNT(b, c) FROM %s", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT SUM(a_count), SUM(bc_count) FROM %s", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithCountDistinct()
    {
        String originalViewSql = format("SELECT COUNT((a)) as a_count, COUNT(b, c) as bc_count FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT COUNT(DISTINCT(a)), COUNT(b, c) FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT COUNT(DISTINCT(a)) as a_count, COUNT(b, c) as bc_count FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT COUNT(DISTINCT(a)), COUNT(b, c) FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithArithmeticBinary()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a + b, a * b - c FROM %s", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a + b, a * b - c FROM %s", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a as mv_a, b, c as mv_c, d FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a + b, c / d, a * c - b * d FROM %s", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT mv_a + b, mv_c / d, mv_a * mv_c - b * d FROM %s", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithWhereCondition()
    {
        String originalViewSql = format("SELECT a, b, c, d FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b FROM %s WHERE a < 10 AND c > 10 or d = 123", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b FROM %s WHERE a < 10 AND c > 10 or d = 123", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a as mv_a, b, c, d as mv_d FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b FROM %s WHERE a < 10 AND c > 10 or d = 456", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT mv_a as a, b FROM %s WHERE mv_a < 10 AND c > 10 or mv_d = 456", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testMismatchingColumnTypes()
    {
        // d is registered as bigint- expect optimization to fail
        String originalViewSql = format("SELECT a, b, c, d FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b FROM %s WHERE a < 10 AND c > 10 or d = '2000-01-01'", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testColumnsNotInTable()
    {
        String originalViewSql = format("SELECT  a, b, c, d, not_a_column FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, c, not_a_column FROM %s WHERE a > 5 OR IF(b > 4, c, 2) = not_a_column AND d IN (1, 2, 3) AND NOT (a IS NULL)", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithOrderBy()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s ORDER BY c ASC, b DESC, a", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s ORDER BY c ASC, b DESC, a", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a as mv_a, b, c as mv_c FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s ORDER BY c ASC, b DESC, a", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT mv_a as a, b, mv_c as c FROM %s ORDER BY mv_c ASC, b DESC, mv_a", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a as mv_a, b, c as mv_c FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s ORDER BY c ASC, b DESC, a", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT mv_a as a, b, mv_c as c FROM %s ORDER BY mv_c ASC, b DESC, mv_a", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT MAX(a) as mv_max_a, b FROM %s GROUP BY b", BASE_TABLE_1);
        baseQuerySql = format("SELECT MAX(a), b FROM %s GROUP BY b ORDER BY MAX(a) DESC, b ASC", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT MAX(mv_max_a), b FROM %s GROUP BY b ORDER BY MAX(mv_max_a) DESC, b ASC", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithNoMatchingBaseTable()
    {
        String originalViewSql = format("SELECT a, b FROM %s", BASE_TABLE_2);
        String baseQuerySql = format("SELECT a, b FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithNoMatchingColumnNames()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT c, d FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, c FROM %s WHERE d = 5", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithDifferentFilterCondition()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5 OR b = 3", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, c FROM %s WHERE a = 5 OR b = 4", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, c FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testIdentifiersInDifferentNodes()
    {
        String originalViewSql = format("SELECT a, b, c, d FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, c FROM %s WHERE a > 5 OR IF(b > 4, c, 2) = 7 AND d IN (1, 2, 3) AND NOT (a IS NULL)", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, c FROM %s WHERE a > 5 OR IF(b > 4, c, 2) = 7 AND d IN (1, 2, 3) AND NOT (a IS NULL)", VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT a, c FROM %s WHERE x = 4", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT a, c FROM %s WHERE NOT(x IS NULL)", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT a, c FROM %s WHERE NOT(x IN (4, 5))", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT a, c FROM %s WHERE IF(a > 2, IF(x > 0, 1, -1), 2) = 0", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithGroupBy()
    {
        String originalViewSql = format("SELECT SUM(a) AS a, SUM(b*c) AS bc, d, e FROM %s GROUP BY d, e", BASE_TABLE_1);
        String baseQuerySql = format("SELECT SUM(a) FROM %s", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT SUM(a) FROM %s", VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT SUM(b*c) FROM %s WHERE d > 10", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT SUM(bc) FROM %s WHERE d > 10", VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT SUM(a), d FROM %s GROUP BY d", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT SUM(a), d FROM %s GROUP BY d", VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT SUM(a), SUM(b*c), d FROM %s GROUP BY d", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT SUM(a), SUM(bc), d FROM %s GROUP BY d", VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT SUM(a), SUM(b*c), d, e FROM %s GROUP BY d, e", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT SUM(a), SUM(bc), d, e FROM %s GROUP BY d, e", VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT d, e FROM %s", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT SUM(d) FROM %s GROUP BY e", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT SUM(a) FROM %s WHERE x > 10", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT SUM(a), x FROM %s GROUP BY x", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT SUM(a) FROM %s WHERE f IN (1, 2)", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT SUM(a) FROM %s WHERE IF(f, 1, 0) = 1", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        baseQuerySql = format("SELECT MAX(sum_a) FROM (SELECT SUM(a) sum_a, d, e, %s GROUP BY d, e)", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT SUM(a) AS a, b FROM %s GROUP BY c", BASE_TABLE_1);
        baseQuerySql = format("SELECT SUM(a) FROM %s GROUP BY c", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT SUM(a) AS a, c FROM %s WHERE b > 0 GROUP BY c", BASE_TABLE_1);
        baseQuerySql = format("SELECT SUM(a) FROM %s GROUP BY c", BASE_TABLE_1);
        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithMissingColumnInOrderBy()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, c FROM %s ORDER BY b DESC, d", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithLimitClause()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s LIMIT 5", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, c FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithUnsupportedFunction()
    {
        String originalViewSql = format("SELECT AVG(a) FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT AVG(a) FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a FROM %s", BASE_TABLE_1);
        baseQuerySql = format("SELECT AVG(a) FROM %s", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithTableAlias()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s ORDER BY a, c", BASE_TABLE_1);
        String originalViewSqlWithAliasPartially = format("SELECT base1.a, b, c FROM %s base1 ORDER BY base1.a, c", BASE_TABLE_1);
        String originalViewSqlWithAliasFully = format("SELECT base1.a, base1.b, base1.c FROM %s base1 ORDER BY base1.a, base1.c", BASE_TABLE_1);
        String originalViewSqlWithTablePrefix = format("SELECT %s.a, b, %s.c FROM %s ORDER BY %s.a, %s.c", BASE_TABLE_1, BASE_TABLE_1, BASE_TABLE_1, BASE_TABLE_1, BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, c FROM %s ORDER BY c, a", BASE_TABLE_1);
        String baseQuerySqlWithAliasPartially1 = format("SELECT base1.a, c FROM %s base1 ORDER BY c, base1.a", BASE_TABLE_1);
        String baseQuerySqlWithAliasPartially2 = format("SELECT a, base1.c FROM %s base1 ORDER BY base1.c, a", BASE_TABLE_1);
        String baseQuerySqlFully = format("SELECT base1.a, base1.c FROM %s base1 ORDER BY base1.c, base1.a", BASE_TABLE_1);
        String baseQuerySqlWithTablePrefix = format("SELECT %s.a, %s.c FROM %s ORDER BY %s.c, %s.a", BASE_TABLE_1, BASE_TABLE_1, BASE_TABLE_1, BASE_TABLE_1, BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, c FROM %s ORDER BY c, a", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially1, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially2, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlFully, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithTablePrefix, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSqlWithAliasPartially, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially1, expectedRewrittenSql, originalViewSqlWithAliasPartially, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially2, expectedRewrittenSql, originalViewSqlWithAliasPartially, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlFully, expectedRewrittenSql, originalViewSqlWithAliasPartially, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithTablePrefix, expectedRewrittenSql, originalViewSqlWithAliasPartially, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSqlWithAliasFully, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially1, expectedRewrittenSql, originalViewSqlWithAliasFully, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially2, expectedRewrittenSql, originalViewSqlWithAliasFully, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlFully, expectedRewrittenSql, originalViewSqlWithAliasFully, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithTablePrefix, expectedRewrittenSql, originalViewSqlWithAliasFully, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSqlWithTablePrefix, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially1, expectedRewrittenSql, originalViewSqlWithTablePrefix, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially2, expectedRewrittenSql, originalViewSqlWithTablePrefix, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlFully, expectedRewrittenSql, originalViewSqlWithTablePrefix, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithTablePrefix, expectedRewrittenSql, originalViewSqlWithTablePrefix, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testAggregationWithTableAlias()
    {
        String originalViewSql = format("SELECT SUM(a) AS sum_a, b FROM %s GROUP BY b", BASE_TABLE_1);
        String originalViewSqlWithAliasPartially1 = format("SELECT SUM(base1.a) AS sum_a, b FROM %s base1 GROUP BY b", BASE_TABLE_1);
        String originalViewSqlWithAliasPartially2 = format("SELECT SUM(a) AS sum_a, base1.b FROM %s base1 GROUP BY base1.b", BASE_TABLE_1);
        String originalViewSqlWithAliasFully = format("SELECT SUM(base1.a) AS sum_a, base1.b FROM %s base1 GROUP BY base1.b", BASE_TABLE_1);
        String originalViewSqlWithTablePrefix = format("SELECT SUM(%s.a) AS sum_a, %s.b FROM %s GROUP BY %s.b", BASE_TABLE_1, BASE_TABLE_1, BASE_TABLE_1, BASE_TABLE_1);
        String baseQuerySql = format("SELECT SUM(a) AS sum_of_a, b FROM %s GROUP BY b", BASE_TABLE_1);
        String baseQuerySqlWithAliasPartially1 = format("SELECT SUM(base1.a) AS sum_of_a, b FROM %s base1 GROUP BY b", BASE_TABLE_1);
        String baseQuerySqlWithAliasPartially2 = format("SELECT SUM(a) AS sum_of_a, base1.b FROM %s base1 GROUP BY base1.b", BASE_TABLE_1);
        String baseQuerySqlFully = format("SELECT SUM(base1.a) AS sum_of_a, base1.b FROM %s base1 GROUP BY base1.b", BASE_TABLE_1);
        String baseQuerySqlWithTablePrefix = format("SELECT SUM(%s.a) AS sum_of_a, %s.b FROM %s GROUP BY %s.b", BASE_TABLE_1, BASE_TABLE_1, BASE_TABLE_1, BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT SUM(sum_a) AS sum_of_a, b FROM %s GROUP BY b", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially1, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially2, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlFully, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithTablePrefix, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSqlWithAliasPartially1, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially1, expectedRewrittenSql, originalViewSqlWithAliasPartially1, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially2, expectedRewrittenSql, originalViewSqlWithAliasPartially1, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlFully, expectedRewrittenSql, originalViewSqlWithAliasPartially1, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithTablePrefix, expectedRewrittenSql, originalViewSqlWithAliasPartially1, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSqlWithAliasPartially2, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially1, expectedRewrittenSql, originalViewSqlWithAliasPartially2, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially2, expectedRewrittenSql, originalViewSqlWithAliasPartially2, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlFully, expectedRewrittenSql, originalViewSqlWithAliasPartially2, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithTablePrefix, expectedRewrittenSql, originalViewSqlWithAliasPartially2, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSqlWithAliasFully, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially1, expectedRewrittenSql, originalViewSqlWithAliasFully, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially2, expectedRewrittenSql, originalViewSqlWithAliasFully, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlFully, expectedRewrittenSql, originalViewSqlWithAliasFully, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithTablePrefix, expectedRewrittenSql, originalViewSqlWithAliasFully, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSqlWithTablePrefix, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially1, expectedRewrittenSql, originalViewSqlWithTablePrefix, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithAliasPartially2, expectedRewrittenSql, originalViewSqlWithTablePrefix, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlFully, expectedRewrittenSql, originalViewSqlWithTablePrefix, BASE_TABLE_1, VIEW_1);
        assertOptimizedQuery(baseQuerySqlWithTablePrefix, expectedRewrittenSql, originalViewSqlWithTablePrefix, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testWithJoinTables()
    {
        String originalViewSql = format(
                "SELECT %s.a, %s.b FROM %s JOIN %s ON %s.c = %s.c",
                BASE_TABLE_1,
                BASE_TABLE_2,
                BASE_TABLE_1,
                BASE_TABLE_2,
                BASE_TABLE_1,
                BASE_TABLE_2);
        String baseQuerySql = format("SELECT a, c FROM %s base1", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        baseQuerySql = format(
                "SELECT %s.a, %s.b FROM %s JOIN %s ON %s.c = %s.c",
                BASE_TABLE_1,
                BASE_TABLE_2,
                BASE_TABLE_1,
                BASE_TABLE_2,
                BASE_TABLE_1,
                BASE_TABLE_2);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void testFilterContainment()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a >= 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a >= 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 5", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a <> 4", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 5", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 4", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a <> 5", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a >= 5", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a < 3", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 4", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE c > 5", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b = 5.0", BASE_TABLE_7);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 5.0", BASE_TABLE_7);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 5.0", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_7, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b = 'apples'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'apples'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'apples'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'apples'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'apples'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b > 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b > 'banana'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b > 'apples'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b > 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b > 'banana'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b > '122'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b > '123'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b > '123'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'apples'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b > 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b > 'banana'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b = 'apples'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_6, VIEW_1);
    }

    @Test
    public void testFilterContainmentWithAnd()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 0", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 AND a > 0", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5 AND a > 0", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 AND b = 7", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5 AND b = 7", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5 AND c = 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 AND b = 7 AND c = 9", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5 AND b = 7 AND c = 9", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3 AND a < 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 5 AND a < 7", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 5 AND a < 7", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a < 5 AND b > 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 3 AND b > 11", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 3 AND b > 11", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a < 5 AND b > 7 AND c <> 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 3 AND b > 9 AND c = 11", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 3 AND b > 9 AND c = 11", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a <> 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 5 AND a > 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 5 AND a > 5", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE a < 9 AND b > 3.0", BASE_TABLE_7);
        baseQuerySql = format("SELECT a, b FROM %s WHERE a < 7 AND b = 3.1", BASE_TABLE_7);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE a < 7 AND b = 3.1", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_7, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'apples' AND b <> 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b <> 'apples' AND b <> 'banana'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE a > 6 AND b <> 'banana'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE a = 8 AND b = 'apples'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE a = 8 AND b = 'apples'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b = 'orange'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'apples' AND b <> 'banana'", BASE_TABLE_6);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_6, VIEW_1);
    }

    @Test
    public void testFilterContainmentWithOr()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 7", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a <> 7", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a >= 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a <> 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 5 OR a > 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 5 OR a > 5", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3 OR a < 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 5 OR a < 7", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 5 OR a < 7", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a < 3 OR a > 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 1 OR a > 11", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 1 OR a > 11", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 3 OR a > 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 9 OR a = 3", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 9 OR a = 3", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a < 3 OR b > 9", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 1 OR b > 11", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 1 OR b > 11", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 3 AND a < 9 OR a > 10", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a > 5 AND a < 7 OR a > 11", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a > 5 AND a < 7 OR a > 11", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 2.91", BASE_TABLE_7);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <= 2.9 AND b >= 3.0", BASE_TABLE_7);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b <= 2.9 AND b >= 3.0", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_7, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'orange'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'apples' OR b = 'banana'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s  WHERE b = 'apples' OR b = 'banana'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 OR b = 6", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5 OR a = 6", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'apples'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'apples' OR b <> 'banana'", BASE_TABLE_6);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'orange'", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b <> 'apples' OR b <> 'banana'", BASE_TABLE_6);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_6, VIEW_1);
    }

    @Test
    public void testFilterContainmentWithIn()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 5", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (4,5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (3,4,5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (3,5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (3,5)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a >= 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a <> 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (4,6)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (4,6)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (4,5) AND a IN (5,6,7)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (4,5) OR a IN (6,7)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (4,5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (3,5) AND a IN (5,6)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a IN (3,5) AND a IN (5,6)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a NOT IN (4,5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (4,5)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a > 5 OR a < 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5)", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5,6) AND b IN (6,8)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a < 5 AND b = 8", BASE_TABLE_1);
        expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a < 5 AND b = 8", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b IN ('USA','CAN')", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'CAN' OR b = 'USA'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'CAN' OR b = 'USA'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b FROM %s WHERE b NOT IN ('USA','CAN')", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'ABC'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'ABC'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_6, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 5", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (5,6,7)", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a IN (5,6)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 7", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5,6)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a <= 5", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5,6)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a NOT IN (6,7)", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);

        originalViewSql = format("SELECT a, b, c FROM %s WHERE a NOT IN (5,6)", BASE_TABLE_1);
        baseQuerySql = format("SELECT a, b, c FROM %s WHERE a IN (6,7)", BASE_TABLE_1);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void singleSubquerySingleCompatibleView()
    {
        String subquery = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String originalViewSql = subquery;

        String baseQuerySql = format("SELECT a, b FROM (%s)", subquery);
        String expectedRewrittenSql = format("SELECT a, b FROM (SELECT a, b FROM (%s))", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void singleSubquerySingleIncompatibleView()
    {
        String subquery = format("SELECT a, b, c FROM %s", BASE_TABLE_1);
        String originalViewSql = format("SELECT a, b FROM %s", BASE_TABLE_1);

        String baseQuerySql = format("SELECT a, b FROM (%s)", subquery);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void multipleViewsSameBaseTableAllCompatible()
    {
        String subquery1 = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT c FROM %s", BASE_TABLE_1);
        String viewSql1 = subquery1;
        String viewSql2 = subquery2;

        String baseQuerySql = format("SELECT a, b, c FROM (%s) UNION ALL (%s)", subquery1, subquery2);
        String expectedRewrittenSql = format("SELECT a, b, c FROM " +
                        "(SELECT a, b FROM (%s)) " +
                        "UNION ALL " +
                        "(SELECT c FROM (%s))",
                VIEW_1, VIEW_2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1, VIEW_2, viewSql2)));
    }

    @Test
    public void multipleViewsSameBaseTableNoneCompatible()
    {
        String subquery1 = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT b, c FROM %s", BASE_TABLE_1);
        String viewSql1 = format("SELECT a FROM %s", BASE_TABLE_1);
        String viewSql2 = format("SELECT b FROM %s", BASE_TABLE_1);

        String baseQuerySql = format("SELECT a, b, c FROM (%s) UNION ALL (%s)", subquery1, subquery2);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, ImmutableMap.of(BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1, VIEW_2, viewSql2)));
    }

    @Test
    public void multipleViewsSameBaseTableSomeCompatible()
    {
        String subquery1 = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT b, c FROM %s", BASE_TABLE_1);
        String viewSql1 = subquery1;
        String viewSql2 = format("SELECT c FROM %s", BASE_TABLE_1);

        String baseQuerySql = format("SELECT a, b, c FROM (%s) UNION ALL (%s)", subquery1, subquery2);
        String expectedRewrittenSql = format("SELECT a, b, c FROM (SELECT a, b FROM (%s)) UNION ALL (%s)",
                VIEW_1, subquery2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1, VIEW_2, viewSql2)));
    }

    @Test
    public void multipleSubqueriesDifferentBaseTablesSomeCompatible()
    {
        String subquery1 = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT a, b, c FROM %s", BASE_TABLE_2);
        String viewSql1 = subquery1;
        String viewSql2 = format("SELECT a, b FROM %s", BASE_TABLE_2);

        String baseQuerySql = format("SELECT a, b, c FROM (%s) UNION ALL (%s)", subquery1, subquery2);
        String expectedRewrittenSql = format("SELECT a, b, c FROM (SELECT a, b FROM (%s)) UNION ALL (%s)",
                VIEW_1, subquery2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1, VIEW_2, viewSql2)));
    }

    @Test
    public void multipleSubqueriesOptimizableFromSameView()
    {
        String subquery1 = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT c FROM %s", BASE_TABLE_1);
        String originalViewSql = format("SELECT a, b, c FROM %s", BASE_TABLE_1);

        String baseQuerySql = format("SELECT a, b, c FROM (%s) UNION ALL (%s)", subquery1, subquery2);
        String expectedRewrittenSql = format("SELECT a, b, c FROM " +
                        "(SELECT a, b FROM (%s)) " +
                        "UNION ALL " +
                        "(SELECT c FROM (%s))",
                VIEW_1, VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void singleInvalidSubquery()
    {
        String subquery1 = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT c FROM %s", BASE_TABLE_1);
        String viewSql1 = subquery1;
        String viewSql2 = format("%s WHERE c > 5", subquery2);

        String baseQuerySql = format("SELECT a, b, c FROM (%s) UNION ALL (%s)", subquery1, subquery2);
        String expectedRewrittenSql = format("SELECT a, b, c FROM " +
                        "(SELECT a, b FROM (%s)) UNION ALL (%s)",
                VIEW_1, subquery2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1, VIEW_2, viewSql2)));
    }

    @Test
    public void multipleInvalidSubqueries()
    {
        String subquery1 = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT c FROM %s", BASE_TABLE_1);
        String viewSql1 = format("SELECT a, sum(b) FROM %s GROUP BY a", BASE_TABLE_1);
        String viewSql2 = format("%s WHERE c > 5", subquery2);

        String baseQuerySql = format("SELECT a, b, c FROM (%s) UNION ALL (%s)", subquery1, subquery2);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, ImmutableMap.of(BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1, VIEW_2, viewSql2)));
    }

    @Test
    public void joinWithSomeValidSubqueries()
    {
        String subquery1 = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT c, d, e FROM %s", BASE_TABLE_2);
        String viewSql1 = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String viewSql2 = format("SELECT c, d FROM %s", BASE_TABLE_2);

        String baseQuerySql = format("SELECT s1.a, s1.b, s2.c, s2.d, s2.e FROM (%s) s1 INNER JOIN (%s) s2 ON s1.a = s2.c", subquery1, subquery2);
        String expectedRewrittenSql = format("SELECT s1.a, s1.b, s2.c, s2.d, s2.e FROM (SELECT a, b FROM (%s)) s1 INNER JOIN (%s) s2 ON s1.a = s2.c",
                VIEW_1, subquery2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1, VIEW_2, viewSql2)));
    }

    @Test
    public void nestedSubqueries()
    {
        String subquery1 = format("SELECT a, b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT c FROM %s", BASE_TABLE_2);
        String subquery3 = format("SELECT d, e from %s", BASE_TABLE_3);

        String viewSql1 = subquery1;
        String viewSql2 = subquery2;
        String viewSql3 = format("SELECT d FROM %s", BASE_TABLE_3);

        String nestedJoin = format("SELECT c, d, e FROM (%s) s2 INNER JOIN (%s) s3 ON s2.c = s3.d", subquery2, subquery3);

        String baseQuerySql = format("SELECT a, b, c, d, e FROM (%s) s1 INNER JOIN (%s) nested_join ON s1.a = nested_join.c", subquery1, nestedJoin);
        String expectedRewrittenSql = format("SELECT a, b, c, d, e FROM (SELECT a, b FROM (%s)) s1 INNER JOIN " +
                        "(SELECT c, d, e FROM (SELECT c FROM (%s)) s2 INNER JOIN (%s) s3 on s2.c = s3.d) nested_join " + //select from view 2 and 3
                        "ON s1.a = nested_join.c",
                VIEW_1, VIEW_2, subquery3);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2),
                BASE_TABLE_3, ImmutableMap.of(VIEW_3, viewSql3)));
    }

    @Test
    public void subqueryAggregationSupportedFunction()
    {
        String subquery1 = format("SELECT COUNT(a) AS count_a1, b FROM %s GROUP BY b", BASE_TABLE_1);
        String subquery2 = format("SELECT COUNT(a) AS count_a2, c FROM %s GROUP BY c", BASE_TABLE_2);

        String viewSql1 = subquery1;
        String viewSql2 = subquery2;

        String baseQuerySql = format("SELECT GREATEST(count_a1, count_a2) AS bigcount FROM (%s) s1 INNER JOIN (%s) s2 ON s1.b = s2.c", subquery1, subquery2);

        String expectedRewrittenSql = format("SELECT GREATEST(count_a1, count_a2) AS bigcount FROM " +
                        "(SELECT SUM(count_a1) AS count_a1, b FROM %s GROUP BY b) s1 " +
                        "INNER JOIN " +
                        "(SELECT SUM(count_a2) AS count_a2, c FROM %s GROUP BY c) s2 " +
                        "ON s1.b = s2.c",
                VIEW_1, VIEW_2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2)));

        subquery1 = format("SELECT MIN(b) AS min_b FROM %s", BASE_TABLE_1);
        subquery2 = format("SELECT MIN(c) AS min_c FROM %s", BASE_TABLE_2);

        viewSql1 = subquery1;
        viewSql2 = subquery2;

        baseQuerySql = format("SELECT min_b AS miny_b FROM (%s) s1 INNER JOIN (%s) s2 ON s1.min_b = s2.min_c", subquery1, subquery2);

        expectedRewrittenSql = format("SELECT min_b AS miny_b FROM " +
                        "(SELECT MIN(min_b) AS min_b FROM %s) s1 \n" +
                        "INNER JOIN " +
                        "(SELECT MIN(min_c) AS min_c FROM %s) s2 \n" +
                        "ON s1.min_b = s2.min_c",
                VIEW_1, VIEW_2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2)));
    }

    @Test
    public void subqueryAggregationUnsupportedFunction()
    {
        String subquery1 = format("SELECT GEOMETRIC_MEAN(b) AS mean_b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT GEOMETRIC_MEAN(c) AS mean_c FROM %s", BASE_TABLE_2);

        String viewSql1 = subquery1;
        String viewSql2 = subquery2;

        String baseQuerySql = format("SELECT mean_b AS meany_b FROM (%s) s1 INNER JOIN (%s) s2 ON s1.mean_b = s2.mean_c", subquery1, subquery2);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2)));
    }

    @Test
    public void subqueryAggregationSomeSupportedFunctions()
    {
        String subquery1 = format("SELECT min(b) AS min_b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT GEOMETRIC_MEAN(c) AS mean_c FROM %s", BASE_TABLE_2);

        String viewSql1 = subquery1;
        String viewSql2 = subquery2;

        String baseQuerySql = format("SELECT min_b AS miny_b FROM (%s) s1 INNER JOIN (%s) s2 ON s1.min_b = s2.mean_c", subquery1, subquery2);

        String expectedRewrittenSql = format("SELECT min_b AS miny_b FROM " +
                        "(SELECT MIN(min_b) AS min_b FROM %s) s1 " +
                        "INNER JOIN " +
                        "(%s) s2 " +
                        "ON s1.min_b = s2.mean_c",
                VIEW_1, subquery2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2)));
    }

    @Test
    public void subqueryJoinAggregates()
    {
        String subquery1 = format("SELECT c, sum(b) AS sum_b FROM %s GROUP BY c", BASE_TABLE_1);
        String subquery2 = format("SELECT c, sum(a) AS sum_a FROM %s GROUP BY c", BASE_TABLE_2);

        String viewSql1 = subquery1;
        String viewSql2 = subquery2;

        String baseQuerySql = format("SELECT sum_b+sum_a AS sum_all FROM (%s) s1 INNER JOIN (%s) s2 ON s1.c = s2.c", subquery1, subquery2);

        String expectedRewrittenSql = format("SELECT sum_b+sum_a AS sum_all FROM " +
                        "(SELECT c, sum(sum_b) AS sum_b FROM %s GROUP BY c) s1 " +
                        "INNER JOIN " +
                        "(SELECT c, sum(sum_a) AS sum_a FROM %s GROUP BY c) s2 " +
                        "ON s1.c = s2.c",
                VIEW_1, VIEW_2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2)));
    }

    @Test
    public void subqueryJoinAggregatesIncompatibleGroupBy()
    {
        String subquery1 = format("SELECT c, a, sum(b) AS sum_b FROM %s GROUP BY c, a", BASE_TABLE_1);
        String subquery2 = format("SELECT c, b, sum(a) AS sum_a FROM %s GROUP BY c, b", BASE_TABLE_2);

        String viewSql1 = format("SELECT c, sum(b) AS sum_b FROM %s GROUP BY c", BASE_TABLE_1);
        String viewSql2 = format("SELECT c, sum(a) AS sum_a FROM %s GROUP BY c", BASE_TABLE_2);

        String baseQuerySql = format("SELECT sum_b+sum_a AS sum_all FROM (%s) s1 INNER JOIN (%s) s2 ON s1.c = s2.c", subquery1, subquery2);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2)));
    }

    @Test
    public void subqueryJoinFilter()
    {
        String subquery1 = format("SELECT a, b FROM %s WHERE b > 5", BASE_TABLE_1);
        String subquery2 = format("SELECT c, b FROM %s WHERE b > 5", BASE_TABLE_2);

        String viewSql1 = format("SELECT a, b FROM %s WHERE b > 5", BASE_TABLE_1);
        String viewSql2 = format("SELECT c, b FROM %s WHERE b > 5", BASE_TABLE_2);

        String baseQuerySql = format(
                "SELECT s1.a, s1.b, s2.b, s2.c FROM" +
                        "(SELECT a, b FROM (%s) WHERE b > 5) s1 " +
                        "INNER JOIN " +
                        "(SELECT c, b FROM (%s) WHERE b > 5) s2 " +
                        "ON s1.b = s2.b",
                subquery1, subquery2);

        String expectedRewrittenSql = format(
                "SELECT s1.a, s1.b, s2.b, s2.c FROM" +
                        "(SELECT a, b FROM " +
                        "(SELECT a, b FROM %s WHERE b > 5) WHERE b > 5) s1 " +
                        "INNER JOIN " +
                        "(SELECT c, b FROM " +
                        "(SELECT c, b FROM (%s) WHERE b > 5) WHERE b > 5) s2 " +
                        "ON s1.b = s2.b",
                VIEW_1, VIEW_2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2)));
    }

    @Test
    public void subqueryJoinIncompatibleFilter()
    {
        String subquery1 = format("SELECT a, b FROM %s WHERE b > 5", BASE_TABLE_1);
        String subquery2 = format("SELECT c, b FROM %s WHERE b > 5", BASE_TABLE_2);

        String viewSql1 = format("SELECT a, b FROM %s WHERE b > 6", BASE_TABLE_1);
        String viewSql2 = format("SELECT c, b FROM %s WHERE b > 6", BASE_TABLE_2);

        String baseQuerySql = format("SELECT s1.a, s1.b, s2.b, s2.c FROM (%s) s1 INNER JOIN (%s) s2 ON s1.b = s2.b", subquery1, subquery2);

        assertOptimizedQuery(baseQuerySql, baseQuerySql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2)));
    }

    @Test
    public void subqueryFilterGroupBy()
    {
        String subquery = format("SELECT a, b, sum(c) AS sum_c FROM %s WHERE b > 5 GROUP BY a, b", BASE_TABLE_1);

        String originalViewSql = subquery;

        String baseQuerySql = format("SELECT a, b, sum(c) AS sum_c FROM (%s)", subquery);

        String expectedRewrittenSql = format("SELECT a, b, sum(c) AS sum_c FROM " +
                        "(SELECT a, b, sum(sum_c) AS sum_c FROM %s WHERE b > 5 GROUP BY a, b)",
                VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, originalViewSql, BASE_TABLE_1, VIEW_1);
    }

    @Test
    public void subqueryMultipleFiltersGroupBys()
    {
        String subquery1 = format("SELECT a, b, sum(c) AS sum_c FROM %s WHERE b > 5 AND a = 3 GROUP BY a, b", BASE_TABLE_1);
        String subquery2 = format("SELECT a, b, sum(d) AS sum_d FROM %s WHERE b >= 5 AND a <> 3 GROUP BY a, b", BASE_TABLE_2);

        String viewSql1 = subquery1;
        String viewSql2 = subquery2;

        String baseQuerySql = format(
                "SELECT s1.a, s1.b, sum_c, s2.a, s2.b, sum_d FROM" +
                        "(%s) s1 " +
                        "INNER JOIN " +
                        "(%s) s2 " +
                        "ON s1.b = s2.b",
                subquery1, subquery2);

        String expectedRewrittenSql = format(
                "SELECT s1.a, s1.b, sum_c, s2.a, s2.b, sum_d FROM" +
                        "(SELECT a, b, sum(sum_c) AS sum_c FROM (%s) WHERE b > 5 AND a = 3 GROUP BY a, b) s1 " +
                        "INNER JOIN " +
                        "(SELECT a, b, sum(sum_d) AS sum_d FROM (%s) WHERE b >= 5 AND a <> 3 GROUP BY a, b) s2 " +
                        "ON s1.b = s2.b",
                VIEW_1, VIEW_2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2)));
    }

    @Test
    public void subqueryJoinAggregatesWith()
    {
        String subquery1 = format("SELECT min(b) AS min_b FROM %s", BASE_TABLE_1);
        String subquery2 = format("SELECT GEOMETRIC_MEAN(c) AS mean_c FROM %s", BASE_TABLE_2);

        String viewSql1 = subquery1;
        String viewSql2 = subquery2;

        String baseQuerySql = format("WITH s1 AS (%s) SELECT min_b AS miny_b FROM s1 INNER JOIN (%s) s2 ON s1.min_b = s2.mean_c", subquery1, subquery2);

        String expectedRewrittenSql = format("WITH s1 AS " +
                        "(SELECT MIN(min_b) AS min_b FROM %s) " +
                        "SELECT min_b AS miny_b FROM s1 " +
                        "INNER JOIN " +
                        "(%s) s2 " +
                        "ON s1.min_b = s2.mean_c",
                VIEW_1, subquery2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2)));
    }

    @Test
    public void subqueryInsideWithClause()
    {
        String subquery1 = format("SELECT d, sum(b) as sum_b from %s GROUP BY d", BASE_TABLE_1);
        String subquery2 = format("SELECT a, GEOMETRIC_MEAN(c) AS mean_c FROM %s GROUP BY a", BASE_TABLE_2);

        String viewSql1 = subquery1;
        String viewSql2 = subquery2;

        String baseQuerySql = format("WITH s3 AS ((%s) UNION ALL (%s)) " +
                "SELECT d, sum_b, mean_c, a FROM s3", subquery1, subquery2);

        String expectedRewrittenSql = format("WITH s3 AS(" +
                "(SELECT d, sum(sum_b) AS sum_b FROM (%s) GROUP BY d) " +
                "UNION ALL (%s)) " +
                "SELECT d, sum_b, mean_c, a FROM s3", VIEW_1, subquery2);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(
                BASE_TABLE_1, ImmutableMap.of(VIEW_1, viewSql1),
                BASE_TABLE_2, ImmutableMap.of(VIEW_2, viewSql2)));
    }

    // Some of DNF conversions on (A^~B) might not be successful due to exponential explosion of sub-expressions
    // TODO: Implement method that utilizes external SAT solver libraries. https://github.com/prestodb/presto/issues/16536
    @Test(enabled = false)
    public void testFilterContainmentDisjunctiveNormalForm()
    {
        String originalViewSql = format("SELECT a, b, c FROM %s WHERE a = 1 AND b = 2 OR b = 3 AND c = 4", BASE_TABLE_1);
        String baseQuerySql = format("SELECT a, b, c FROM %s WHERE a = 1 AND b = 2 AND c = 3", BASE_TABLE_1);
        String expectedRewrittenSql = format("SELECT a, b, c FROM %s WHERE a = 1 AND b = 2 AND c = 3", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(BASE_TABLE_1, ImmutableMap.of(VIEW_1, originalViewSql)));

        originalViewSql = format(
                "SELECT a, b, c FROM %s WHERE " +
                        "a = 1 AND b = 2 " +
                        "OR b = 3 AND c = 4 " +
                        "OR a = 5 AND c = 6",
                BASE_TABLE_1);
        baseQuerySql = format(
                "SELECT a, b, c FROM %s WHERE " +
                        "a = 1 AND b = 2 AND c = 3 " +
                        "OR a = 5 AND b = 7 AND c = 6",
                BASE_TABLE_1);
        expectedRewrittenSql = format(
                "SELECT a, b, c FROM %s WHERE " +
                        "a = 1 AND b = 2 AND c = 3 " +
                        "OR a = 5 AND b = 7 AND c = 6",
                VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(BASE_TABLE_1, ImmutableMap.of(VIEW_1, originalViewSql)));
    }

    // Mismatch Domain Type Problem: https://github.com/prestodb/presto/issues/16530
    @Test(enabled = false)
    public void testFilterContainmentWithMismatchStringLength()
    {
        String originalViewSql = format("SELECT a, b FROM %s WHERE b <> 'banana'", BASE_TABLE_6);
        String baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'apple'", BASE_TABLE_6);
        String expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'apple'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(BASE_TABLE_1, ImmutableMap.of(VIEW_1, originalViewSql)));

        originalViewSql = format("SELECT a, b FROM %s WHERE b NOT IN ('USA','CAN')", BASE_TABLE_6);
        baseQuerySql = format("SELECT a, b FROM %s WHERE b = 'UK'", BASE_TABLE_6);
        expectedRewrittenSql = format("SELECT a, b FROM %s WHERE b = 'UK'", VIEW_1);

        assertOptimizedQuery(baseQuerySql, expectedRewrittenSql, ImmutableMap.of(BASE_TABLE_1, ImmutableMap.of(VIEW_1, originalViewSql)));
    }

    private void assertOptimizedQuery(String baseQuerySql, String expectedViewSql, String originalViewSql, String baseTableName, String originalViewName)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(TEST_SESSION, session -> {
                    Query baseQuery = (Query) SQL_PARSER.createStatement(baseQuerySql, createParsingOptions(session));
                    Query expectedViewQuery = (Query) SQL_PARSER.createStatement(expectedViewSql, createParsingOptions(session));

                    metadata.createMaterializedView(
                            session,
                            TPCH_CATALOG,
                            null,
                            createStubConnectorMaterializedViewDefinition(
                                    originalViewName,
                                    originalViewSql,
                                    SESSION_SCHEMA,
                                    ImmutableList.of(new SchemaTableName(SESSION_SCHEMA, baseTableName))),
                            false);

                    Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer(
                            metadata,
                            session,
                            SQL_PARSER,
                            accessControl,
                            domainTranslator)
                            .process(baseQuery);
                    assertEquals(optimizedBaseToViewQuery, expectedViewQuery);

                    metadata.dropMaterializedView(session, QualifiedObjectName.valueOf(TPCH_CATALOG, SESSION_SCHEMA, baseTableName));
                });
    }

    private void assertOptimizedQuery(String baseQuerySql, String expectedQueryAfterOptimization, Map<String, Map<String, String>> viewQueries)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(TEST_SESSION, session -> {
                    Query baseQuery = (Query) SQL_PARSER.createStatement(baseQuerySql, createParsingOptions(session));
                    Query expectedViewQuery = (Query) SQL_PARSER.createStatement(expectedQueryAfterOptimization, createParsingOptions(session));

                    List<QualifiedObjectName> createdMaterializedViews = new ArrayList<>();

                    for (Map.Entry<String, Map<String, String>> baseToViewMap : viewQueries.entrySet()) {
                        for (Map.Entry<String, String> viewNameToSqlMap : baseToViewMap.getValue().entrySet()) {
                            metadata.createMaterializedView(
                                    session,
                                    TPCH_CATALOG,
                                    null,
                                    createStubConnectorMaterializedViewDefinition(
                                            viewNameToSqlMap.getKey(),
                                            viewNameToSqlMap.getValue(),
                                            SESSION_SCHEMA,
                                            ImmutableList.of(new SchemaTableName(SESSION_SCHEMA, baseToViewMap.getKey()))),
                                    false);

                            createdMaterializedViews.add(QualifiedObjectName.valueOf(TPCH_CATALOG, SESSION_SCHEMA, viewNameToSqlMap.getKey()));
                        }
                    }

                    Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer(
                            metadata,
                            session,
                            SQL_PARSER,
                            accessControl,
                            domainTranslator)
                            .process(baseQuery);
                    assertEquals(optimizedBaseToViewQuery, expectedViewQuery);

                    for (QualifiedObjectName materializedView : createdMaterializedViews) {
                        metadata.dropMaterializedView(session, materializedView);
                    }
                });
    }

    private ConnectorMaterializedViewDefinition createStubConnectorMaterializedViewDefinition(String viewName, String viewSql, String schema, List<SchemaTableName> baseTables)
    {
        return new ConnectorMaterializedViewDefinition(
                viewSql,
                schema,
                viewName,
                baseTables,
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());
    }
}
