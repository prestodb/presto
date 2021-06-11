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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Table;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestMaterializedViewQueryOptimizer
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final String BASE_TABLE = "base_table";
    private static final String BASE_TABLE2 = "base_table2";
    private static final String VIEW = "view";

    @Test
    public void testWithSimpleQuery()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT a, b From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b From %s", BASE_TABLE);
        String expectedViewSql = format("SELECT a, b From %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithAlias()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b, c From %s", BASE_TABLE);
        String expectedViewSql = format("SELECT mv_a, b, mv_c From %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithAliasInBaseQuery()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c, d From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a as result_a, b as result_b, c, d From %s", BASE_TABLE);
        String expectedViewSql = format("SELECT mv_a as result_a, b as result_b, mv_c, d From %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithAllColumnsSelect()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT * From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT * From %s", BASE_TABLE);
        String expectedViewSql = format("SELECT * From %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithDerivedFields()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT SUM(a * b + c) as mv_sum, MAX(a * b + c) as mv_max, d, e FROM %s group by d, e", BASE_TABLE);
        String baseQuerySql = format("SELECT SUM(a * b + c), MAX(a * b + c), d, e from %s group by d, e", BASE_TABLE);
        String expectedViewSql = format("SELECT SUM(mv_sum), MAX(mv_max), d, e FROM %s group by d, e", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithDerivedFieldsWithAlias()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT SUM(a * b + c) as mv_sum, MAX(a * b + c) as mv_max, d as mv_d, e FROM %s group by d, e", BASE_TABLE);
        String baseQuerySql = format("SELECT SUM(a * b + c) as sum_of_abc, MAX(a * b + c) as max_of_abc, d, e from %s group by d, e", BASE_TABLE);
        String expectedViewSql = format("SELECT SUM(mv_sum) as sum_of_abc, MAX(mv_max) as max_of_abc, mv_d, e FROM %s group by mv_d, e", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithArithmeticBinary()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT a, b, c From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a + b, a * b - c From %s", BASE_TABLE);
        String expectedViewSql = format("SELECT a + b, a * b - c From %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithArithmeticBinaryWithAlias()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c, d From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a + b, c / d, a * c - b * d From %s", BASE_TABLE);
        String expectedViewSql = format("SELECT mv_a + b, mv_c / d, mv_a * mv_c - b * d From %s", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithWhereCondition()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT a, b, c, d From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b From %s where a < 10 and c > 10 or d = '2000-01-01'", BASE_TABLE);
        String expectedViewSql = format("SELECT a, b From %s where a < 10 and c > 10 or d = '2000-01-01'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithWhereConditionWithAlias()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT a as mv_a, b, c, d as mv_d From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b From %s where a < 10 and c > 10 or d = '2000-01-01'", BASE_TABLE);
        String expectedViewSql = format("SELECT mv_a, b From %s where mv_a < 10 and c > 10 or mv_d = '2000-01-01'", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithOrderBy()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT a, b, c From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b, c From %s ORDER BY c ASC, b DESC, a", BASE_TABLE);
        String expectedViewSql = format("SELECT a, b, c From %s ORDER BY c ASC, b DESC, a", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithOrderByWithAlias()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b, c From %s ORDER BY c ASC, b DESC, a", BASE_TABLE);
        String expectedViewSql = format("SELECT mv_a, b, mv_c From %s ORDER BY mv_c ASC, b DESC, mv_a", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithFunctionCallInOrderBy()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT MAX(a) as mv_max_a, b From %s GROUP BY b", BASE_TABLE);
        String baseQuerySql = format("SELECT MAX(a), b From %s GROUP BY b ORDER BY MAX(a) DESC, b ASC", BASE_TABLE);
        String expectedViewSql = format("SELECT MAX(mv_max_a), b From %s GROUP BY b ORDER BY MAX(mv_max_a) DESC, b ASC", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithHaving()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT MAX(id) as mv_max_id, country From %s GROUP BY country", BASE_TABLE);
        String baseQuerySql = format("SELECT MAX(id), country " +
                "From %s " +
                "WHERE country <> 'USA' " +
                "GROUP BY country " +
                "HAVING MAX(id) <=15 " +
                "ORDER BY MAX(id) DESC", BASE_TABLE);
        String expectedViewSql = format("SELECT MAX(mv_max_id), country " +
                "From %s " +
                "WHERE country <> 'USA' " +
                "GROUP BY country " +
                "HAVING MAX(mv_max_id) <=15 " +
                "ORDER BY MAX(mv_max_id) DESC", VIEW);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithNoMatchingMaterializedView()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT a, b From %s", BASE_TABLE2);
        String baseQuerySql = format("SELECT a, b From %s", BASE_TABLE);
        String expectedViewSql = format("SELECT a, b From %s", BASE_TABLE);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    @Test
    public void testWithNoMatchingColumnNames()
    {
        // Definition of materialized view
        String originalViewSql = format("SELECT a, b, c From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT c, d From %s", BASE_TABLE);
        String expectedViewSql = format("SELECT c, d From %s", BASE_TABLE);

        assertOptimizedQuery(originalViewSql, baseQuerySql, expectedViewSql);
    }

    private void assertOptimizedQuery(String originalViewSql, String baseQuerySql, String expectedViewSql)
    {
        Table viewTable = new Table(QualifiedName.of(VIEW));

        Query originalViewQuery = (Query) SQL_PARSER.createStatement(originalViewSql);
        Query baseQuery = (Query) SQL_PARSER.createStatement(baseQuerySql);
        Query expectedViewQuery = (Query) SQL_PARSER.createStatement(expectedViewSql);

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer(viewTable, originalViewQuery).rewrite(baseQuery);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }
}
