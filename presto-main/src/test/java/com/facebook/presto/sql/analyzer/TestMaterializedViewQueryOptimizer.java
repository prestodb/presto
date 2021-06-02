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
    @Test
    public void testQueryOptimizationUsingMaterializedView()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT a, b From %s", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT a, b From %s", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT a, b From %s", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithAlias()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c From %s", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT a, b, c From %s", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT mv_a, b, mv_c From %s", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithAliasResult()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c, d From %s", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT a as result_a, b as result_b, c, d From %s", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT mv_a as result_a, b as result_b, mv_c, d From %s", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithAllColumnsSelect()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT * From %s", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT * From %s", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT * From %s", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithDerivedFields()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT SUM(a * b + c) as mv_sum, " +
                "MAX(a * b + c) as mv_max, " +
                "d, " +
                "e FROM %s group by d, e", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT SUM(a * b + c) as sum, " +
                "MAX(a * b + c) as max, " +
                "d, " +
                "e from %s group by d, e", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT SUM(mv_sum) as sum, " +
                "MAX(mv_max) as max, " +
                "d, " +
                "e FROM %s group by d, e", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithDerivedFieldsWithAlias()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT SUM(a * b + c) as mv_sum, " +
                "MAX(a * b + c) as mv_max, " +
                "d as mv_d, " +
                "e FROM %s group by d, e", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT SUM(a * b + c) as sum, " +
                "MAX(a * b + c) as max, " +
                "d, " +
                "e from %s group by d, e", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT SUM(mv_sum) as sum, " +
                "MAX(mv_max) as max, " +
                "mv_d, " +
                "e FROM %s group by mv_d, e", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithArithmeticBinary()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT a, b, c From %s", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT a + b, a * b - c From %s", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT a + b, a * b - c From %s", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithArithmeticBinaryWithAlias()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c, d From %s", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT a + b, c / d, a * c - b * d From %s", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT mv_a + b, mv_c / d, mv_a * mv_c - b * d From %s", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithWhereCondition()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT a, b, c, d From %s", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT a, b From %s where a < 10 and c > 10 or d = '2000-01-01'", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT a, b From %s where a < 10 and c > 10 or d = '2000-01-01'", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithWhereConditionWithAlias()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT a as mv_a, b, c, d as mv_d From %s", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT a, b From %s where a < 10 and c > 10 or d = '2000-01-01'", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT mv_a, b From %s where mv_a < 10 and c > 10 or mv_d = '2000-01-01'", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithOrderBy()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT a, b, c From %s", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT a, b, c From %s ORDER BY c ASC, b DESC, a", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT a, b, c From %s ORDER BY c ASC, b DESC, a", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithOrderByWithAlias()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT a as mv_a, b, c as mv_c From %s", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT a, b, c From %s ORDER BY c ASC, b DESC, a", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT mv_a, b, mv_c From %s ORDER BY mv_c ASC, b DESC, mv_a", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithFunctionCallInOrderBy()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT COUNT(a) as mv_count_a, b From %s GROUP BY b", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT COUNT(a), b From %s GROUP BY b ORDER BY COUNT(a) DESC, b ASC", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT COUNT(mv_count_a), b From %s GROUP BY b ORDER BY COUNT(mv_count_a) DESC, b ASC", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }

    @Test
    public void testQueryOptimizationUsingMaterializedViewWithHaving()
    {
        SqlParser sqlParser = new SqlParser();
        String baseTable = "base_table";
        String view = "view";

        // Definition of materialized view
        String originalViewSql = format("SELECT COUNT(id) as mv_count_id, country From %s GROUP BY country", baseTable);
        Query originalViewQuery = (Query) sqlParser.createStatement(originalViewSql);

        String baseQuerySql = format("SELECT COUNT(id), country From %s WHERE country <> 'USA' GROUP BY country HAVING COUNT(id) <=15 ORDER BY COUNT(id) DESC", baseTable);
        Query baseQuery = (Query) sqlParser.createStatement(baseQuerySql);

        Table viewTable = new Table(QualifiedName.of(view));

        Query optimizedBaseToViewQuery = (Query) new MaterializedViewQueryOptimizer()
                .process(baseQuery, new MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext(viewTable, originalViewQuery));

        String expectedViewSql = format("SELECT COUNT(mv_count_id), country From %s WHERE country <> 'USA' GROUP BY country HAVING COUNT(mv_count_id) <=15 ORDER BY COUNT(mv_count_id) DESC", view);
        Query expectedViewQuery = (Query) sqlParser.createStatement(expectedViewSql);

        assertEquals(optimizedBaseToViewQuery, expectedViewQuery);
    }
}
