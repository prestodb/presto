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
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestMaterializedViewCandidateValidator
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final String BASE_TABLE = "base_table";

    @Test
    public void testWithSimpleQuery()
    {
        String materializedViewDefinitionSql = format("SELECT a, b FROM %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b FROM %s", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithSubsetInSelect()
    {
        String materializedViewDefinitionSql = format("SELECT a, b FROM %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a FROM %s", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithNonSubsetInSelect()
    {
        String materializedViewDefinitionSql = format("SELECT a, b FROM %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, c FROM %s", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, false);
    }

    @Test
    public void testWithAlias()
    {
        String materializedViewDefinitionSql = format("SELECT a as mv_a, b, c as mv_c FROM %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b, c FROM %s", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithSubsetInGroupBy()
    {
        String materializedViewDefinitionSql = format("SELECT SUM(a) as sum_a, b, c FROM %s GROUP BY b, c", BASE_TABLE);
        String baseQuerySql = format("SELECT SUM(a) as sum_a, b FROM %s GROUP BY b", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithNonSubsetInGroupBy()
    {
        String materializedViewDefinitionSql = format("SELECT SUM(a) as sum_a, b, c FROM %s GROUP BY b, c", BASE_TABLE);
        String baseQuerySql = format("SELECT SUM(a) as sum_a, b FROM %s GROUP BY b, d", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, false);
    }

    @Test
    public void testWithNoGroupByInBaseQuery()
    {
        String materializedViewDefinitionSql = format("SELECT SUM(a) as sum_a, b, c FROM %s GROUP BY b, c", BASE_TABLE);
        String baseQuerySql = format("SELECT b FROM %s", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, false);
    }

    @Test
    public void testWithOrderBy()
    {
        String materializedViewDefinitionSql = format("SELECT a, b, c FROM %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b, c From %s ORDER BY c ASC, b DESC, a", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithDifferentOrderBy()
    {
        String materializedViewDefinitionSql = format("SELECT a, b, c FROM %s ORDER BY a, b, c", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b, c From %s ORDER BY c ASC, b DESC, a", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithExclusiveOrderBy()
    {
        String materializedViewDefinitionSql = format("SELECT a, b, c FROM %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b, c From %s ORDER BY c ASC, b DESC, d", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, false);
    }

    @Test
    public void testWithWhereConditionAndOr()
    {
        String materializedViewDefinitionSql = format("SELECT a, b, c, d From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b From %s where a < 10 and c > 10 or d = '2000-01-01'", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithDerivedFieldsWithAlias()
    {
        String materializedViewDefinitionSql = format("SELECT SUM(a * b + c) as mv_sum, MAX(a * b + c) as mv_max, d as mv_d, e FROM %s group by d, e", BASE_TABLE);
        String baseQuerySql = format("SELECT SUM(a * b + c) as sum_of_abc, MAX(a * b + c) as max_of_abc, d, e from %s group by d, e", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithArithmeticBinary()
    {
        String materializedViewDefinitionSql = format("SELECT a, b, c From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a + b, a * b - c From %s", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithArithmeticBinaryWithAlias()
    {
        String materializedViewDefinitionSql = format("SELECT a as mv_a, b, c as mv_c, d From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a + b, c / d, a * c - b * d From %s", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithWhereCondition()
    {
        String materializedViewDefinitionSql = format("SELECT a, b, c, d From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b From %s where a < 10 and c > 10 or d = '2000-01-01'", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithWhereConditionWithAlias()
    {
        String materializedViewDefinitionSql = format("SELECT a as mv_a, b, c, d as mv_d From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b From %s where a < 10 and c > 10 or d = '2000-01-01'", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithOrderByWithAlias()
    {
        String materializedViewDefinitionSql = format("SELECT a as mv_a, b, c as mv_c From %s", BASE_TABLE);
        String baseQuerySql = format("SELECT a, b, c From %s ORDER BY c ASC, b DESC, a", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithFunctionCallInOrderBy()
    {
        String materializedViewDefinitionSql = format("SELECT MAX(a) as mv_max_a, b From %s GROUP BY b", BASE_TABLE);
        String baseQuerySql = format("SELECT MAX(a), b From %s GROUP BY b ORDER BY MAX(b) DESC, b ASC", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    @Test
    public void testWithHaving()
    {
        String materializedViewDefinitionSql = format("SELECT MAX(id) as mv_max_id, country From %s GROUP BY country", BASE_TABLE);
        String baseQuerySql = format("SELECT MAX(id), country " +
                "From %s " +
                "WHERE country <> 'USA' " +
                "GROUP BY country " +
                "HAVING MAX(id) <=15 " +
                "ORDER BY MAX(id) DESC", BASE_TABLE);

        assertOptimizedQuery(materializedViewDefinitionSql, baseQuerySql, true);
    }

    private void assertOptimizedQuery(String materializedViewDefinition, String baseQuerySql, boolean expectedValidateBoolean)
    {
        Query materializedViewDefinitionQuery = (Query) SQL_PARSER.createStatement(materializedViewDefinition);
        Query baseQuery = (Query) SQL_PARSER.createStatement(baseQuerySql);

        MaterializedViewInformationExtractor materializedViewInformationExtractor = new MaterializedViewInformationExtractor();
        materializedViewInformationExtractor.process(materializedViewDefinitionQuery);

        ImmutableSet<String> materializedViewDefinitionColumns = materializedViewInformationExtractor.getMaterializedViewColumns();
        Optional<ImmutableSet<GroupingElement>> materializedViewDefinitionGroupBy = materializedViewInformationExtractor.getMaterializedViewGroupBy();

        MaterializedViewCandidateValidator materializedViewCandidateValidator = new MaterializedViewCandidateValidator(materializedViewDefinitionColumns, materializedViewDefinitionGroupBy);
        materializedViewCandidateValidator.process(baseQuery);

        assertEquals(materializedViewCandidateValidator.isConvertible(), expectedValidateBoolean);
    }
}
