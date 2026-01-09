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

import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.AccessControlReferences;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.facebook.presto.util.AnalyzerUtil.checkAccessPermissions;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestViewDefinitionCollector
        extends AbstractAnalyzerTest
{
    public void testSelectLeftJoinViews()
    {
        @Language("SQL") String query = "SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testCreateViewWithNestedViews()
    {
        @Language("SQL") String query = "CREATE VIEW top_level_view1 AS SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testCreateTableAsSelectWithViews()
    {
        @Language("SQL") String query = "CREATE TABLE top_level_view1 AS SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainWithViews()
    {
        @Language("SQL") String query = "EXPLAIN SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainTypeIoWithViews()
    {
        @Language("SQL") String query = "EXPLAIN (TYPE IO) SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainTypeValidateWithViews()
    {
        @Language("SQL") String query = "EXPLAIN (TYPE VALIDATE) SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainAnalyzeWithViews()
    {
        @Language("SQL") String query = "EXPLAIN ANALYZE SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainExplainWithViews()
    {
        @Language("SQL") String query = "EXPLAIN EXPLAIN SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainExplainTypeValidateWithViews()
    {
        @Language("SQL") String query = "EXPLAIN EXPLAIN (TYPE VALIDATE) SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainTypeValidateExplainWithViews()
    {
        @Language("SQL") String query = "EXPLAIN (TYPE VALIDATE) EXPLAIN SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainTypeValidateExplainTypeValidateWithViews()
    {
        @Language("SQL") String query = "EXPLAIN (TYPE VALIDATE) EXPLAIN (TYPE VALIDATE) SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainAnalyzeExplainWithViews()
    {
        @Language("SQL") String query = "EXPLAIN ANALYZE EXPLAIN SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainAnalyzeExplainAnalyzeWithViews()
    {
        @Language("SQL") String query = "EXPLAIN ANALYZE EXPLAIN ANALYZE SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainExplainAnalyzeWithViews()
    {
        @Language("SQL") String query = "EXPLAIN EXPLAIN ANALYZE SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainAnalyzeExplainTypeValidateWithViews()
    {
        @Language("SQL") String query = "EXPLAIN ANALYZE EXPLAIN (TYPE VALIDATE) SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    public void testExplainTypeValidateExplainAnalyzeWithViews()
    {
        @Language("SQL") String query = "EXPLAIN (TYPE VALIDATE) EXPLAIN ANALYZE SELECT view_definer1.a, view_definer1.c, view_invoker2.y FROM view_definer1 left join view_invoker2 on view_invoker2.y = view_definer1.c";

        assertViewDefinitions(query, ImmutableMap.of(
                "tpch.s1.view_invoker2", "select x, y, z from t13",
                "tpch.s1.view_definer1", "select a,b,c from t1"
        ), ImmutableMap.of());
    }

    private void assertViewDefinitions(@Language("SQL") String query, Map<String, String> expectedViewDefinitions, Map<String, String> expectedMaterializedViewDefinitions)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(CLIENT_SESSION, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata, WarningCollector.NOOP, Optional.of(createTestingQueryExplainer(session, accessControl, metadata)), query);
                    Statement statement = SQL_PARSER.createStatement(query);
                    Analysis analysis = analyzer.analyzeSemantic(statement, false);
                    AccessControlReferences accessControlReferences = analysis.getAccessControlReferences();
                    checkAccessPermissions(accessControlReferences, analysis.getViewDefinitionReferences(), query, session.getPreparedStatements(), session.getIdentity(), accessControl, session.getAccessControlContext());

                    Map<String, String> viewDefinitionsMap = analysis.getViewDefinitionReferences().getViewDefinitions().entrySet().stream()
                            .collect(Collectors.toMap(
                                    entry -> entry.getKey().toString(),
                                    entry -> entry.getValue().getOriginalSql()));
                    Map<String, String> materializedDefinitionsMap = analysis.getViewDefinitionReferences().getMaterializedViewDefinitions().entrySet().stream()
                            .collect(Collectors.toMap(
                                    entry -> entry.getKey().toString(),
                                    entry -> entry.getValue().getOriginalSql()));

                    assertEquals(viewDefinitionsMap, expectedViewDefinitions);
                    assertEquals(materializedDefinitionsMap, expectedMaterializedViewDefinitions);
                });
    }
}
