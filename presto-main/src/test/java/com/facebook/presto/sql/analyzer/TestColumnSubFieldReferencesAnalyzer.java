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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.CHECK_ACCESS_CONTROL_ON_UTILIZED_COLUMNS_ONLY;
import static com.facebook.presto.SystemSessionProperties.CHECK_ACCESS_CONTROL_WITH_SUBFIELDS;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestColumnSubFieldReferencesAnalyzer
        extends AbstractAnalyzerTest
{
    @Test
    public void testSelect()
    {
        assertTableColumns("SELECT * FROM tpch.s1.t10",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("a", "b", "c")));

        assertTableColumns("SELECT a, b FROM tpch.s1.t10",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("a", "b")));

        assertTableColumns("SELECT b.w, b.x.y FROM tpch.s1.t10",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("b.w", "b.x.y")));

        assertTableColumns("SELECT b, b.x FROM tpch.s1.t10",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("b", "b.x")));

        assertTableColumns("SELECT b[2][1], b[2].z FROM tpch.s1.t10",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("b.x.y", "b.x.z")));

        assertTableColumns("SELECT b.x[0][6].y FROM tpch.s1.t11",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t11"), ImmutableSet.of("b.x.y")));

        assertTableColumns("SELECT b[2][0][6].y FROM tpch.s1.t11",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t11"), ImmutableSet.of("b.x.y")));

        assertTableColumns("SELECT b[2][0][6][1] FROM tpch.s1.t11",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t11"), ImmutableSet.of("b.x.y")));

        assertTableColumns("SELECT b.x[0][6] FROM tpch.s1.t11",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t11"), ImmutableSet.of("b.x")));

        assertTableColumns("SELECT b[2][0][6] FROM tpch.s1.t11",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t11"), ImmutableSet.of("b.x")));

        assertTableColumns("SELECT a[0].x FROM tpch.s1.t11",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t11"), ImmutableSet.of("a.x")));

        assertTableColumns("SELECT a[0] FROM tpch.s1.t11",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t11"), ImmutableSet.of("a")));
    }

    @Test
    public void testSelectWithAlias()
    {
        assertTableColumns("SELECT g.b, g.b.x FROM tpch.s1.t10 g",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("b", "b.x")));

        assertTableColumns("SELECT g.b AS col0, g.b.x AS col1 FROM tpch.s1.t10 g",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("b", "b.x")));

        assertTableColumns("SELECT tpch.s1.t10.b, t10.b.x FROM tpch.s1.t10",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("b", "b.x")));
    }

    @Test
    public void testNonSelect()
    {
        assertTableColumns("SELECT b.w FROM tpch.s1.t10 WHERE b.x.y = 1",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("b.w", "b.x.y")));

        assertTableColumns("SELECT count(*) FROM tpch.s1.t10 GROUP BY b.x.y",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("b.x.y")));

        assertTableColumns("SELECT b.x FROM tpch.s1.t10 GROUP BY 1",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("b.x")));
    }

    @Test
    public void testUtillized()
    {
        // TODO: Properly support utilized column check. Correct output should be just "b.x"
        assertUtilizedTableColumns("SELECT b.x FROM (SELECT * from tpch.s1.t10)",
                ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t10"), ImmutableSet.of("b", "b.x")));
    }

    private void assertTableColumns(@Language("SQL") String query, Map<QualifiedObjectName, Set<String>> expected, boolean utilized)
    {
        Session session = testSessionBuilder()
                .setCatalog(TPCH_CATALOG)
                .setSchema("s1")
                .setSystemProperty(CHECK_ACCESS_CONTROL_WITH_SUBFIELDS, "true")
                .setSystemProperty(CHECK_ACCESS_CONTROL_ON_UTILIZED_COLUMNS_ONLY, utilized ? "true" : "false")
                .build();
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(session, s -> {
                    Analyzer analyzer = createAnalyzer(s, metadata, WarningCollector.NOOP);
                    Statement statement = SQL_PARSER.createStatement(query);
                    Analysis analysis = analyzer.analyze(statement);
                    assertEquals(analysis.getTableColumnWithSubfieldReferencesForAccessControl(session).values().stream().findFirst().get(), expected);
                });
    }

    private void assertTableColumns(@Language("SQL") String query, Map<QualifiedObjectName, Set<String>> expected)
    {
        assertTableColumns(query, expected, false);
    }

    private void assertUtilizedTableColumns(@Language("SQL") String query, Map<QualifiedObjectName, Set<String>> expected)
    {
        assertTableColumns(query, expected, true);
    }
}
