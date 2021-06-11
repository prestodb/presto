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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;

public class TestMaterializedViewCandidateExtractor
{
    private static final Session SESSION = testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties())).build();
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testWithSimpleQuery()
    {
        String baseTable = "base_table";
        QualifiedObjectName baseTableName = QualifiedObjectName.valueOf("catalog.schema.base_table");
        QualifiedObjectName materializedViewName = QualifiedObjectName.valueOf("catalog.schema.view");

        Map<QualifiedObjectName, List<QualifiedObjectName>> baseTableToMaterializedViews = new HashMap<>();
        baseTableToMaterializedViews.put(baseTableName, ImmutableList.of(materializedViewName));

        ImmutableSet<QualifiedObjectName> expectedMaterializedViewCandidates = ImmutableSet.of(materializedViewName);

        String baseQuerySql = format("SELECT x, y From %s", baseTable);

        assertCandidateMaterializedView(expectedMaterializedViewCandidates, baseQuerySql, baseTableToMaterializedViews);
    }

    @Test
    public void testWithAliasedRelation()
    {
        String baseTable = "base_table";
        QualifiedObjectName baseTableName = QualifiedObjectName.valueOf("catalog.schema.base_table");
        QualifiedObjectName materializedViewName = QualifiedObjectName.valueOf("catalog.schema.view");

        Map<QualifiedObjectName, List<QualifiedObjectName>> baseTableToMaterializedViews = new HashMap<>();
        baseTableToMaterializedViews.put(baseTableName, ImmutableList.of(materializedViewName));

        ImmutableSet<QualifiedObjectName> expectedMaterializedViewCandidates = ImmutableSet.of(materializedViewName);

        String baseQuerySql = format("SELECT x, y From %s b1", baseTable);

        assertCandidateMaterializedView(expectedMaterializedViewCandidates, baseQuerySql, baseTableToMaterializedViews);
    }

    @Test
    public void testWithJoin()
    {
        String baseTable1 = "base_table1";
        String baseTable2 = "base_table2";
        QualifiedObjectName baseTableName1 = QualifiedObjectName.valueOf("catalog.schema.base_table1");
        QualifiedObjectName baseTableName2 = QualifiedObjectName.valueOf("catalog.schema.base_table2");
        QualifiedObjectName materializedViewName = QualifiedObjectName.valueOf("catalog.schema.view");

        Map<QualifiedObjectName, List<QualifiedObjectName>> baseTableToMaterializedViews = new HashMap<>();
        baseTableToMaterializedViews.put(baseTableName1, ImmutableList.of(materializedViewName));
        baseTableToMaterializedViews.put(baseTableName2, ImmutableList.of(materializedViewName));

        ImmutableSet<QualifiedObjectName> expectedMaterializedViewCandidates = ImmutableSet.of(materializedViewName);

        String baseQuerySql = format("SELECT x, y FROM %s b1 JOIN %s b2 ON b1.id = b2.id", baseTable1, baseTable2);

        assertCandidateMaterializedView(expectedMaterializedViewCandidates, baseQuerySql, baseTableToMaterializedViews);
    }

    @Test
    public void testWithOneIntersection()
    {
        String baseTable1 = "base_table1";
        String baseTable2 = "base_table2";
        QualifiedObjectName baseTableName1 = QualifiedObjectName.valueOf("catalog.schema.base_table1");
        QualifiedObjectName baseTableName2 = QualifiedObjectName.valueOf("catalog.schema.base_table2");
        QualifiedObjectName materializedViewName1 = QualifiedObjectName.valueOf("catalog.schema.view1");
        QualifiedObjectName materializedViewName2 = QualifiedObjectName.valueOf("catalog.schema.view2");
        QualifiedObjectName materializedViewName3 = QualifiedObjectName.valueOf("catalog.schema.view3");

        Map<QualifiedObjectName, List<QualifiedObjectName>> baseTableToMaterializedViews = new HashMap<>();
        baseTableToMaterializedViews.put(baseTableName1, ImmutableList.of(materializedViewName1, materializedViewName2));
        baseTableToMaterializedViews.put(baseTableName2, ImmutableList.of(materializedViewName1, materializedViewName3));

        ImmutableSet<QualifiedObjectName> expectedMaterializedViewCandidates = ImmutableSet.of(materializedViewName1);

        String baseQuerySql = format("SELECT x, y FROM %s JOIN %s ON %s.id = %s.id", baseTable1, baseTable2, baseTable1, baseTable2);

        assertCandidateMaterializedView(expectedMaterializedViewCandidates, baseQuerySql, baseTableToMaterializedViews);
    }

    @Test
    public void testWithNoIntersection()
    {
        String baseTable1 = "base_table1";
        String baseTable2 = "base_table2";
        QualifiedObjectName baseTableName1 = QualifiedObjectName.valueOf("catalog.schema.base_table1");
        QualifiedObjectName baseTableName2 = QualifiedObjectName.valueOf("catalog.schema.base_table2");
        QualifiedObjectName materializedViewName1 = QualifiedObjectName.valueOf("catalog.schema.view1");
        QualifiedObjectName materializedViewName2 = QualifiedObjectName.valueOf("catalog.schema.view2");
        QualifiedObjectName materializedViewName3 = QualifiedObjectName.valueOf("catalog.schema.view3");
        QualifiedObjectName materializedViewName4 = QualifiedObjectName.valueOf("catalog.schema.view4");

        Map<QualifiedObjectName, List<QualifiedObjectName>> baseTableToMaterializedViews = new HashMap<>();
        baseTableToMaterializedViews.put(baseTableName1, ImmutableList.of(materializedViewName1, materializedViewName2));
        baseTableToMaterializedViews.put(baseTableName2, ImmutableList.of(materializedViewName3, materializedViewName4));

        ImmutableSet<QualifiedObjectName> expectedMaterializedViewCandidates = ImmutableSet.of();

        String baseQuerySql = format("SELECT x, y FROM %s JOIN %s ON %s.id = %s.id", baseTable1, baseTable2, baseTable1, baseTable2);

        assertCandidateMaterializedView(expectedMaterializedViewCandidates, baseQuerySql, baseTableToMaterializedViews);
    }

    @Test
    public void testWithMultipleJoin()
    {
        String baseTable1 = "base_table1";
        String baseTable2 = "base_table2";
        String baseTable3 = "base_table3";
        QualifiedObjectName baseTableName1 = QualifiedObjectName.valueOf("catalog.schema.base_table1");
        QualifiedObjectName baseTableName2 = QualifiedObjectName.valueOf("catalog.schema.base_table2");
        QualifiedObjectName baseTableName3 = QualifiedObjectName.valueOf("catalog.schema.base_table3");
        QualifiedObjectName materializedViewName1 = QualifiedObjectName.valueOf("catalog.schema.view1");
        QualifiedObjectName materializedViewName2 = QualifiedObjectName.valueOf("catalog.schema.view2");
        QualifiedObjectName materializedViewName3 = QualifiedObjectName.valueOf("catalog.schema.view3");
        QualifiedObjectName materializedViewName4 = QualifiedObjectName.valueOf("catalog.schema.view4");

        Map<QualifiedObjectName, List<QualifiedObjectName>> baseTableToMaterializedViews = new HashMap<>();
        baseTableToMaterializedViews.put(baseTableName1, ImmutableList.of(materializedViewName1, materializedViewName2, materializedViewName3, materializedViewName4));
        baseTableToMaterializedViews.put(baseTableName2, ImmutableList.of(materializedViewName1, materializedViewName3, materializedViewName4));
        baseTableToMaterializedViews.put(baseTableName3, ImmutableList.of(materializedViewName2, materializedViewName3, materializedViewName4));

        ImmutableSet<QualifiedObjectName> expectedMaterializedViewCandidates = ImmutableSet.of(materializedViewName3, materializedViewName4);

        String baseQuerySql = format("SELECT x, y FROM %s b1 JOIN %s b2 ON b1.id = b2.id JOIN %s b3 ON b2.id = b3.id",
                baseTable1,
                baseTable2,
                baseTable3);

        assertCandidateMaterializedView(expectedMaterializedViewCandidates, baseQuerySql, baseTableToMaterializedViews);
    }

    @Test
    public void testWithMultipleJoinWithNoIntersection()
    {
        String baseTable1 = "base_table1";
        String baseTable2 = "base_table2";
        String baseTable3 = "base_table3";
        QualifiedObjectName baseTableName1 = QualifiedObjectName.valueOf("catalog.schema.base_table1");
        QualifiedObjectName baseTableName2 = QualifiedObjectName.valueOf("catalog.schema.base_table2");
        QualifiedObjectName baseTableName3 = QualifiedObjectName.valueOf("catalog.schema.base_table3");
        QualifiedObjectName materializedViewName1 = QualifiedObjectName.valueOf("catalog.schema.view1");
        QualifiedObjectName materializedViewName2 = QualifiedObjectName.valueOf("catalog.schema.view2");
        QualifiedObjectName materializedViewName3 = QualifiedObjectName.valueOf("catalog.schema.view3");

        Map<QualifiedObjectName, List<QualifiedObjectName>> baseTableToMaterializedViews = new HashMap<>();
        baseTableToMaterializedViews.put(baseTableName1, ImmutableList.of(materializedViewName1, materializedViewName2));
        baseTableToMaterializedViews.put(baseTableName2, ImmutableList.of(materializedViewName1, materializedViewName3));
        baseTableToMaterializedViews.put(baseTableName3, ImmutableList.of(materializedViewName2, materializedViewName3));

        ImmutableSet<QualifiedObjectName> expectedMaterializedViewCandidates = ImmutableSet.of();

        String baseQuerySql = format("SELECT x, y FROM %s b1 JOIN %s b2 ON b1.id = b2.id JOIN %s b3 ON b2.id = b3.id",
                baseTable1,
                baseTable2,
                baseTable3);

        assertCandidateMaterializedView(expectedMaterializedViewCandidates, baseQuerySql, baseTableToMaterializedViews);
    }

    private void assertCandidateMaterializedView(
            ImmutableSet<QualifiedObjectName> expectedMaterializedViewCandidates,
            String baseQuerySql,
            Map<QualifiedObjectName, List<QualifiedObjectName>> baseTableToMaterializedViews)
    {
        Query baseQuery = (Query) SQL_PARSER.createStatement(baseQuerySql);

        MaterializedViewCandidateExtractor materializedViewCandidateExtractor = new MaterializedViewCandidateExtractor(SESSION, baseTableToMaterializedViews);
        materializedViewCandidateExtractor.process(baseQuery);
        Set<QualifiedObjectName> materializedViewCandidates = materializedViewCandidateExtractor.getMaterializedViewCandidates();

        assertEquals(materializedViewCandidates, expectedMaterializedViewCandidates);
    }
}
