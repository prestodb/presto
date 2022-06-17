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
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Query;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class TestMaterializedViewCandidateExtractor
{
    private static final Session SESSION = testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties())).build();
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = new MockMetadata();
    private static final QualifiedObjectName VIEW_0 = nameOf("view0");
    private static final QualifiedObjectName VIEW_1 = nameOf("view1");
    private static final QualifiedObjectName VIEW_2 = nameOf("view2");
    private static final QualifiedObjectName VIEW_3 = nameOf("view3");
    private static final QualifiedObjectName VIEW_4 = nameOf("view4");
    private static final QualifiedObjectName VIEW_5 = nameOf("view5");

    @Test
    public void testWithSimpleQuery()
    {
        String baseTable = "base_table_v0";
        String baseQuerySql = format("SELECT x, y From %s", baseTable);
        assertCandidateMaterializedView(
                baseQuerySql,
                ImmutableMap.of(
                        nameOf(baseTable), ImmutableList.of(VIEW_0)));
    }

    @Test
    public void testWithAliasedRelation()
    {
        String baseTable = "base_table_v0";
        String baseQuerySql = format("SELECT x, y From %s b1", baseTable);
        assertCandidateMaterializedView(
                baseQuerySql,
                ImmutableMap.of(
                        nameOf(baseTable), ImmutableList.of(VIEW_0)));
    }

    @Test
    public void testWithJoin()
    {
        String baseTable1 = "base_table1_v0";
        String baseTable2 = "base_table2_v0";
        String baseQuerySql = format("SELECT x, y FROM %s b1 JOIN %s b2 ON b1.id = b2.id", baseTable1, baseTable2);
        assertCandidateMaterializedView(
                baseQuerySql,
                ImmutableMap.of(
                        nameOf(baseTable1), ImmutableList.of(VIEW_0),
                        nameOf(baseTable2), ImmutableList.of(VIEW_0)));
    }

    @Test
    public void testWithOneIntersection()
    {
        String baseTable1 = "base_table_v1v2";
        String baseTable2 = "base_table_v2v3";
        String baseQuerySql = format("SELECT x, y FROM %s JOIN %s ON %s.id = %s.id", baseTable1, baseTable2, baseTable1, baseTable2);
        assertCandidateMaterializedView(
                baseQuerySql,
                ImmutableMap.of(
                        nameOf(baseTable1), ImmutableList.of(VIEW_1, VIEW_2),
                        nameOf(baseTable2), ImmutableList.of(VIEW_2, VIEW_3)));
    }

    @Test
    public void testWithNoIntersection()
    {
        String baseTable1 = "base_table_v1v2";
        String baseTable2 = "base_table_v4v5";
        String baseQuerySql = format("SELECT x, y FROM %s JOIN %s ON %s.id = %s.id", baseTable1, baseTable2, baseTable1, baseTable2);
        assertCandidateMaterializedView(
                baseQuerySql,
                ImmutableMap.of(
                        nameOf(baseTable1), ImmutableList.of(VIEW_1, VIEW_2),
                        nameOf(baseTable2), ImmutableList.of(VIEW_4, VIEW_5)));
    }

    @Test
    public void testWithMultipleJoin()
    {
        String baseTable1 = "base_table_v1v2v3v4";
        String baseTable2 = "base_table_v1v2v3";
        String baseTable3 = "base_table_v1v2v4";
        String baseQuerySql = format("SELECT x, y FROM %s b1 JOIN %s b2 ON b1.id = b2.id JOIN %s b3 ON b2.id = b3.id",
                baseTable1,
                baseTable2,
                baseTable3);
        assertCandidateMaterializedView(
                baseQuerySql,
                ImmutableMap.of(
                        nameOf(baseTable1), ImmutableList.of(VIEW_1, VIEW_2, VIEW_3, VIEW_4),
                        nameOf(baseTable2), ImmutableList.of(VIEW_1, VIEW_2, VIEW_3),
                        nameOf(baseTable3), ImmutableList.of(VIEW_1, VIEW_2, VIEW_4)));
    }

    @Test
    public void testWithMultipleJoinWithNoIntersection()
    {
        String baseTable1 = "base_table_v1v2";
        String baseTable2 = "base_table_v2v3";
        String baseTable3 = "base_table_v1v3";
        String baseQuerySql = format("SELECT x, y FROM %s b1 JOIN %s b2 ON b1.id = b2.id JOIN %s b3 ON b2.id = b3.id",
                baseTable1,
                baseTable2,
                baseTable3);
        assertCandidateMaterializedView(
                baseQuerySql,
                ImmutableMap.of(
                        nameOf(baseTable1), ImmutableList.of(VIEW_1, VIEW_2),
                        nameOf(baseTable2), ImmutableList.of(VIEW_2, VIEW_3),
                        nameOf(baseTable3), ImmutableList.of(VIEW_1, VIEW_3)));
    }

    private void assertCandidateMaterializedView(
            String baseQuerySql, Map<QualifiedObjectName, List<QualifiedObjectName>> expectedMaterializedViewCandidates)
    {
        Query baseQuery = (Query) SQL_PARSER.createStatement(baseQuerySql);

        MaterializedViewCandidateExtractor materializedViewCandidateExtractor = new MaterializedViewCandidateExtractor(SESSION, METADATA);
        materializedViewCandidateExtractor.process(baseQuery);
        Map<QualifiedObjectName, List<QualifiedObjectName>> materializedViewCandidates = materializedViewCandidateExtractor.getMaterializedViewCandidatesForTable();

        assertEquals(materializedViewCandidates, expectedMaterializedViewCandidates);
    }

    private static QualifiedObjectName nameOf(String baseName)
    {
        checkArgument(!baseName.contains("."), "baseName cannot already be qualified");
        return QualifiedObjectName.valueOf(format("catalog.schema.%s", baseName));
    }

    private static class MockMetadata
            extends AbstractMockMetadata
    {
        @Override
        public List<QualifiedObjectName> getReferencedMaterializedViews(Session session, QualifiedObjectName tableName)
        {
            switch (tableName.toString()) {
                case "catalog.schema.base_table_v0":
                case "catalog.schema.base_table1_v0":
                case "catalog.schema.base_table2_v0":
                    return ImmutableList.of(VIEW_0);
                case "catalog.schema.base_table_v1v2":
                    return ImmutableList.of(VIEW_1, VIEW_2);
                case "catalog.schema.base_table_v2v3":
                    return ImmutableList.of(VIEW_2, VIEW_3);
                case "catalog.schema.base_table_v4v5":
                    return ImmutableList.of(VIEW_4, VIEW_5);
                case "catalog.schema.base_table_v1v2v3v4":
                    return ImmutableList.of(VIEW_1, VIEW_2, VIEW_3, VIEW_4);
                case "catalog.schema.base_table_v1v2v3":
                    return ImmutableList.of(VIEW_1, VIEW_2, VIEW_3);
                case "catalog.schema.base_table_v1v2v4":
                    return ImmutableList.of(VIEW_1, VIEW_2, VIEW_4);
                case "catalog.schema.base_table_v1v3":
                    return ImmutableList.of(VIEW_1, VIEW_3);
            }
            return ImmutableList.of();
        }
    }
}
