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
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;

public class TestMaterializedViewCandidateExtractor
{
    private static final Session SESSION = testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties())).build();
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = new MockMetadata();

    @Test
    public void testWithSimpleQuery()
    {
        String baseTable = "base_table_v0";
        QualifiedObjectName materializedViewName = QualifiedObjectName.valueOf("catalog.schema.view0");

        ImmutableSet<QualifiedObjectName> expectedMaterializedViewCandidates = ImmutableSet.of(materializedViewName);

        String baseQuerySql = format("SELECT x, y From %s", baseTable);

        assertCandidateMaterializedView(expectedMaterializedViewCandidates, baseQuerySql);
    }

    @Test
    public void testWithAliasedRelation()
    {
        String baseTable = "base_table_v0";
        String baseQuerySql = format("SELECT x, y From %s b1", baseTable);
        assertCandidateMaterializedView(ImmutableSet.of(QualifiedObjectName.valueOf("catalog.schema.view0")), baseQuerySql);
    }

    @Test
    public void testWithJoin()
    {
        String baseTable1 = "base_table1_v0";
        String baseTable2 = "base_table2_v0";
        String baseQuerySql = format("SELECT x, y FROM %s b1 JOIN %s b2 ON b1.id = b2.id", baseTable1, baseTable2);
        assertCandidateMaterializedView(ImmutableSet.of(QualifiedObjectName.valueOf("catalog.schema.view0")), baseQuerySql);
    }

    @Test
    public void testWithOneIntersection()
    {
        String baseTable1 = "base_table_v1v2";
        String baseTable2 = "base_table_v2v3";
        String baseQuerySql = format("SELECT x, y FROM %s JOIN %s ON %s.id = %s.id", baseTable1, baseTable2, baseTable1, baseTable2);
        assertCandidateMaterializedView(ImmutableSet.of(QualifiedObjectName.valueOf("catalog.schema.view2")), baseQuerySql);
    }

    @Test
    public void testWithNoIntersection()
    {
        String baseTable1 = "base_table_v1v2";
        String baseTable2 = "base_table_v3v4";
        String baseQuerySql = format("SELECT x, y FROM %s JOIN %s ON %s.id = %s.id", baseTable1, baseTable2, baseTable1, baseTable2);
        assertCandidateMaterializedView(ImmutableSet.of(), baseQuerySql);
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
        assertCandidateMaterializedView(ImmutableSet.of(QualifiedObjectName.valueOf("catalog.schema.view1"), QualifiedObjectName.valueOf("catalog.schema.view2")), baseQuerySql);
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
        assertCandidateMaterializedView(ImmutableSet.of(), baseQuerySql);
    }

    private void assertCandidateMaterializedView(
            ImmutableSet<QualifiedObjectName> expectedMaterializedViewCandidates,
            String baseQuerySql)
    {
        Query baseQuery = (Query) SQL_PARSER.createStatement(baseQuerySql);

        MaterializedViewCandidateExtractor materializedViewCandidateExtractor = new MaterializedViewCandidateExtractor(SESSION, METADATA);
        materializedViewCandidateExtractor.process(baseQuery);
        Set<QualifiedObjectName> materializedViewCandidates = materializedViewCandidateExtractor.getMaterializedViewCandidates();

        assertEquals(materializedViewCandidates, expectedMaterializedViewCandidates);
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
                    return ImmutableList.of(QualifiedObjectName.valueOf("catalog.schema.view0"));
                case "catalog.schema.base_table_v1v2":
                    return ImmutableList.of(QualifiedObjectName.valueOf("catalog.schema.view1"), QualifiedObjectName.valueOf("catalog.schema.view2"));
                case "catalog.schema.base_table_v2v3":
                    return ImmutableList.of(QualifiedObjectName.valueOf("catalog.schema.view2"), QualifiedObjectName.valueOf("catalog.schema.view3"));
                case "catalog.schema.base_table_v3v4":
                    return ImmutableList.of(QualifiedObjectName.valueOf("catalog.schema.view4"), QualifiedObjectName.valueOf("catalog.schema.view5"));
                case "catalog.schema.base_table_v1v2v3v4":
                    return ImmutableList.of(
                            QualifiedObjectName.valueOf("catalog.schema.view1"),
                            QualifiedObjectName.valueOf("catalog.schema.view2"),
                            QualifiedObjectName.valueOf("catalog.schema.view3"),
                            QualifiedObjectName.valueOf("catalog.schema.view4"));
                case "catalog.schema.base_table_v1v2v3":
                    return ImmutableList.of(
                            QualifiedObjectName.valueOf("catalog.schema.view1"),
                            QualifiedObjectName.valueOf("catalog.schema.view2"),
                            QualifiedObjectName.valueOf("catalog.schema.view3"));
                case "catalog.schema.base_table_v1v2v4":
                    return ImmutableList.of(
                            QualifiedObjectName.valueOf("catalog.schema.view1"),
                            QualifiedObjectName.valueOf("catalog.schema.view2"),
                            QualifiedObjectName.valueOf("catalog.schema.view4"));
                case "catalog.schema.base_table_v1v3":
                    return ImmutableList.of(QualifiedObjectName.valueOf("catalog.schema.view1"), QualifiedObjectName.valueOf("catalog.schema.view3"));
            }
            return ImmutableList.of();
        }
    }
}
