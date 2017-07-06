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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.tests.tpch.IndexedTpchPlugin;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestDistributedQueriesIndexed
        extends AbstractTestIndexedQueries
{
    public TestDistributedQueriesIndexed()
            throws Exception
    {
        super(TestDistributedQueriesIndexed::createQueryRunner);
    }

    private static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch_indexed")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 3);

        queryRunner.installPlugin(new IndexedTpchPlugin(INDEX_SPEC));
        queryRunner.createCatalog("tpch_indexed", "tpch_indexed");
        return queryRunner;
    }

    @Test
    public void testAnalyzeIndexedJoin()
            throws Exception
    {
        assertExplainAnalyze("EXPLAIN ANALYZE " +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 8 = 0) l\n" +
                "JOIN orders o\n" +
                "  ON l.orderkey = o.orderkey");
    }

    private void assertExplainAnalyze(@Language("SQL") String query)
    {
        String value = getOnlyElement(computeActual(query).getOnlyColumnAsSet());

        assertTrue(value.matches("(?s:.*)CPU:.*, Input:.*, Output(?s:.*)"), format("Expected output to contain \"CPU:.*, Input:.*, Output\", but it is %s", value));
    }
}
