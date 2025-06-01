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
package com.facebook.presto.sql.query;

import com.facebook.presto.Session;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.OFFSET_CLAUSE_ENABLED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestOffsetLimit
{
    private LocalQueryRunner runner;

    @BeforeClass
    public void init()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .build();
        runner = new LocalQueryRunner(session);
        runner.createCatalog(session.getCatalog().get(), new TpchConnectorFactory(1), ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        runner.close();
    }

    @Test
    public void testOrderByConsistency()
    {
        String query = "SELECT name FROM customer ORDER BY name OFFSET 1 LIMIT 256";
        MaterializedResult expectedResults = runner.execute(query).toTestTypes();

        List<MaterializedRow> expectedRows = expectedResults.getMaterializedRows();

        for (int i = 0; i < 5; i++) {
            MaterializedResult actualResults = runner.execute(query).toTestTypes();
            List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
            assertEquals(actualRows, expectedRows, "Mismatched results on run " + i);
        }
    }
}
