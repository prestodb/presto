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
package com.facebook.presto.raptor;

import com.facebook.presto.tests.AbstractTestDistributedQueries;
import org.testng.annotations.Test;

import static com.facebook.presto.raptor.RaptorQueryRunner.createRaptorQueryRunner;
import static com.facebook.presto.raptor.RaptorQueryRunner.createSampledSession;
import static io.airlift.tpch.TpchTable.getTables;

public class TestRaptorDistributedQueries
        extends AbstractTestDistributedQueries
{
    public TestRaptorDistributedQueries()
            throws Exception
    {
        super(createRaptorQueryRunner(getTables()), createSampledSession());
    }

    @Test
    public void testCreateArrayTable()
            throws Exception
    {
        assertQuery("CREATE TABLE array_test AS SELECT ARRAY [1, 2, 3] AS c", "SELECT 1");
        assertQuery("SELECT cardinality(c) FROM array_test", "SELECT 3");
        assertQueryTrue("DROP TABLE array_test");
    }

    @Test
    public void testMapTable()
            throws Exception
    {
        assertQuery("CREATE TABLE map_test AS SELECT MAP(ARRAY [1, 2, 3], ARRAY ['hi', 'bye', NULL]) AS c", "SELECT 1");
        assertQuery("SELECT c[1] FROM map_test", "SELECT 'hi'");
        assertQuery("SELECT c[3] FROM map_test", "SELECT NULL");
        assertQueryTrue("DROP TABLE map_test");
    }
}
