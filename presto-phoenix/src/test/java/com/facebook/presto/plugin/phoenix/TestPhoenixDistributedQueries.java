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
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.tests.AbstractTestQueries;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

@Test
public class TestPhoenixDistributedQueries
        extends AbstractTestQueries
{
    public TestPhoenixDistributedQueries()
            throws Exception
    {
        super(() -> PhoenixQueryRunner.createPhoenixQueryRunner(ImmutableMap.of(), TpchTable.getTables()));
    }

    @Test
    public void testInsertDuplicateRows()
    {
        // Insert a row without specifying the comment column. That column will be null.
        // https://prestodb.io/docs/current/sql/insert.html
        try {
            assertUpdate("CREATE TABLE test_insert_duplicate AS SELECT 1 a, 2 b, '3' c", 1);
            assertQuery("SELECT a, b, c FROM test_insert_duplicate", "SELECT 1, 2, '3'");
            assertUpdate("INSERT INTO test_insert_duplicate (a, c) VALUES (1, '4')", 1);
            assertQuery("SELECT a, b, c FROM test_insert_duplicate", "SELECT 1, null, '4'");
            assertUpdate("INSERT INTO test_insert_duplicate (a, b) VALUES (1, 3)", 1);
            assertQuery("SELECT a, b, c FROM test_insert_duplicate", "SELECT 1, 3, null");
        }
        finally {
            assertUpdate("DROP TABLE test_insert_duplicate");
        }
    }
}
