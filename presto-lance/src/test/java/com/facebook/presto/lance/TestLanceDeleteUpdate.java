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
package com.facebook.presto.lance;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public class TestLanceDeleteUpdate
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LanceQueryRunner.createLanceQueryRunner(ImmutableMap.of());
    }

    @Test
    public void testDeleteAllDataFromTable()
    {
        String table = "test_delete_all";
        assertUpdate("CREATE TABLE " + table + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name)", 3);
        assertUpdate("DELETE FROM " + table, 3);
        assertQuery("SELECT count(*) FROM " + table, "SELECT 0");
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testDeleteWithBigintPredicate()
    {
        String table = "test_delete_bigint";
        assertUpdate("CREATE TABLE " + table + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name)", 3);
        assertUpdate("DELETE FROM " + table + " WHERE id = 2", 1);
        assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 1, 3");
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testDeleteWithVarcharPredicate()
    {
        String table = "test_delete_varchar";
        assertUpdate("CREATE TABLE " + table + " AS SELECT * FROM (VALUES (1, 'keep'), (2, 'drop'), (3, 'keep')) t(id, name)", 3);
        assertUpdate("DELETE FROM " + table + " WHERE name = 'drop'", 1);
        assertQuery("SELECT id FROM " + table + " ORDER BY id", "VALUES 1, 3");
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testDeleteEmptyResult()
    {
        String table = "test_delete_none";
        assertUpdate("CREATE TABLE " + table + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)", 2);
        assertUpdate("DELETE FROM " + table + " WHERE id = 999", 0);
        assertQuery("SELECT count(*) FROM " + table, "SELECT 2");
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testUpdate()
    {
        String table = "test_update_all";
        assertUpdate("CREATE TABLE " + table + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name)", 3);
        assertUpdate("UPDATE " + table + " SET name = 'z'", 3);
        assertQuery("SELECT id, name FROM " + table + " ORDER BY id", "VALUES (1, 'z'), (2, 'z'), (3, 'z')");
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testRowLevelUpdate()
    {
        String table = "test_update_row";
        assertUpdate("CREATE TABLE " + table + " AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) t(id, name)", 3);
        assertUpdate("UPDATE " + table + " SET name = 'updated' WHERE id = 2", 1);
        // Only the matched row changes; unmodified columns are preserved.
        assertQuery("SELECT id, name FROM " + table + " ORDER BY id", "VALUES (1, 'a'), (2, 'updated'), (3, 'c')");
        assertUpdate("DROP TABLE " + table);
    }
}
