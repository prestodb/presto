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
package com.facebook.presto.iceberg.security;

import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;

public class TestIcebergReadOnlySecurity
{
    private QueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = createIcebergQueryRunner(ImmutableMap.of("hive.security", "read-only"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Access Denied: Cannot create .*")
    public void testCannotCreate()
    {
        queryRunner.execute("CREATE TABLE test_table (a bigint, b double, c varchar)");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Access Denied: Cannot drop .*")
    public void testCannotDrop()
    {
        queryRunner.execute("DROP TABLE test_table;");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Access Denied: Cannot insert .*")
    public void testCannotInsert()
    {
        queryRunner.execute("INSERT INTO test_table VALUES (1, 1.0, 'test');");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Access Denied: Cannot delete .*")
    public void testCannotDelete()
    {
        queryRunner.execute("DELETE FROM test_table WHERE c = 'test';");
    }
}
