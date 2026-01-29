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
package com.facebook.presto.sidecar;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManagerFactory;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;

public class TestNativeSidecarHiveCatalog
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createNation(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createRegion(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(true)
                .build();
        TestNativeSidecarPlugin.setupNativeSidecarPlugin(queryRunner);
        queryRunner.loadFunctionNamespaceManager(
                NativeFunctionNamespaceManagerFactory.NAME,
                "hive",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP"));

        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testInitcap()
    {
        assertQuery("SELECT hive.default.initcap('Hello world')", "SELECT('Hello World')");
        assertQuery("SELECT hive.default.initcap('abcd')", "SELECT('Abcd')");
        assertQuery("SELECT hive.default.initcap('a   b   c')", "SELECT('A   B   C')");
        assertQuery("SELECT hive.default.initcap('')", "SELECT('')");
        assertQuery("SELECT hive.default.initcap('x')", "SELECT('X')");
        assertQuery("SELECT hive.default.initcap('hello123world')", "SELECT('Hello123world')");
        assertQuery("SELECT hive.default.initcap('hello-world')", "SELECT('Hello-world')");
        assertQuery("SELECT hive.default.initcap(NULL)", "SELECT CAST(NULL AS VARCHAR)");
        assertQuery("SELECT hive.default.initcap('test')", "SELECT('Test')");
    }

    @Test
    public void testInitcapWithBuiltInFunctions()
    {
        assertQuery("SELECT hive.default.initcap(reverse('Hello world'))", "SELECT('Dlrow Olleh')");
        assertQuery("SELECT hive.default.initcap(from_utf8(from_base64('aGVsbG8gd29ybGQ=')))", "SELECT from_utf8(from_base64('SGVsbG8gV29ybGQ='))");
        assertQuery("SELECT to_base64(to_utf8(hive.default.initcap('a   b   c')))", "SELECT to_base64(to_utf8('A   B   C'))");
        assertQuery("SELECT to_base64(to_utf8(hive.default.initcap('hello123world')))", "SELECT to_base64(to_utf8('Hello123world'))");
    }

    @Test(enabled = false)
    public void testInitcapWithNullValues()
    {
        assertQuery(
                "SELECT hive.default.initcap(CASE WHEN nationkey = 0 THEN NULL ELSE name END) " +
                        "FROM nation WHERE nationkey < 2 ORDER BY nationkey",
                "VALUES (CAST(NULL AS VARCHAR)), ('Argentina')");
        assertQuery(
                "SELECT COUNT(*) FROM nation WHERE hive.default.initcap(CASE WHEN nationkey < 0 THEN name ELSE NULL END) IS NULL",
                "SELECT BIGINT '25'");
    }

    @Test
    public void testInitcapWithStringOperations()
    {
        assertQuery(
                "SELECT hive.default.initcap(CONCAT(name, ' region')) FROM region WHERE regionkey = 0",
                "SELECT 'Africa Region'");
        assertQuery(
                "SELECT hive.default.initcap(SUBSTR(name, 1, 3)) FROM nation WHERE nationkey = 0",
                "SELECT 'Alg'");
        assertQuery(
                "SELECT hive.default.initcap(LOWER(name)) FROM nation WHERE nationkey IN (0, 1) ORDER BY nationkey",
                "VALUES ('Algeria'), ('Argentina')");
        assertQuery(
                "SELECT hive.default.initcap(UPPER(name)) FROM region WHERE regionkey < 2 ORDER BY regionkey",
                "VALUES ('Africa'), ('America')");
        assertQuery(
                "SELECT hive.default.initcap(REPLACE(name, 'A', 'X')) FROM region WHERE regionkey = 0",
                "SELECT 'Xfricx'");
        assertQuery(
                "SELECT hive.default.initcap(TRIM(CONCAT('  ', name, '  '))) FROM nation WHERE nationkey = 0",
                "SELECT 'Algeria'");
    }

    @Test
    public void testInitcapWithColumnValues()
    {
        // Test initcap with column values to ensure worker-side evaluation
        assertQuery("SELECT hive.default.initcap(name) FROM region", "VALUES ('Africa'), ('America'), ('Asia'), ('Europe'), ('Middle East')");
        assertQuery(
                "SELECT hive.default.initcap(name) FROM nation WHERE nationkey < 5 ORDER BY nationkey",
                "VALUES ('Algeria'), ('Argentina'), ('Brazil'), ('Canada'), ('Egypt')");
        assertQuery(
                "SELECT hive.default.initcap(name) FROM region ORDER BY regionkey",
                "VALUES ('Africa'), ('America'), ('Asia'), ('Europe'), ('Middle East')");
        assertQuery(
                "SELECT COUNT(*), hive.default.initcap(name) FROM region GROUP BY name ORDER BY name",
                "VALUES (BIGINT '1', 'Africa'), (BIGINT '1', 'America'), (BIGINT '1', 'Asia'), (BIGINT '1', 'Europe'), (BIGINT '1', 'Middle East')");
        assertQuery(
                "SELECT LENGTH(hive.default.initcap(comment)) FROM nation WHERE nationkey = 0",
                "SELECT LENGTH(comment) FROM nation WHERE nationkey = 0");
        assertQuery(
                "SELECT regionkey, LENGTH(hive.default.initcap(comment)) FROM region WHERE regionkey < 3 ORDER BY regionkey",
                "SELECT regionkey, LENGTH(comment) FROM region WHERE regionkey < 3 ORDER BY regionkey");
        assertQuery(
                "SELECT hive.default.initcap(name) FROM nation WHERE name IN ('ALGERIA', 'ARGENTINA', 'BRAZIL') ORDER BY name",
                "VALUES ('Algeria'), ('Argentina'), ('Brazil')");
        assertQuery(
                "SELECT COUNT(DISTINCT hive.default.initcap(name)) FROM nation",
                "SELECT BIGINT '25'");
    }

    @Test(enabled = false)
    public void testDataMismatch()
    {
        assertQuery(
                "SELECT hive.default.initcap(CASE WHEN nationkey = 0 THEN NULL ELSE name END) " +
                        "FROM nation WHERE nationkey < 2 ORDER BY nationkey",
                "VALUES (CAST(NULL AS VARCHAR)), ('Argentina')");
    }

    @Test(enabled = false)
    public void testServerCrashes()
    {
        assertQuery(
                "SELECT COUNT(*) FROM nation WHERE hive.default.initcap(CASE WHEN nationkey < 0 THEN name ELSE NULL END) IS NULL",
                "SELECT BIGINT '25'");
    }
}
