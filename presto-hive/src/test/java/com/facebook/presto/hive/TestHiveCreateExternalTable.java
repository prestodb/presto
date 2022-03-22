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
package com.facebook.presto.hive;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestHiveCreateExternalTable
        extends AbstractTestQueryFramework
{
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(NATION),
                ImmutableMap.of(),
                "sql-standard",
                ImmutableMap.of(
                        "hive.non-managed-table-writes-enabled", "true"),
                Optional.empty());
    }

    @Test
    public void testCreateExternalTableWithData()
            throws IOException
    {
        String tableName = "test_create_external";
        File tempDir = createTempDir();
        File tableLocation = new File(tempDir, tableName);

        @Language("SQL")
        String createTableSql = format("" +
                        "CREATE TABLE test_create_external " +
                        "WITH (external_location = '%s') AS " +
                        "SELECT * FROM tpch.tiny.nation",
                tableLocation.toURI().toASCIIString());

        assertUpdate(createTableSql, 25);

        MaterializedResult expected = computeActual("SELECT * FROM tpch.tiny.nation");
        MaterializedResult actual = computeActual("SELECT * FROM " + tableName);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        String tablePath = (String) computeActual(
                "SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '/') FROM " + tableName)
                .getOnlyValue();
        assertTrue(tablePath.startsWith(tableLocation.toURI().toString()));

        assertUpdate("DROP TABLE test_create_external");
        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testCreateExternalTableAsWithExistingDirectory()
    {
        File tempDir = createTempDir();

        @Language("SQL")
        String createTableSql = format("" +
                        "CREATE TABLE test_create_external " +
                        "WITH (external_location = '%s') AS " +
                        "SELECT * FROM tpch.tiny.nation",
                tempDir.toURI().toASCIIString());

        assertQueryFails(createTableSql, "Target directory for table '.*' already exists:.*");
    }
}
