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
package com.facebook.presto.nativetests.iceberg;

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

/**
 * End-to-end native tests for Iceberg Copy-on-Write (COW) partition-level DELETE.
 *
 * <p>For COW tables, DELETE is only supported when the predicate covers one or more
 * complete partitions (metadata delete). Row-level deletes on COW tables must be
 * rejected with an explicit error.
 */
public class TestIcebergCopyOnWriteDelete
        extends AbstractTestQueryFramework
{
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .setAddStorageFormatToPath(true)
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testCopyOnWritePartitionLevelDeleteSucceeds()
    {
        String tableName = "test_cow_partition_delete";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer) WITH (\"format-version\" = '2', partitioning = ARRAY['id'], \"write.delete.mode\" = 'copy-on-write')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 10)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 1)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 5)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 10), (2, 1), (3, 5)");

            // DELETE on the partition column removes the entire partition — supported for COW.
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 3", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 10), (2, 1)");

            // Verify the COW partition delete is reflected in Iceberg metadata system tables.
            assertQuery("SELECT COUNT(*) > 0 FROM \"" + tableName + "$snapshots\" WHERE operation = 'delete'", "VALUES (true)");
            assertQuery("SELECT COUNT(*) > 0 FROM \"" + tableName + "$history\" WHERE snapshot_id IS NOT NULL", "VALUES (true)");
            assertQuery("SELECT COUNT(*) > 0 FROM \"" + tableName + "$manifests\"", "VALUES (true)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCopyOnWriteRowLevelDeleteFails()
    {
        String tableName = "test_cow_row_level_delete";
        @Language("RegExp") String errorMessage = "This connector only supports delete where one or more partitions are deleted entirely. To enable row level deletions, change the write.delete.mode table property to `merge-on-read`.";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer) WITH (\"format-version\" = '2', partitioning = ARRAY['id'], \"write.delete.mode\" = 'copy-on-write')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 10)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 1)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 5)", 1);

            // DELETE on a non-partition column requires row-level delete, which is not supported for COW.
            assertQueryFails("DELETE FROM " + tableName + " WHERE value = 1", errorMessage);

            // Verify the failed DELETE left all rows intact.
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 10), (2, 1), (3, 5)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
