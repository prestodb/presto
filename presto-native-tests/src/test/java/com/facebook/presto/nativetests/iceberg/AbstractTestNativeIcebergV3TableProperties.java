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

import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * ALTER TABLE ... SET PROPERTIES on V3 tables: every updatable property is persisted and leaves
 * the table readable and writable on native workers; other properties are rejected.
 */
public abstract class AbstractTestNativeIcebergV3TableProperties
        extends AbstractNativeIcebergV3Test
{
    protected AbstractTestNativeIcebergV3TableProperties(String storageFormat)
    {
        super(storageFormat);
    }

    @Test
    public void testSetUpdatableProperties()
    {
        assertPropertyUpdate("props_commit_retries", "commit.retry.num-retries", "9", "9");
        assertPropertyUpdate("props_split_size", "read.split.target-size", "1048576", "1048576");
        assertPropertyUpdate("props_previous_versions", "write.metadata.previous-versions-max", "5", "5");
        assertPropertyUpdate("props_delete_after_commit", "write.metadata.delete-after-commit.enabled", "true", "true");
    }

    private void assertPropertyUpdate(String table, String property, String value, String expected)
    {
        createV3Table(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a')", 1);
        assertUpdate(format("ALTER TABLE %s SET PROPERTIES (\"%s\" = %s)", table, property, value));
        assertQuery(format("SELECT value FROM \"%s$properties\" WHERE key = '%s'", table, property), format("VALUES '%s'", expected));
        assertEquals(formatVersion(table), 3, property + " must not change the format version");
        assertUpdate("INSERT INTO " + table + " VALUES (2, 'b')", 1);
        assertQuery("SELECT * FROM " + table, "VALUES (1, 'a'), (2, 'b')");
        dropTable(table);
    }

    @Test
    public void testPreviousVersionsMaxPrunesMetadataLog()
    {
        String table = "props_prune_metadata";
        createV3Table(table, "id integer");
        assertUpdate(format("ALTER TABLE %s SET PROPERTIES (\"write.metadata.previous-versions-max\" = 1, \"write.metadata.delete-after-commit.enabled\" = true)", table));
        for (int i = 0; i < 5; i++) {
            assertUpdate(format("INSERT INTO %s VALUES (%s)", table, i), 1);
        }
        assertEquals(scalarLong(format("SELECT count(*) FROM \"%s$metadata_log_entries\"", table)), 2L);
        assertQuery("SELECT count(*) FROM " + table, "VALUES 5");
        dropTable(table);
    }

    @Test
    public void testSmallSplitTargetSizeKeepsResults()
    {
        String table = "props_small_splits";
        createV3Table(table, "id bigint, payload varchar");
        assertUpdate(format("INSERT INTO %s SELECT x, 'payload_' || CAST(x AS varchar) FROM (%s)", table, rangeOneTo(20_000)), 20_000);
        assertUpdate(format("ALTER TABLE %s SET PROPERTIES (\"read.split.target-size\" = 1024)", table));
        assertQuery("SELECT count(*), sum(id), count(DISTINCT payload) FROM " + table, "VALUES (20000, 200010000, 20000)");
        dropTable(table);
    }

    @Test
    public void testUnsupportedPropertyUpdates()
    {
        String table = "props_unsupported";
        createV3Table(table, "id integer");
        assertQueryFails(format("ALTER TABLE %s SET PROPERTIES (\"format-version\" = '3')", table), "Updating property format-version is not supported currently");
        assertQueryFails(format("ALTER TABLE %s SET PROPERTIES (\"write.format.default\" = 'ORC')", table), "Updating property write.format.default is not supported currently");
        assertQueryFails(format("ALTER TABLE %s SET PROPERTIES (partitioning = ARRAY['id'])", table), "Updating property partitioning is not supported currently");
        assertQueryFails(format("ALTER TABLE %s SET PROPERTIES (\"no.such.property\" = 'x')", table), ".*no.such.property.*");
        assertEquals(formatVersion(table), 3);
        dropTable(table);
    }
}
