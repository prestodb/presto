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
package com.facebook.presto.hive.metastore.glue;

import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.PrincipalType;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.amazonaws.util.CollectionUtils.isNullOrEmpty;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestColumn;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestDatabase;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestPartition;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestTable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestGlueToPrestoConverter
{
    private static final String PUBLIC_OWNER = "PUBLIC";

    private Database testDb;
    private Table testTbl;
    private Partition testPartition;

    @BeforeMethod
    public void setup()
    {
        testDb = getGlueTestDatabase();
        testTbl = getGlueTestTable(testDb.getName());
        testPartition = getGlueTestPartition(testDb.getName(), testTbl.getName(), ImmutableList.of("val1"));
    }

    @Test
    public void testConvertDatabase()
    {
        com.facebook.presto.hive.metastore.Database prestoDb = GlueToPrestoConverter.convertDatabase(testDb);
        assertEquals(prestoDb.getDatabaseName(), testDb.getName());
        assertEquals(prestoDb.getLocation().get(), testDb.getLocationUri());
        assertEquals(prestoDb.getComment().get(), testDb.getDescription());
        assertEquals(prestoDb.getParameters(), testDb.getParameters());
        assertEquals(prestoDb.getOwnerName(), PUBLIC_OWNER);
        assertEquals(prestoDb.getOwnerType(), PrincipalType.ROLE);
    }

    @Test
    public void testConvertTable()
    {
        com.facebook.presto.hive.metastore.Table prestoTbl = GlueToPrestoConverter.convertTable(testTbl, testDb.getName());
        assertEquals(prestoTbl.getTableName(), testTbl.getName());
        assertEquals(prestoTbl.getDatabaseName(), testDb.getName());
        assertEquals(prestoTbl.getTableType(), testTbl.getTableType());
        assertEquals(prestoTbl.getOwner(), testTbl.getOwner());
        assertEquals(prestoTbl.getParameters(), testTbl.getParameters());
        assertColumnList(prestoTbl.getDataColumns(), testTbl.getStorageDescriptor().getColumns());
        assertColumnList(prestoTbl.getPartitionColumns(), testTbl.getPartitionKeys());
        assertStorage(prestoTbl.getStorage(), testTbl.getStorageDescriptor());
        assertEquals(prestoTbl.getViewOriginalText().get(), testTbl.getViewOriginalText());
        assertEquals(prestoTbl.getViewExpandedText().get(), testTbl.getViewExpandedText());
    }

    @Test
    public void testConvertTableNullPartitions()
    {
        testTbl.setPartitionKeys(null);
        com.facebook.presto.hive.metastore.Table prestoTbl = GlueToPrestoConverter.convertTable(testTbl, testDb.getName());
        assertTrue(prestoTbl.getPartitionColumns().isEmpty());
    }

    @Test
    public void testConvertTableUppercaseColumnType()
    {
        com.amazonaws.services.glue.model.Column uppercaseCol = getGlueTestColumn().withType("String");
        testTbl.getStorageDescriptor().setColumns(ImmutableList.of(uppercaseCol));
        GlueToPrestoConverter.convertTable(testTbl, testDb.getName());
    }

    @Test
    public void testConvertPartition()
    {
        com.facebook.presto.hive.metastore.Partition prestoPartition = GlueToPrestoConverter.convertPartition(testPartition);
        assertEquals(prestoPartition.getDatabaseName(), testPartition.getDatabaseName());
        assertEquals(prestoPartition.getTableName(), testPartition.getTableName());
        assertColumnList(prestoPartition.getColumns(), testPartition.getStorageDescriptor().getColumns());
        assertEquals(prestoPartition.getValues(), testPartition.getValues());
        assertStorage(prestoPartition.getStorage(), testPartition.getStorageDescriptor());
        assertEquals(prestoPartition.getParameters(), testPartition.getParameters());
    }

    @Test
    public void testDatabaseNullParameters()
    {
        testDb.setParameters(null);
        assertNotNull(GlueToPrestoConverter.convertDatabase(testDb).getParameters());
    }

    @Test
    public void testTableNullParameters()
    {
        testTbl.setParameters(null);
        testTbl.getStorageDescriptor().getSerdeInfo().setParameters(null);
        com.facebook.presto.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(testTbl, testDb.getName());
        assertNotNull(prestoTable.getParameters());
        assertNotNull(prestoTable.getStorage().getSerdeParameters());
    }

    @Test
    public void testPartitionNullParameters()
    {
        testPartition.setParameters(null);
        assertNotNull(GlueToPrestoConverter.convertPartition(testPartition).getParameters());
    }

    private static void assertColumnList(List<Column> actual, List<com.amazonaws.services.glue.model.Column> expected)
    {
        if (expected == null) {
            assertNull(actual);
        }
        assertEquals(actual.size(), expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(Column actual, com.amazonaws.services.glue.model.Column expected)
    {
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getType().getHiveTypeName().toString(), expected.getType());
        assertEquals(actual.getComment().get(), expected.getComment());
    }

    private static void assertStorage(Storage actual, StorageDescriptor expected)
    {
        assertEquals(actual.getLocation(), expected.getLocation());
        assertEquals(actual.getStorageFormat().getSerDe(), expected.getSerdeInfo().getSerializationLibrary());
        assertEquals(actual.getStorageFormat().getInputFormat(), expected.getInputFormat());
        assertEquals(actual.getStorageFormat().getOutputFormat(), expected.getOutputFormat());
        if (!isNullOrEmpty(expected.getBucketColumns())) {
            HiveBucketProperty bucketProperty = actual.getBucketProperty().get();
            assertEquals(bucketProperty.getBucketedBy(), expected.getBucketColumns());
            assertEquals(bucketProperty.getBucketCount(), expected.getNumberOfBuckets().intValue());
        }
    }
}
