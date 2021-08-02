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
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter;
import com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter.GluePartitionConverter;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.amazonaws.util.CollectionUtils.isNullOrEmpty;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestColumn;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestDatabase;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestPartition;
import static com.facebook.presto.hive.metastore.glue.TestingMetastoreObjects.getGlueTestTable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
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
        assertEquals(prestoTbl.getTableType().toString(), testTbl.getTableType());
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
        GluePartitionConverter converter = new GluePartitionConverter(testPartition.getDatabaseName(), testPartition.getTableName());
        com.facebook.presto.hive.metastore.Partition prestoPartition = converter.apply(testPartition);
        assertEquals(prestoPartition.getDatabaseName(), testPartition.getDatabaseName());
        assertEquals(prestoPartition.getTableName(), testPartition.getTableName());
        assertColumnList(prestoPartition.getColumns(), testPartition.getStorageDescriptor().getColumns());
        assertEquals(prestoPartition.getValues(), testPartition.getValues());
        assertStorage(prestoPartition.getStorage(), testPartition.getStorageDescriptor());
        assertEquals(prestoPartition.getParameters(), testPartition.getParameters());
    }

    @Test
    public void testPartitionConversionMemoization()
    {
        String fakeS3Location = "s3://some-fake-location";
        testPartition.getStorageDescriptor().setLocation(fakeS3Location);
        //  Second partition to convert with equal (but not aliased) values
        Partition partitionTwo = getGlueTestPartition(testPartition.getDatabaseName(), testPartition.getTableName(), new ArrayList<>(testPartition.getValues()));
        //  Ensure storage fields are equal but not aliased as well
        partitionTwo.getStorageDescriptor().setColumns(new ArrayList<>(testPartition.getStorageDescriptor().getColumns()));
        partitionTwo.getStorageDescriptor().setBucketColumns(new ArrayList<>(testPartition.getStorageDescriptor().getBucketColumns()));
        partitionTwo.getStorageDescriptor().setLocation("" + fakeS3Location);
        partitionTwo.getStorageDescriptor().setInputFormat("" + testPartition.getStorageDescriptor().getInputFormat());
        partitionTwo.getStorageDescriptor().setOutputFormat("" + testPartition.getStorageDescriptor().getOutputFormat());
        partitionTwo.getStorageDescriptor().setParameters(new HashMap<>(testPartition.getStorageDescriptor().getParameters()));

        GluePartitionConverter converter = new GluePartitionConverter(testDb.getName(), testTbl.getName());
        com.facebook.presto.hive.metastore.Partition prestoPartition = converter.apply(testPartition);
        com.facebook.presto.hive.metastore.Partition prestoPartition2 = converter.apply(partitionTwo);

        assertNotSame(prestoPartition, prestoPartition2);
        assertSame(prestoPartition2.getDatabaseName(), prestoPartition.getDatabaseName());
        assertSame(prestoPartition2.getTableName(), prestoPartition.getTableName());
        assertSame(prestoPartition2.getColumns(), prestoPartition.getColumns());
        assertSame(prestoPartition2.getParameters(), prestoPartition.getParameters());
        assertNotSame(prestoPartition2.getValues(), prestoPartition.getValues());

        Storage storage = prestoPartition.getStorage();
        Storage storage2 = prestoPartition2.getStorage();

        assertSame(storage2.getStorageFormat(), storage.getStorageFormat());
        assertSame(storage2.getBucketProperty(), storage.getBucketProperty());
        assertSame(storage2.getSerdeParameters(), storage.getSerdeParameters());
        assertNotSame(storage2.getLocation(), storage.getLocation());
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
        assertNotNull(new GluePartitionConverter(testDb.getName(), testTbl.getName()).apply(testPartition).getParameters());
    }

    @Test
    public void testConvertTableWithoutTableType()
    {
        Table table = getGlueTestTable(testDb.getName());
        table.setTableType(null);
        com.facebook.presto.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(table, testDb.getName());
        assertEquals(prestoTable.getTableType(), EXTERNAL_TABLE);
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
