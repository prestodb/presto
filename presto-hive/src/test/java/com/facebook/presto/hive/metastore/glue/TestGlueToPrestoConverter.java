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

import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter;
import com.facebook.presto.hive.metastore.glue.converter.GlueToPrestoConverter.GluePartitionConverter;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static com.facebook.presto.hive.metastore.MetastoreUtil.DELTA_LAKE_PROVIDER;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_VALUE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.SPARK_TABLE_PROVIDER_KEY;
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
import static software.amazon.awssdk.utils.CollectionUtils.isNullOrEmpty;

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
        testTbl = getGlueTestTable(testDb.name());
        testPartition = getGlueTestPartition(testDb.name(), testTbl.name(), ImmutableList.of("val1"));
    }

    @Test
    public void testConvertDatabase()
    {
        com.facebook.presto.hive.metastore.Database prestoDb = GlueToPrestoConverter.convertDatabase(testDb);
        assertEquals(prestoDb.getDatabaseName(), testDb.name());
        assertEquals(prestoDb.getLocation().get(), testDb.locationUri());
        assertEquals(prestoDb.getComment().get(), testDb.description());
        assertEquals(prestoDb.getParameters(), testDb.parameters());
        assertEquals(prestoDb.getOwnerName(), PUBLIC_OWNER);
        assertEquals(prestoDb.getOwnerType(), PrincipalType.ROLE);
    }

    @Test
    public void testConvertTable()
    {
        com.facebook.presto.hive.metastore.Table prestoTbl = GlueToPrestoConverter.convertTable(testTbl, testDb.name());
        assertEquals(prestoTbl.getTableName(), testTbl.name());
        assertEquals(prestoTbl.getDatabaseName(), testDb.name());
        assertEquals(prestoTbl.getTableType().toString(), testTbl.tableType());
        assertEquals(prestoTbl.getOwner(), testTbl.owner());
        assertEquals(prestoTbl.getParameters(), testTbl.parameters());
        assertColumnList(prestoTbl.getDataColumns(), testTbl.storageDescriptor().columns());
        assertColumnList(prestoTbl.getPartitionColumns(), testTbl.partitionKeys());
        assertStorage(prestoTbl.getStorage(), testTbl.storageDescriptor());
        assertEquals(prestoTbl.getViewOriginalText().get(), testTbl.viewOriginalText());
        assertEquals(prestoTbl.getViewExpandedText().get(), testTbl.viewExpandedText());
    }

    @Test
    public void testConvertTableNullPartitions()
    {
        testTbl = testTbl.toBuilder().partitionKeys((Collection<software.amazon.awssdk.services.glue.model.Column>) null).build();
        com.facebook.presto.hive.metastore.Table prestoTbl = GlueToPrestoConverter.convertTable(testTbl, testDb.name());
        assertTrue(prestoTbl.getPartitionColumns().isEmpty());
    }

    @Test
    public void testConvertTableUppercaseColumnType()
    {
        software.amazon.awssdk.services.glue.model.Column uppercaseCol = getGlueTestColumn().toBuilder().type("String").build();

        StorageDescriptor sd = testTbl.storageDescriptor();
        testTbl = testTbl.toBuilder().storageDescriptor(sd.toBuilder().columns(ImmutableList.of(uppercaseCol)).build()).build();
        GlueToPrestoConverter.convertTable(testTbl, testDb.name());
    }

    @Test
    public void testConvertPartition()
    {
        GluePartitionConverter converter = new GluePartitionConverter(testPartition.databaseName(), testPartition.tableName());
        com.facebook.presto.hive.metastore.Partition prestoPartition = converter.apply(testPartition);
        assertEquals(prestoPartition.getDatabaseName(), testPartition.databaseName());
        assertEquals(prestoPartition.getTableName(), testPartition.tableName());
        assertColumnList(prestoPartition.getColumns(), testPartition.storageDescriptor().columns());
        assertEquals(prestoPartition.getValues(), testPartition.values());
        assertStorage(prestoPartition.getStorage(), testPartition.storageDescriptor());
        assertEquals(prestoPartition.getParameters(), testPartition.parameters());
    }

    @Test
    public void testPartitionConversionMemoization()
    {
        String fakeS3Location = "s3://some-fake-location";

        StorageDescriptor sdPartition = testPartition.storageDescriptor();
        testPartition = testPartition.toBuilder().storageDescriptor(sdPartition.toBuilder().location(fakeS3Location).build()).build();

        //  Second partition to convert with equal (but not aliased) values
        Partition partitionTwo = getGlueTestPartition(testPartition.databaseName(), testPartition.tableName(), new ArrayList<>(testPartition.values()));
        //  Ensure storage fields are equal but not aliased as well
        StorageDescriptor sdPartitionTwo = partitionTwo.storageDescriptor();
        partitionTwo = partitionTwo.toBuilder().storageDescriptor(
                sdPartitionTwo.toBuilder()
                        .columns(new ArrayList<>(testPartition.storageDescriptor().columns()))
                        .bucketColumns(new ArrayList<>(testPartition.storageDescriptor().bucketColumns()))
                        .location("" + fakeS3Location)
                        .inputFormat("" + testPartition.storageDescriptor().inputFormat())
                        .outputFormat("" + testPartition.storageDescriptor().outputFormat())
                        .parameters(new HashMap<>(testPartition.storageDescriptor().parameters()))
                        .build()).build();

        GluePartitionConverter converter = new GluePartitionConverter(testDb.name(), testTbl.name());
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
        testDb = testDb.toBuilder().parameters(null).build();
        assertNotNull(GlueToPrestoConverter.convertDatabase(testDb).getParameters());
    }

    @Test
    public void testTableNullParameters()
    {
        StorageDescriptor sd = testTbl.storageDescriptor();
        SerDeInfo serDeInfo = sd.serdeInfo();
        testTbl = testTbl.toBuilder()
                .parameters(null)
                .storageDescriptor(sd.toBuilder().serdeInfo(serDeInfo.toBuilder().parameters(null).build()).build())
                .build();
        com.facebook.presto.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(testTbl, testDb.name());
        assertNotNull(prestoTable.getParameters());
        assertNotNull(prestoTable.getStorage().getSerdeParameters());
    }

    @Test
    public void testPartitionNullParameters()
    {
        testPartition = testPartition.toBuilder().parameters(null).build();
        assertNotNull(new GluePartitionConverter(testDb.name(), testTbl.name()).apply(testPartition).getParameters());
    }

    @Test
    public void testConvertTableWithoutTableType()
    {
        Table table = getGlueTestTable(testDb.name()).toBuilder().tableType(null).build();
        com.facebook.presto.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(table, testDb.name());
        assertEquals(prestoTable.getTableType(), EXTERNAL_TABLE);
    }

    @Test
    public void testIcebergTableNonNullStorageDescriptor()
    {
        testTbl = testTbl.toBuilder().parameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE)).build();
        assertNotNull(testTbl.storageDescriptor());
        com.facebook.presto.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(testTbl, testDb.name());
        assertEquals(prestoTable.getDataColumns().size(), 1);
    }

    @Test
    public void testDeltaTableNonNullStorageDescriptor()
    {
        testTbl = testTbl.toBuilder().parameters(ImmutableMap.of(SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER)).build();
        assertNotNull(testTbl.storageDescriptor());
        com.facebook.presto.hive.metastore.Table prestoTable = GlueToPrestoConverter.convertTable(testTbl, testDb.name());
        assertEquals(prestoTable.getDataColumns().size(), 1);
    }

    private static void assertColumnList(List<Column> actual, List<software.amazon.awssdk.services.glue.model.Column> expected)
    {
        if (expected == null) {
            assertNull(actual);
        }
        assertEquals(actual.size(), expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(Column actual, software.amazon.awssdk.services.glue.model.Column expected)
    {
        assertEquals(actual.getName(), expected.name());
        assertEquals(actual.getType().getHiveTypeName().toString(), expected.type());
        assertEquals(actual.getComment().get(), expected.comment());
    }

    private static void assertStorage(Storage actual, StorageDescriptor expected)
    {
        assertEquals(actual.getLocation(), expected.location());
        assertEquals(actual.getStorageFormat().getSerDe(), expected.serdeInfo().serializationLibrary());
        assertEquals(actual.getStorageFormat().getInputFormat(), expected.inputFormat());
        assertEquals(actual.getStorageFormat().getOutputFormat(), expected.outputFormat());
        if (!isNullOrEmpty(expected.bucketColumns())) {
            HiveBucketProperty bucketProperty = actual.getBucketProperty().get();
            assertEquals(bucketProperty.getBucketedBy(), expected.bucketColumns());
            assertEquals(bucketProperty.getBucketCount(), expected.numberOfBuckets().intValue());
        }
    }
}
