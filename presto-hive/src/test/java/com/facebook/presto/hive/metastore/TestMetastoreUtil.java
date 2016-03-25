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

package com.facebook.presto.hive.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class TestMetastoreUtil
{
    private static final StorageDescriptor TEST_STORAGE_DESCRIPTOR = new StorageDescriptor(
            ImmutableList.of(
                    new FieldSchema("col1", "bigint", "comment1"),
                    new FieldSchema("col2", "binary", null),
                    new FieldSchema("col3", "string", null)),
            "hdfs://VOL1:9000/db_name/table_name",
            "com.facebook.hive.orc.OrcInputFormat",
            "com.facebook.hive.orc.OrcOutputFormat",
            false,
            100,
            new SerDeInfo("table_name", "com.facebook.hive.orc.OrcSerde", ImmutableMap.of("sdk1", "sdv1", "sdk2", "sdv2")),
            ImmutableList.of("col2", "col3"),
            null,
            ImmutableMap.of());
    private static final org.apache.hadoop.hive.metastore.api.Table TEST_TABLE = new org.apache.hadoop.hive.metastore.api.Table(
            "table_name",
            "db_name",
            "owner_name",
            0,
            0,
            0,
            TEST_STORAGE_DESCRIPTOR,
            ImmutableList.of(
                    new FieldSchema("pk1", "string", "comment pk1"),
                    new FieldSchema("pk2", "string", null)),
            ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"),
            "view original text",
            "view extended text",
            "MANAGED_TABLE");
    private static final org.apache.hadoop.hive.metastore.api.Partition TEST_PARTITION = new org.apache.hadoop.hive.metastore.api.Partition(
            ImmutableList.of("pk1v", "pk2v"),
            "db_name",
            "table_name",
            0,
            0,
            TEST_STORAGE_DESCRIPTOR,
            ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"));
    private static final StorageDescriptor TEST_STORAGE_DESCRIPTOR_WITH_UNSUPPORTED_FIELDS = new StorageDescriptor(
            ImmutableList.of(
                    new FieldSchema("col1", "bigint", "comment1"),
                    new FieldSchema("col2", "binary", null),
                    new FieldSchema("col3", "string", null)),
            "hdfs://VOL1:9000/db_name/table_name",
            "com.facebook.hive.orc.OrcInputFormat",
            "com.facebook.hive.orc.OrcOutputFormat",
            false,
            100,
            new SerDeInfo("table_name", "com.facebook.hive.orc.OrcSerde", ImmutableMap.of("sdk1", "sdv1", "sdk2", "sdv2")),
            ImmutableList.of("col2", "col3"),
            ImmutableList.of(new Order("col2", 0), new Order("col3", 1)),
            ImmutableMap.of("sk1", "sv1"));
    private static final org.apache.hadoop.hive.metastore.api.Table TEST_TABLE_WITH_UNSUPPORTED_FIELDS = new org.apache.hadoop.hive.metastore.api.Table(
            "table_name",
            "db_name",
            "owner_name",
            1234567890,
            1234567891,
            34,
            TEST_STORAGE_DESCRIPTOR_WITH_UNSUPPORTED_FIELDS,
            ImmutableList.of(
                    new FieldSchema("pk1", "string", "comment pk1"),
                    new FieldSchema("pk2", "string", null)),
            ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"),
            "view original text",
            "view extended text",
            "MANAGED_TABLE");
    private static final org.apache.hadoop.hive.metastore.api.Partition TEST_PARTITION_WITH_UNSUPPORTED_FIELDS = new org.apache.hadoop.hive.metastore.api.Partition(
            ImmutableList.of("pk1v", "pk2v"),
            "db_name",
            "table_name",
            1234567892,
            1234567893,
            TEST_STORAGE_DESCRIPTOR_WITH_UNSUPPORTED_FIELDS,
            ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"));

    @Test
    public void testTableRoundTrip()
            throws Exception
    {
        Table table = MetastoreUtil.fromMetastoreApiTable(TEST_TABLE);
        org.apache.hadoop.hive.metastore.api.Table metastoreApiTable = MetastoreUtil.toMetastoreApiTable(table, null);
        assertEquals(metastoreApiTable, TEST_TABLE);
    }

    @Test
    public void testPartitionRoundTrip()
    {
        Partition partition = MetastoreUtil.fromMetastoreApiPartition(TEST_PARTITION);
        org.apache.hadoop.hive.metastore.api.Partition metastoreApiPartition = MetastoreUtil.toMetastoreApiPartition(partition);
        assertEquals(metastoreApiPartition, TEST_PARTITION);
    }

    @Test
    public void testHiveSchemaTable()
    {
        Properties expected = MetaStoreUtils.getTableMetadata(TEST_TABLE_WITH_UNSUPPORTED_FIELDS);
        Properties actual = MetastoreUtil.getHiveSchema(MetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS));
        assertEquals(actual, expected);
    }

    @Test
    public void testHiveSchemaPartition()
    {
        Properties expected = MetaStoreUtils.getPartitionMetadata(TEST_PARTITION_WITH_UNSUPPORTED_FIELDS, TEST_TABLE_WITH_UNSUPPORTED_FIELDS);
        Properties actual = MetastoreUtil.getHiveSchema(MetastoreUtil.fromMetastoreApiPartition(TEST_PARTITION_WITH_UNSUPPORTED_FIELDS), MetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS));
        assertEquals(actual, expected);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Writing to sorted and/or skewed table/partition is not supported")
    public void testTableRoundTripUnsupported()
            throws Exception
    {
        Table table = MetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS);
        MetastoreUtil.toMetastoreApiTable(table, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Writing to sorted and/or skewed table/partition is not supported")
    public void testPartitionRoundTripUnsupported()
    {
        Partition partition = MetastoreUtil.fromMetastoreApiPartition(TEST_PARTITION_WITH_UNSUPPORTED_FIELDS);
        MetastoreUtil.toMetastoreApiPartition(partition);
    }
}
