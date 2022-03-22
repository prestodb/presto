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

import com.facebook.presto.hive.PartitionMutator;
import com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.hive.metastore.MetastoreUtil.reconstructPartitionSchema;
import static org.apache.hadoop.hive.serde.serdeConstants.COLUMN_NAME_DELIMITER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestHiveMetastoreUtil
{
    private static final List<FieldSchema> TEST_SCHEMA = ImmutableList.of(
            new FieldSchema("col1", "bigint", "comment1"),
            new FieldSchema("col2", "binary", null),
            new FieldSchema("col3", "string", null));
    private static final StorageDescriptor TEST_STORAGE_DESCRIPTOR = new StorageDescriptor(
            TEST_SCHEMA,
            "hdfs://VOL1:9000/db_name/table_name",
            "com.facebook.hive.orc.OrcInputFormat",
            "com.facebook.hive.orc.OrcOutputFormat",
            false,
            100,
            new SerDeInfo("table_name", "com.facebook.hive.orc.OrcSerde", ImmutableMap.of("sdk1", "sdv1", "sdk2", "sdv2")),
            ImmutableList.of("col2", "col3"),
            ImmutableList.of(new Order("col2", 1)),
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

    static {
        TEST_TABLE.setPrivileges(new PrincipalPrivilegeSet(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()));
    }

    private static final org.apache.hadoop.hive.metastore.api.Partition TEST_PARTITION = new org.apache.hadoop.hive.metastore.api.Partition(
            ImmutableList.of("pk1v", "pk2v"),
            "db_name",
            "table_name",
            0,
            0,
            TEST_STORAGE_DESCRIPTOR,
            ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"));
    private static final StorageDescriptor TEST_STORAGE_DESCRIPTOR_WITH_UNSUPPORTED_FIELDS = new StorageDescriptor(
            TEST_SCHEMA,
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
    private static final PartitionMutator TEST_PARTITION_VERSION_FETCHER = new HivePartitionMutator();

    static {
        TEST_STORAGE_DESCRIPTOR_WITH_UNSUPPORTED_FIELDS.setSkewedInfo(new SkewedInfo(
                ImmutableList.of("col1"),
                ImmutableList.of(ImmutableList.of("val1")),
                ImmutableMap.of(ImmutableList.of("val1"), "loc1")));
    }

    @Test
    public void testTableRoundTrip()
    {
        Table table = ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE, TEST_SCHEMA);
        PrincipalPrivileges privileges = new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of());
        org.apache.hadoop.hive.metastore.api.Table metastoreApiTable = ThriftMetastoreUtil.toMetastoreApiTable(table, privileges);
        assertEquals(metastoreApiTable, TEST_TABLE);
    }

    @Test
    public void testPartitionRoundTrip()
    {
        Partition partition = ThriftMetastoreUtil.fromMetastoreApiPartition(TEST_PARTITION, TEST_PARTITION_VERSION_FETCHER);
        org.apache.hadoop.hive.metastore.api.Partition metastoreApiPartition = ThriftMetastoreUtil.toMetastoreApiPartition(partition);
        assertEquals(metastoreApiPartition, TEST_PARTITION);
    }

    @Test
    public void testHiveSchemaTable()
    {
        Properties expected = MetaStoreUtils.getTableMetadata(TEST_TABLE_WITH_UNSUPPORTED_FIELDS);
        expected.remove(COLUMN_NAME_DELIMITER);
        Properties actual = getHiveSchema(ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS, TEST_SCHEMA));
        assertEquals(actual, expected);
    }

    @Test
    public void testHiveSchemaPartition()
    {
        Properties expected = MetaStoreUtils.getPartitionMetadata(TEST_PARTITION_WITH_UNSUPPORTED_FIELDS, TEST_TABLE_WITH_UNSUPPORTED_FIELDS);
        expected.remove(COLUMN_NAME_DELIMITER);
        Properties actual = getHiveSchema(ThriftMetastoreUtil.fromMetastoreApiPartition(TEST_PARTITION_WITH_UNSUPPORTED_FIELDS, TEST_PARTITION_VERSION_FETCHER), ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS, TEST_SCHEMA));
        assertEquals(actual, expected);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Writing to skewed table/partition is not supported")
    public void testTableRoundTripUnsupported()
    {
        Table table = ThriftMetastoreUtil.fromMetastoreApiTable(TEST_TABLE_WITH_UNSUPPORTED_FIELDS, TEST_SCHEMA);
        ThriftMetastoreUtil.toMetastoreApiTable(table, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Writing to skewed table/partition is not supported")
    public void testPartitionRoundTripUnsupported()
    {
        Partition partition = ThriftMetastoreUtil.fromMetastoreApiPartition(TEST_PARTITION_WITH_UNSUPPORTED_FIELDS, TEST_PARTITION_VERSION_FETCHER);
        ThriftMetastoreUtil.toMetastoreApiPartition(partition);
    }

    @Test
    public void testReconstructPartitionSchema()
    {
        Column c1 = new Column("_c1", HIVE_STRING, Optional.empty());
        Column c2 = new Column("_c2", HIVE_INT, Optional.empty());
        Column c3 = new Column("_c3", HIVE_DOUBLE, Optional.empty());
        Column c4 = new Column("_c4", HIVE_DATE, Optional.empty());

        assertEquals(reconstructPartitionSchema(ImmutableList.of(), 0, ImmutableMap.of(), Optional.empty()), ImmutableList.of());
        assertEquals(reconstructPartitionSchema(ImmutableList.of(c1), 0, ImmutableMap.of(), Optional.empty()), ImmutableList.of());
        assertEquals(reconstructPartitionSchema(ImmutableList.of(c1), 1, ImmutableMap.of(), Optional.empty()), ImmutableList.of(c1));
        assertEquals(reconstructPartitionSchema(ImmutableList.of(c1, c2), 1, ImmutableMap.of(), Optional.empty()), ImmutableList.of(c1));
        assertEquals(reconstructPartitionSchema(ImmutableList.of(c1, c2), 3, ImmutableMap.of(2, c3), Optional.empty()), ImmutableList.of(c1, c2, c3));
        assertEquals(reconstructPartitionSchema(ImmutableList.of(c1, c2, c3), 3, ImmutableMap.of(1, c4), Optional.empty()), ImmutableList.of(c1, c4, c3));

        assertThatThrownBy(() -> reconstructPartitionSchema(ImmutableList.of(), 1, ImmutableMap.of(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reconstructPartitionSchema(ImmutableList.of(c1), 2, ImmutableMap.of(), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reconstructPartitionSchema(ImmutableList.of(c1), 2, ImmutableMap.of(0, c2), Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testExtractPartitionValues()
    {
        String datePartition = "20201221";
        String countryPartition = "united_states";
        assertEquals(extractPartitionValues("datePartition=20201221/countryPartition=united_states", Optional.of(ImmutableList.of("datePartition", "countryPartition"))), ImmutableList.of(datePartition, countryPartition));
        assertEquals(extractPartitionValues("countryPartition=united_states/datePartition=20201221", Optional.of(ImmutableList.of("datePartition", "countryPartition"))), ImmutableList.of(datePartition, countryPartition));
        assertEquals(extractPartitionValues("datePartition=20201221/countryPartition=united_states"), ImmutableList.of(datePartition, countryPartition));
        assertEquals(extractPartitionValues("countryPartition=united_states/datePartition=20201221"), ImmutableList.of(countryPartition, datePartition));
    }
}
