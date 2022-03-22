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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.ColumnEncryptionInformation.fromHiveProperty;
import static com.facebook.presto.hive.DwrfTableEncryptionProperties.forPerColumn;
import static com.facebook.presto.hive.DwrfTableEncryptionProperties.forTable;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveStorageFormat.DWRF;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.TestDwrfEncryptionInformationSource.TEST_EXTRA_METADATA;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAbstractDwrfEncryptionInformationSource
{
    private static final HiveType STRUCT_TYPE = HiveType.valueOf("struct<a:string,b:struct<b1:map<string,bigint>,b2:bigint>>");
    private final EncryptionInformationSource encryptionInformationSource = new TestDwrfEncryptionInformationSource();

    private static Table createTable(HiveStorageFormat storageFormat, Optional<DwrfTableEncryptionProperties> tableEncryptionProperties, boolean isPartitioned)
    {
        return new Table(
                "dbName",
                "tableName",
                "owner",
                MANAGED_TABLE,
                new Storage(
                        fromHiveStorageFormat(storageFormat),
                        "location",
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                ImmutableList.of(
                        new Column("col_string", HIVE_STRING, Optional.empty()),
                        new Column("col_bigint", HIVE_LONG, Optional.empty()),
                        new Column("col_map", HiveType.valueOf("map<string,string>"), Optional.empty()),
                        new Column("col_struct", STRUCT_TYPE, Optional.empty())),
                isPartitioned ? ImmutableList.of(new Column("ds", HIVE_STRING, Optional.empty())) : ImmutableList.of(),
                tableEncryptionProperties.map(DwrfTableEncryptionProperties::toHiveProperties).orElse(ImmutableMap.of()),
                Optional.empty(),
                Optional.empty());
    }

    @Test
    public void testNotDwrfTable()
    {
        Table table = createTable(ORC, Optional.of(forTable("foo", "AES_GCM_256", "TEST")), true);
        assertFalse(encryptionInformationSource.getReadEncryptionInformation(SESSION, table, Optional.of(ImmutableSet.of()), ImmutableMap.of()).isPresent());
        assertFalse(encryptionInformationSource.getReadEncryptionInformation(SESSION, table, Optional.of(ImmutableSet.of())).isPresent());
        assertFalse(encryptionInformationSource.getWriteEncryptionInformation(SESSION, new NonDwrfTableEncryptionProperties(), "db", "table").isPresent());
    }

    @Test
    public void testNotEncryptedTable()
    {
        Table table = createTable(DWRF, Optional.empty(), true);
        assertFalse(encryptionInformationSource.getReadEncryptionInformation(SESSION, table, Optional.of(ImmutableSet.of()), ImmutableMap.of()).isPresent());
        assertFalse(encryptionInformationSource.getReadEncryptionInformation(SESSION, table, Optional.of(ImmutableSet.of())).isPresent());
    }

    @Test
    public void testGetReadEncryptionInformationForPartitionedTableWithTableLevelEncryption()
    {
        Table table = createTable(DWRF, Optional.of(forTable("table_level_key", "algo", "provider")), true);
        Optional<Map<String, EncryptionInformation>> encryptionInformation = encryptionInformationSource.getReadEncryptionInformation(
                SESSION,
                table,
                Optional.of(ImmutableSet.of(
                        // hiveColumnIndex value does not matter in this test
                        new HiveColumnHandle("col_bigint", HIVE_LONG, HIVE_LONG.getTypeSignature(), 0, REGULAR, Optional.empty(), Optional.empty()),
                        new HiveColumnHandle(
                                "col_struct",
                                STRUCT_TYPE,
                                STRUCT_TYPE.getTypeSignature(),
                                0,
                                REGULAR,
                                Optional.empty(),
                                ImmutableList.of(new Subfield("col_struct.a"), new Subfield("col_struct.b.b2")),
                                Optional.empty()))),
                ImmutableMap.of(
                        "ds=2020-01-01", new Partition("dbName", "tableName", ImmutableList.of("2020-01-01"), table.getStorage(), table.getDataColumns(), ImmutableMap.of(), Optional.empty(), false, true, 0),
                        "ds=2020-01-02", new Partition("dbName", "tableName", ImmutableList.of("2020-01-02"), table.getStorage(), table.getDataColumns(), ImmutableMap.of(), Optional.empty(), false, true, 0)));

        assertTrue(encryptionInformation.isPresent());
        assertEquals(
                encryptionInformation.get(),
                ImmutableMap.of(
                        "ds=2020-01-01", EncryptionInformation.fromEncryptionMetadata(DwrfEncryptionMetadata.forTable("table_level_key".getBytes(), ImmutableMap.of(TEST_EXTRA_METADATA, "ds=2020-01-01"), "algo", "provider")),
                        "ds=2020-01-02", EncryptionInformation.fromEncryptionMetadata(DwrfEncryptionMetadata.forTable("table_level_key".getBytes(), ImmutableMap.of(TEST_EXTRA_METADATA, "ds=2020-01-02"), "algo", "provider"))));
    }

    @Test
    public void testGetReadEncryptionInformationForPartitionedTableWithTableLevelEncryptionAndNoRequestedColumns()
    {
        Table table = createTable(DWRF, Optional.of(forTable("key1", "algo", "provider")), true);
        Optional<Map<String, EncryptionInformation>> encryptionInformation = encryptionInformationSource.getReadEncryptionInformation(
                SESSION,
                table,
                Optional.of(ImmutableSet.of()),
                ImmutableMap.of(
                        "ds=2020-01-01", new Partition("dbName", "tableName", ImmutableList.of("2020-01-01"), table.getStorage(), table.getDataColumns(), ImmutableMap.of(), Optional.empty(), false, true, 0),
                        "ds=2020-01-02", new Partition("dbName", "tableName", ImmutableList.of("2020-01-02"), table.getStorage(), table.getDataColumns(), ImmutableMap.of(), Optional.empty(), false, true, 0)));

        assertTrue(encryptionInformation.isPresent());
        assertEquals(
                encryptionInformation.get(),
                ImmutableMap.of(
                        "ds=2020-01-01", EncryptionInformation.fromEncryptionMetadata(DwrfEncryptionMetadata.forPerField(ImmutableMap.of(), ImmutableMap.of(TEST_EXTRA_METADATA, "ds=2020-01-01"), "algo", "provider")),
                        "ds=2020-01-02", EncryptionInformation.fromEncryptionMetadata(DwrfEncryptionMetadata.forPerField(ImmutableMap.of(), ImmutableMap.of(TEST_EXTRA_METADATA, "ds=2020-01-02"), "algo", "provider"))));
    }

    @Test
    public void testGetReadEncryptionInformationForPartitionedTableWithColumnLevelEncryption()
    {
        Table table = createTable(DWRF, Optional.of(forPerColumn(fromHiveProperty("key1:col_string,col_struct.b.b2;key2:col_bigint,col_struct.a"), "algo", "provider")), true);
        Optional<Map<String, EncryptionInformation>> encryptionInformation = encryptionInformationSource.getReadEncryptionInformation(
                SESSION,
                table,
                Optional.of(ImmutableSet.of(
                        // hiveColumnIndex value does not matter in this test
                        new HiveColumnHandle("col_bigint", HIVE_LONG, HIVE_LONG.getTypeSignature(), 0, REGULAR, Optional.empty(), Optional.empty()),
                        new HiveColumnHandle("col_map", HIVE_LONG, HIVE_LONG.getTypeSignature(), 0, REGULAR, Optional.empty(), Optional.empty()),
                        new HiveColumnHandle(
                                "col_struct",
                                STRUCT_TYPE,
                                STRUCT_TYPE.getTypeSignature(),
                                0,
                                REGULAR,
                                Optional.empty(),
                                ImmutableList.of(new Subfield("col_struct.a"), new Subfield("col_struct.b.b2")),
                                Optional.empty()))),
                ImmutableMap.of(
                        "ds=2020-01-01", new Partition("dbName", "tableName", ImmutableList.of("2020-01-01"), table.getStorage(), table.getDataColumns(), ImmutableMap.of(), Optional.empty(), false, true, 0),
                        "ds=2020-01-02", new Partition("dbName", "tableName", ImmutableList.of("2020-01-02"), table.getStorage(), table.getDataColumns(), ImmutableMap.of(), Optional.empty(), false, true, 0)));

        Map<String, byte[]> expectedFieldToKeyData = ImmutableMap.of("col_bigint", "key2".getBytes(), "col_struct.a", "key2".getBytes(), "col_struct.b.b2", "key1".getBytes());
        assertTrue(encryptionInformation.isPresent());
        assertEquals(
                encryptionInformation.get(),
                ImmutableMap.of(
                        "ds=2020-01-01", EncryptionInformation.fromEncryptionMetadata(DwrfEncryptionMetadata.forPerField(expectedFieldToKeyData, ImmutableMap.of(TEST_EXTRA_METADATA, "ds=2020-01-01"), "algo", "provider")),
                        "ds=2020-01-02", EncryptionInformation.fromEncryptionMetadata(DwrfEncryptionMetadata.forPerField(expectedFieldToKeyData, ImmutableMap.of(TEST_EXTRA_METADATA, "ds=2020-01-02"), "algo", "provider"))));
    }

    @Test
    public void testGetReadEncryptionInformationForUnPartitionedTableWithColumnLevelEncryption()
    {
        Table table = createTable(DWRF, Optional.of(forPerColumn(fromHiveProperty("key1:col_string,col_struct.b.b2;key2:col_bigint,col_struct.a"), "algo", "provider")), false);
        Optional<EncryptionInformation> encryptionInformation = encryptionInformationSource.getReadEncryptionInformation(
                SESSION,
                table,
                Optional.of(ImmutableSet.of(
                        // hiveColumnIndex value does not matter in this test
                        new HiveColumnHandle("col_bigint", HIVE_LONG, HIVE_LONG.getTypeSignature(), 0, REGULAR, Optional.empty(), Optional.empty()),
                        new HiveColumnHandle("col_map", HIVE_LONG, HIVE_LONG.getTypeSignature(), 0, REGULAR, Optional.empty(), Optional.empty()),
                        new HiveColumnHandle(
                                "col_struct",
                                STRUCT_TYPE,
                                STRUCT_TYPE.getTypeSignature(),
                                0,
                                REGULAR,
                                Optional.empty(),
                                ImmutableList.of(new Subfield("col_struct.a"), new Subfield("col_struct.b.b2")),
                                Optional.empty()))));

        Map<String, byte[]> expectedFieldToKeyData = ImmutableMap.of("col_bigint", "key2".getBytes(), "col_struct.a", "key2".getBytes(), "col_struct.b.b2", "key1".getBytes());
        assertTrue(encryptionInformation.isPresent());
        assertEquals(
                encryptionInformation.get(),
                EncryptionInformation.fromEncryptionMetadata(DwrfEncryptionMetadata.forPerField(expectedFieldToKeyData, ImmutableMap.of(TEST_EXTRA_METADATA, table.getTableName()), "algo", "provider")));
    }

    @Test
    public void testGetWriteEncryptionInformation()
    {
        Optional<EncryptionInformation> encryptionInformation = encryptionInformationSource.getWriteEncryptionInformation(SESSION, forTable("table_level", "algo", "provider"), "dbName", "tableName");
        assertTrue(encryptionInformation.isPresent());
        assertEquals(
                encryptionInformation.get(),
                EncryptionInformation.fromEncryptionMetadata(DwrfEncryptionMetadata.forTable("table_level".getBytes(), ImmutableMap.of(TEST_EXTRA_METADATA, "algo"), "algo", "provider")));
    }

    private static final class NonDwrfTableEncryptionProperties
            extends TableEncryptionProperties
    {
        NonDwrfTableEncryptionProperties()
        {
            super(Optional.of("foo"), Optional.empty());
        }
    }
}
