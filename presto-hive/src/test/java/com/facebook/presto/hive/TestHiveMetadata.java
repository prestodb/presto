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

import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HiveColumnConverter;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.testng.annotations.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.HiveMetadata.createPredicate;
import static com.facebook.presto.hive.HiveMetadata.decodePreferredOrderingColumnsFromStorage;
import static com.facebook.presto.hive.HiveMetadata.encodePreferredOrderingColumns;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveTableProperties.PREFERRED_ORDERING_COLUMNS;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.metastore.SortingColumn.Order.ASCENDING;
import static com.facebook.presto.hive.metastore.SortingColumn.Order.DESCENDING;
import static com.facebook.presto.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static org.testng.Assert.assertEquals;

public class TestHiveMetadata
{
    private static final HiveColumnHandle TEST_COLUMN_HANDLE = new HiveColumnHandle(
            "test",
            HiveType.HIVE_STRING,
            TypeSignature.parseTypeSignature("varchar"),
            0,
            HiveColumnHandle.ColumnType.PARTITION_KEY,
            Optional.empty(),
            Optional.empty());

    @Test
    public void testColumnMetadataGetter()
    {
        TypeManager mockTypeManager = new TestingTypeManager();
        Column column1 = new Column("c1", HIVE_INT, Optional.empty(), Optional.of("some-metadata"));
        HiveColumnHandle hiveColumnHandle1 = new HiveColumnHandle(
                column1.getName(),
                HiveType.HIVE_INT,
                TypeSignature.parseTypeSignature("int"),
                0,
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.empty(),
                Optional.empty());
        HiveColumnHandle hidden = new HiveColumnHandle(
                HiveColumnHandle.PATH_COLUMN_NAME,
                HiveType.HIVE_INT,
                TypeSignature.parseTypeSignature("int"),
                0,
                HiveColumnHandle.ColumnType.SYNTHESIZED,
                Optional.empty(),
                Optional.empty());
        Column partitionColumn = new Column("ds", HIVE_STRING, Optional.empty(), Optional.empty());
        Table mockTable = new Table(
                "schema",
                "table",
                "user",
                PrestoTableType.MANAGED_TABLE,
                new Storage(fromHiveStorageFormat(ORC),
                        "location",
                        Optional.of(new HiveBucketProperty(
                                ImmutableList.of(column1.getName()),
                                100,
                                ImmutableList.of(),
                                HIVE_COMPATIBLE,
                                Optional.empty())),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                ImmutableList.of(column1),
                ImmutableList.of(partitionColumn),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty());

        ColumnMetadata actual = HiveMetadata.columnMetadataGetter(mockTable, mockTypeManager, new HiveColumnConverter()).apply(hiveColumnHandle1);
        ColumnMetadata expected = new ColumnMetadata("c1", IntegerType.INTEGER);
        assertEquals(actual, expected);

        actual = HiveMetadata.columnMetadataGetter(mockTable, mockTypeManager, new TestColumnConverter()).apply(hidden);
        expected = ColumnMetadata.builder().setName(HiveColumnHandle.PATH_COLUMN_NAME).setType(IntegerType.INTEGER).setHidden(true).build();
        assertEquals(actual, expected);
    }

    @Test(timeOut = 5000)
    public void testCreatePredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5_000; i++) {
            partitions.add(new HivePartition(
                    new SchemaTableName("test", "test"),
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.of(VarcharType.VARCHAR, Slices.utf8Slice(Integer.toString(i))))));
        }

        createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build());
    }

    @Test
    public void testCreateOnlyNullsPredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5; i++) {
            partitions.add(new HivePartition(
                    new SchemaTableName("test", "test"),
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.asNull(VarcharType.VARCHAR))));
        }

        createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build());
    }

    @Test
    public void testCreateMixedPredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5; i++) {
            partitions.add(new HivePartition(
                    new SchemaTableName("test", "test"),
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.of(VarcharType.VARCHAR, Slices.utf8Slice(Integer.toString(i))))));
        }

        partitions.add(new HivePartition(
                new SchemaTableName("test", "test"),
                "null",
                ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.asNull(VarcharType.VARCHAR))));

        createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build());
    }

    @Test
    public void testPreferredOrderingColumnsSerDe()
    {
        verifyPreferredOrderingColumnsRoundTrip(ImmutableList.of());
        verifyPreferredOrderingColumnsRoundTrip(ImmutableList.of(new SortingColumn("a", ASCENDING)));
        verifyPreferredOrderingColumnsRoundTrip(ImmutableList.of(new SortingColumn("a", DESCENDING)));
        verifyPreferredOrderingColumnsRoundTrip(ImmutableList.of(new SortingColumn("a", ASCENDING), new SortingColumn("b", DESCENDING)));
        verifyPreferredOrderingColumnsRoundTrip(ImmutableList.of(new SortingColumn("a", ASCENDING), new SortingColumn("b", ASCENDING)));
        verifyPreferredOrderingColumnsRoundTrip(ImmutableList.of(new SortingColumn("a", DESCENDING), new SortingColumn("b", ASCENDING)));
        verifyPreferredOrderingColumnsRoundTrip(ImmutableList.of(new SortingColumn("a", DESCENDING), new SortingColumn("b", DESCENDING)));

        verifyPreferredOrderingColumnsRoundTrip(ImmutableList.of(new SortingColumn("ASC", ASCENDING)));
        verifyPreferredOrderingColumnsRoundTrip(ImmutableList.of(new SortingColumn("DESC", DESCENDING)));
    }

    private void verifyPreferredOrderingColumnsRoundTrip(List<SortingColumn> sortingColumns)
    {
        List<SortingColumn> decoded = decodePreferredOrderingColumnsFromStorage(
                Storage.builder()
                        .setStorageFormat(VIEW_STORAGE_FORMAT)
                        .setLocation("test")
                        .setParameters(ImmutableMap.of(PREFERRED_ORDERING_COLUMNS, encodePreferredOrderingColumns(sortingColumns)))
                        .build());
        assertEquals(sortingColumns, decoded);
    }

    private class TestColumnConverter
            implements ColumnConverter
    {
        TestColumnConverter() {}

        @Override
        public Column toColumn(FieldSchema fieldSchema)
        {
            throw new NotImplementedException();
        }

        @Override
        public FieldSchema fromColumn(Column column)
        {
            throw new NotImplementedException();
        }

        @Override
        public TypeSignature getTypeSignature(HiveType hiveType, Optional<String> typeMetadata)
        {
            if (typeMetadata == null) {
                throw new AssertionError("typeMetadata is null");
            }
            return hiveType.getTypeSignature();
        }
    }
}
