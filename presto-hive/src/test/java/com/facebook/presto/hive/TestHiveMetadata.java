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
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveMetadata.createPredicate;
import static com.facebook.presto.hive.HiveMetadata.decodePreferredOrderingColumnsFromStorage;
import static com.facebook.presto.hive.HiveMetadata.encodePreferredOrderingColumns;
import static com.facebook.presto.hive.HiveTableProperties.PREFERRED_ORDERING_COLUMNS;
import static com.facebook.presto.hive.metastore.SortingColumn.Order.ASCENDING;
import static com.facebook.presto.hive.metastore.SortingColumn.Order.DESCENDING;
import static com.facebook.presto.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
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
}
