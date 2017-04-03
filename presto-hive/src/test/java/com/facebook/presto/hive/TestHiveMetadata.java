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

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveMetadata.createPredicate;

public class TestHiveMetadata
{
    private static final HiveColumnHandle TEST_COLUMN_HANDLE = new HiveColumnHandle(
            "test",
            "test",
            HiveType.HIVE_STRING,
            TypeSignature.parseTypeSignature("varchar"),
            0,
            HiveColumnHandle.ColumnType.PARTITION_KEY,
            Optional.empty());

    @Test(timeOut = 5000)
    public void testCreatePredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5_000; i++) {
            ColumnDomain<HiveColumnHandle> columnDomain = new ColumnDomain<>(TEST_COLUMN_HANDLE, Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice(Integer.toString(i))));
            TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.fromColumnDomains(Optional.of(ImmutableList.of(columnDomain)));
            partitions.add(new HivePartition(
                    SchemaTableName.valueOf("test.test"),
                    tupleDomain,
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.of(VarcharType.VARCHAR, Slices.utf8Slice(Integer.toString(i)))), ImmutableList.of()));
        }

        createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build());
    }

    @Test
    public void testCreateOnlyNullsPredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5; i++) {
            ColumnDomain<HiveColumnHandle> columnDomain = new ColumnDomain<>(TEST_COLUMN_HANDLE, Domain.onlyNull(VarcharType.VARCHAR));
            TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.fromColumnDomains(Optional.of(ImmutableList.of(columnDomain)));
            partitions.add(new HivePartition(
                    SchemaTableName.valueOf("test.test"),
                    tupleDomain,
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.asNull(VarcharType.VARCHAR)), ImmutableList.of()));
        }

        createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build());
    }

    @Test
    public void testCreateMixedPredicate()
    {
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();

        for (int i = 0; i < 5; i++) {
            ColumnDomain<HiveColumnHandle> columnDomain = new ColumnDomain<>(TEST_COLUMN_HANDLE, Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice(Integer.toString(i))));
            TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.fromColumnDomains(Optional.of(ImmutableList.of(columnDomain)));
            partitions.add(new HivePartition(
                    SchemaTableName.valueOf("test.test"),
                    tupleDomain,
                    Integer.toString(i),
                    ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.of(VarcharType.VARCHAR, Slices.utf8Slice(Integer.toString(i)))), ImmutableList.of()));
        }

        ColumnDomain<HiveColumnHandle> columnDomain = new ColumnDomain<>(TEST_COLUMN_HANDLE, Domain.onlyNull(VarcharType.VARCHAR));
        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.fromColumnDomains(Optional.of(ImmutableList.of(columnDomain)));
        partitions.add(new HivePartition(
                SchemaTableName.valueOf("test.test"),
                tupleDomain,
                "null",
                ImmutableMap.of(TEST_COLUMN_HANDLE, NullableValue.asNull(VarcharType.VARCHAR)), ImmutableList.of()));

        createPredicate(ImmutableList.of(TEST_COLUMN_HANDLE), partitions.build());
    }
}
