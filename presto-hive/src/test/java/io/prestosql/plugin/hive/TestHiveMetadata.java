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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.hive.HiveMetadata.createPredicate;

public class TestHiveMetadata
{
    private static final HiveColumnHandle TEST_COLUMN_HANDLE = new HiveColumnHandle(
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
}
