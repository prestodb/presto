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
import io.airlift.json.JsonCodec;
import io.prestosql.plugin.hive.HiveColumnHandle.ColumnType;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestHiveSplit
{
    private final JsonCodec<HiveSplit> codec = JsonCodec.jsonCodec(HiveSplit.class);

    @Test
    public void testJsonRoundTrip()
    {
        Properties schema = new Properties();
        schema.setProperty("foo", "bar");
        schema.setProperty("bar", "baz");

        ImmutableList<HivePartitionKey> partitionKeys = ImmutableList.of(new HivePartitionKey("a", "apple"), new HivePartitionKey("b", "42"));
        ImmutableList<HostAddress> addresses = ImmutableList.of(HostAddress.fromParts("127.0.0.1", 44), HostAddress.fromParts("127.0.0.1", 45));
        HiveSplit expected = new HiveSplit(
                "db",
                "table",
                "partitionId",
                "path",
                42,
                87,
                88,
                schema,
                partitionKeys,
                addresses,
                OptionalInt.empty(),
                true,
                TupleDomain.all(),
                ImmutableMap.of(1, HIVE_STRING),
                Optional.of(new HiveSplit.BucketConversion(
                        32,
                        16,
                        ImmutableList.of(new HiveColumnHandle("col", HIVE_LONG, BIGINT.getTypeSignature(), 5, ColumnType.REGULAR, Optional.of("comment"))))),
                false);

        String json = codec.toJson(expected);
        HiveSplit actual = codec.fromJson(json);

        assertEquals(actual.getDatabase(), expected.getDatabase());
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getPartitionName(), expected.getPartitionName());
        assertEquals(actual.getPath(), expected.getPath());
        assertEquals(actual.getStart(), expected.getStart());
        assertEquals(actual.getLength(), expected.getLength());
        assertEquals(actual.getFileSize(), expected.getFileSize());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getPartitionKeys(), expected.getPartitionKeys());
        assertEquals(actual.getAddresses(), expected.getAddresses());
        assertEquals(actual.getColumnCoercions(), expected.getColumnCoercions());
        assertEquals(actual.getBucketConversion(), expected.getBucketConversion());
        assertEquals(actual.isForceLocalScheduling(), expected.isForceLocalScheduling());
        assertEquals(actual.isS3SelectPushdownEnabled(), expected.isS3SelectPushdownEnabled());
    }
}
