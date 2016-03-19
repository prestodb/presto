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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.sql.JDBCType;

import static com.facebook.presto.raptor.RaptorColumnHandle.shardUuidColumnHandle;
import static com.facebook.presto.raptor.util.UuidUtil.uuidStringToBytes;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;

public class TestShardPredicate
{
    @Test
    public void testSimpleShardUuidPredicate()
            throws Exception
    {
        String uuid = randomUUID().toString();
        TupleDomain<RaptorColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                shardUuidColumnHandle("test"), Domain.singleValue(VARCHAR, utf8Slice(uuid))
        ));

        ShardPredicate shardPredicate = ShardPredicate.create(tupleDomain);

        assertEquals(shardPredicate.getPredicate(), "shard_uuid = ?");
        assertEquals(shardPredicate.getTypes(), ImmutableList.of(JDBCType.VARBINARY));
        assertEquals(shardPredicate.getValues(), ImmutableList.of(uuidStringToBytes(utf8Slice(uuid))));
    }

    @Test
    public void testRangeShardUuidPredicate()
            throws Exception
    {
        Slice uuid0 = utf8Slice(randomUUID().toString());
        Slice uuid1 = utf8Slice(randomUUID().toString());
        TupleDomain<RaptorColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                shardUuidColumnHandle("test"), Domain.create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                        Range.equal(VARCHAR, uuid0),
                        Range.equal(VARCHAR, uuid1)
                )), false)
        ));

        ShardPredicate shardPredicate = ShardPredicate.create(tupleDomain);

        assertEquals(shardPredicate.getPredicate(), "shard_uuid = ? OR shard_uuid = ?");
        assertEquals(shardPredicate.getTypes(), ImmutableList.of(JDBCType.VARBINARY, JDBCType.VARBINARY));
        assertEquals(ImmutableSet.copyOf(shardPredicate.getValues()),
                ImmutableSet.of(uuidStringToBytes(uuid0), uuidStringToBytes(uuid1)));
    }

    @Test
    public void testRangeMarkerShardUuidPredicate()
            throws Exception
    {
        Slice uuid0 = utf8Slice(randomUUID().toString());
        TupleDomain<RaptorColumnHandle> tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(
                shardUuidColumnHandle("test"), Domain.create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(
                        Range.greaterThanOrEqual(VARCHAR, uuid0)
                )), false)
        ));

        ShardPredicate shardPredicate = ShardPredicate.create(tupleDomain);

        assertEquals(shardPredicate.getPredicate(), "shard_uuid >= ?");
        assertEquals(shardPredicate.getTypes(), ImmutableList.of(JDBCType.VARBINARY));
        assertEquals(shardPredicate.getValues(), ImmutableSet.of(uuidStringToBytes(uuid0)));
    }
}
