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
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.facebook.presto.raptor.RaptorColumnHandle.bucketNumberColumnHandle;
import static com.facebook.presto.raptor.RaptorColumnHandle.shardUuidColumnHandle;
import static com.facebook.presto.raptor.util.UuidUtil.uuidStringToBytes;
import static com.facebook.presto.spi.predicate.Domain.create;
import static com.facebook.presto.spi.predicate.Domain.singleValue;
import static com.facebook.presto.spi.predicate.Range.equal;
import static com.facebook.presto.spi.predicate.Range.greaterThanOrEqual;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.sql.JDBCType.VARBINARY;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;

public class TestShardPredicate
{
    private static final boolean bucketed = false;

    @Test
    public void testSimpleShardUuidPredicate()
            throws Exception
    {
        String uuid = randomUUID().toString();
        TupleDomain<RaptorColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(
                shardUuidColumnHandle("test"), singleValue(VARCHAR, utf8Slice(uuid))
        ));

        ShardPredicate shardPredicate = ShardPredicate.create(tupleDomain, bucketed);

        assertEquals(shardPredicate.getPredicate(), "shard_uuid = ?");
        assertEquals(shardPredicate.getTypes(), ImmutableList.of(VARBINARY));
        assertEquals(shardPredicate.getValues(), ImmutableList.of(uuidStringToBytes(utf8Slice(uuid))));
    }

    @Test
    public void testDiscreteShardUuidPredicate()
            throws Exception
    {
        Slice uuid0 = utf8Slice(randomUUID().toString());
        Slice uuid1 = utf8Slice(randomUUID().toString());
        TupleDomain<RaptorColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(
                shardUuidColumnHandle("test"),
                create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(equal(VARCHAR, uuid0), equal(VARCHAR, uuid1))), false)));

        ShardPredicate shardPredicate = ShardPredicate.create(tupleDomain, bucketed);

        assertEquals(shardPredicate.getPredicate(), "shard_uuid = ? OR shard_uuid = ?");
        assertEquals(shardPredicate.getTypes(), ImmutableList.of(VARBINARY, VARBINARY));
        assertEquals(ImmutableSet.copyOf(shardPredicate.getValues()), ImmutableSet.of(uuidStringToBytes(uuid0), uuidStringToBytes(uuid1)));
    }

    @Test
    public void testInvalidUuid()
            throws Exception
    {
        Slice uuid0 = utf8Slice("test1");
        Slice uuid1 = utf8Slice("test2");
        TupleDomain<RaptorColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(
                shardUuidColumnHandle("test"),
                create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(equal(VARCHAR, uuid0), equal(VARCHAR, uuid1))), false)));

        ShardPredicate shardPredicate = ShardPredicate.create(tupleDomain, bucketed);

        assertEquals(shardPredicate.getPredicate(), "true");
    }

    @Test
    public void testRangeShardUuidPredicate()
            throws Exception
    {
        Slice uuid0 = utf8Slice(randomUUID().toString());
        TupleDomain<RaptorColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(
                shardUuidColumnHandle("test"),
                create(SortedRangeSet.copyOf(VARCHAR, ImmutableList.of(greaterThanOrEqual(VARCHAR, uuid0))), false)));

        ShardPredicate shardPredicate = ShardPredicate.create(tupleDomain, bucketed);
        assertEquals(shardPredicate.getPredicate(), "true");
    }

    @Test
    public void testBucketNumber()
            throws Exception
    {
        TupleDomain<RaptorColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(
                bucketNumberColumnHandle("test"),
                create(SortedRangeSet.copyOf(INTEGER, ImmutableList.of(equal(INTEGER, 1L))), false)));

        ShardPredicate shardPredicate = ShardPredicate.create(tupleDomain, true);
        assertEquals(shardPredicate.getPredicate(), "(bucket_number >= ? OR bucket_number IS NULL) AND (bucket_number <= ? OR bucket_number IS NULL)");
    }

    @Test
    public void testBucketNumberForNonBucketed()
            throws Exception
    {
        TupleDomain<RaptorColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(
                bucketNumberColumnHandle("test"),
                create(SortedRangeSet.copyOf(INTEGER, ImmutableList.of(equal(INTEGER, 1L))), false)));

        ShardPredicate shardPredicate = ShardPredicate.create(tupleDomain, false);
        assertEquals(shardPredicate.getPredicate(), "false");
    }
}
