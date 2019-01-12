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
package io.prestosql.plugin.cassandra;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;

import java.nio.ByteBuffer;

public class CassandraPartition
{
    static final String UNPARTITIONED_ID = "<UNPARTITIONED>";
    public static final CassandraPartition UNPARTITIONED = new CassandraPartition();

    private final String partitionId;
    private final byte[] key;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final boolean indexedColumnPredicatePushdown;

    private CassandraPartition()
    {
        partitionId = UNPARTITIONED_ID;
        tupleDomain = TupleDomain.all();
        key = null;
        indexedColumnPredicatePushdown = false;
    }

    public CassandraPartition(byte[] key, String partitionId, TupleDomain<ColumnHandle> tupleDomain, boolean indexedColumnPredicatePushdown)
    {
        this.key = key;
        this.partitionId = partitionId;
        this.tupleDomain = tupleDomain;
        this.indexedColumnPredicatePushdown = indexedColumnPredicatePushdown;
    }

    public boolean isUnpartitioned()
    {
        return partitionId.equals(UNPARTITIONED_ID);
    }

    public boolean isIndexedColumnPredicatePushdown()
    {
        return indexedColumnPredicatePushdown;
    }

    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return tupleDomain;
    }

    public String getPartitionId()
    {
        return partitionId;
    }

    @Override
    public String toString()
    {
        return partitionId;
    }

    public ByteBuffer getKeyAsByteBuffer()
    {
        return ByteBuffer.wrap(key);
    }

    public byte[] getKey()
    {
        return key;
    }
}
