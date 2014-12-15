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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.CappedRowSink;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class OrcShardedRowSink
        implements RowSink
{
    private final RowSinkProvider rowSinkProvider;
    private final List<Integer> bucketFields;
    private final Optional<Integer> rowsPerShard;
    private final Map<Integer, RowSink> rowSinks;
    private final int bucketCount;

    public OrcShardedRowSink(RowSinkProvider rowSinkProvider, int bucketCount, List<Integer> bucketFields,  Optional<Integer> rowsPerShard)
    {
        this.rowSinkProvider = checkNotNull(rowSinkProvider, "rowSinkProvider is null");
        checkArgument(bucketCount > 1, "bucketCount must be > 1");
        this.bucketCount = bucketCount;
        this.bucketFields = ImmutableList.copyOf(checkNotNull(bucketFields, "bucketFields is null"));
        this.rowsPerShard = checkNotNull(rowsPerShard, "rowsPerShard is null");
        this.rowSinks = Maps.newHashMap();
    }

    public static RowSink from(RowSinkProvider rowSinkProvider, int bucketCount, List<Integer> bucketFields, Optional<Integer> rowsPerShard)
    {
        checkNotNull(rowSinkProvider, "rowSinkProvider is null");
        checkArgument(bucketCount > 1, "bucketCount must be > 1");
        checkNotNull(bucketFields, "bucketColumnIds is null");
        checkArgument(!bucketFields.isEmpty(), "bucketFields is empty");
        checkNotNull(rowsPerShard, "rowsPerShard is null");
        return new OrcShardedRowSink(rowSinkProvider, bucketCount, bucketFields, rowsPerShard);
    }

    @Override
    public void appendTuple(TupleBuffer tupleBuffer)
    {
        BucketHasher bucketHasher = new BucketHasher(bucketCount);
        for (int bucketField : bucketFields) {
            bucketHasher.addValue(tupleBuffer, bucketField);
        }
        int bucket = bucketHasher.computeBucketId();
        getRowSink(bucket).appendTuple(tupleBuffer);
    }

    @Override
    public void close()
    {
        for (RowSink rowSink : rowSinks.values()) {
            rowSink.close();
        }
        rowSinks.clear();
    }

    private RowSink getRowSink(Integer shardId)
    {
        RowSink rowSink = rowSinks.get(shardId);
        if (rowSink != null) {
            return rowSink;
        }
        if (rowsPerShard.isPresent()) {
            rowSink = CappedRowSink.from(rowSinkProvider, rowsPerShard.get());
        }
        else {
            rowSink = rowSinkProvider.getRowSink();
        }
        rowSinks.put(shardId, rowSink);
        return rowSink;
    }
}
