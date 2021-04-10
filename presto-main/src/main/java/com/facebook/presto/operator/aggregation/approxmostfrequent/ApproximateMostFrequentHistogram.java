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
package com.facebook.presto.operator.aggregation.approxmostfrequent;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.clearspring.analytics.util.ListNode2;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 *  Calculate the histogram approximately for topk elements based on the
 *  <i>Space-Saving</i> algorithm and the <i>Stream-Summary</i> data structure
 *  as described in:
 *  <i>Efficient Computation of Frequent and Top-k Elements in Data Streams</i>
 *  by Metwally, Agrawal, and Abbadi
 * @param <K>
 */
public class ApproximateMostFrequentHistogram<K>
{
    private static final byte FORMAT_TAG = 0;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ApproximateMostFrequentHistogram.class).instanceSize();

    private static final int STREAM_SUMMARY_SIZE = ClassLayout.parseClass(StreamSummary.class).instanceSize();
    private static final int LIST_NODE2_SIZE = ClassLayout.parseClass(ListNode2.class).instanceSize();
    private static final int COUNTER_SIZE = ClassLayout.parseClass(Counter.class).instanceSize();

    private final StreamSummary<K> streamSummary;
    private final int maxBuckets;
    private final int capacity;
    private final ApproximateMostFrequentBucketSerializer<K> serializer;
    private final ApproximateMostFrequentBucketDeserializer<K> deserializer;

    /**
     * @param maxBuckets The maximum number of elements stored in the bucket.
     * @param capacity The maximum capacity of the stream summary data structure.
     * @param serializer It serializes a bucket into varbinary slice.
     * @param deserializer It appends a bucket into the histogram.
     */
    public ApproximateMostFrequentHistogram(
            int maxBuckets,
            int capacity,
            ApproximateMostFrequentBucketSerializer<K> serializer,
            ApproximateMostFrequentBucketDeserializer<K> deserializer)
    {
        requireNonNull(serializer, "serializer is null");
        requireNonNull(deserializer, "deserializer is null");
        streamSummary = new StreamSummary<>(capacity);
        this.maxBuckets = maxBuckets;
        this.capacity = capacity;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public ApproximateMostFrequentHistogram(
            Slice serialized,
            ApproximateMostFrequentBucketSerializer<K> serializer,
            ApproximateMostFrequentBucketDeserializer<K> deserializer)
    {
        SliceInput input = serialized.getInput();

        checkArgument(input.readByte() == FORMAT_TAG, "Unsupported format tag");

        this.maxBuckets = input.readInt();
        this.capacity = input.readInt();
        int bucketSize = input.readInt();
        this.streamSummary = new StreamSummary<>(capacity);
        this.serializer = serializer;
        this.deserializer = deserializer;

        for (int i = 0; i < bucketSize; i++) {
            this.deserializer.deserialize(input, this);
        }
    }

    public void add(K value)
    {
        streamSummary.offer(value);
    }

    public void add(K value, long incrementCount)
    {
        streamSummary.offer(value, toIntExact(incrementCount));
    }

    public Slice serialize()
    {
        List<Counter<K>> counters = streamSummary.topK(maxBuckets);
        int estimatedSliceSize = Byte.BYTES + // FORMAT_TAG
                Integer.BYTES + // maxBuckets
                Integer.BYTES + // capacity
                Integer.BYTES + // Counters size
                counters.size() * Long.BYTES * 2; // Bytes allocated for item and count. Although the estimation is not correct for variable length slices, it should work.
        DynamicSliceOutput output = new DynamicSliceOutput(estimatedSliceSize);
        output.appendByte(FORMAT_TAG);
        output.appendInt(maxBuckets);
        output.appendInt(capacity);
        output.appendInt(counters.size());
        // Serialize key and counts.
        for (Counter<K> counter : counters) {
            serializer.serialize(counter.getItem(), counter.getCount(), output);
        }

        return output.slice();
    }

    public void merge(ApproximateMostFrequentHistogram<K> other)
    {
        List<Counter<K>> counters = other.streamSummary.topK(maxBuckets);
        for (Counter<K> counter : counters) {
            add(counter.getItem(), counter.getCount());
        }
    }

    public void forEachBucket(BucketConsumer<K> consumer)
    {
        List<Counter<K>> counters = streamSummary.topK(maxBuckets);
        for (Counter<K> counter : counters) {
            consumer.process(counter.getItem(), counter.getCount());
        }
    }

    @VisibleForTesting
    public Map<K, Long> getBuckets()
    {
        ImmutableMap.Builder<K, Long> buckets = new ImmutableMap.Builder<>();
        forEachBucket(buckets::put);

        return buckets.build();
    }

    public long estimatedInMemorySize()
    {
        // imperfect estimate of the size of the underlying StreamSummary. TODO: reimplement StreamSummary with flat structures and proper size accounting
        return INSTANCE_SIZE +
                STREAM_SUMMARY_SIZE +
                streamSummary.size() * (LIST_NODE2_SIZE + COUNTER_SIZE + Long.BYTES); // Long.BYTES as a proxy for the size of K
    }
}
