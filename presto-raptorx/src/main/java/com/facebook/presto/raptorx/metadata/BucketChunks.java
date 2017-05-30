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
package com.facebook.presto.raptorx.metadata;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BucketChunks
{
    private final int bucketNumber;
    private final Set<Long> chunkIds;

    public BucketChunks(int bucketNumber, Long chunkId)
    {
        this(bucketNumber, ImmutableSet.of(chunkId));
    }

    public BucketChunks(int bucketNumber, Set<Long> chunkIds)
    {
        this.bucketNumber = bucketNumber;
        this.chunkIds = ImmutableSet.copyOf(requireNonNull(chunkIds, "chunkIds is null"));
    }

    public int getBucketNumber()
    {
        return bucketNumber;
    }

    public Set<Long> getChunkIds()
    {
        return chunkIds;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucket", bucketNumber)
                .add("chunkIds", chunkIds)
                .toString();
    }
}
