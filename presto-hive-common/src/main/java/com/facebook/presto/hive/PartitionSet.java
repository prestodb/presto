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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PartitionSet
        implements Iterable<HivePartition>
{
    private volatile boolean fullyLoaded;
    private PartitionLoader partitionLoader;
    private List<HivePartition> partitions;

    public PartitionSet(List<HivePartition> partitions)
    {
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.fullyLoaded = true;
    }

    public PartitionSet(PartitionLoader partitionLoader)
    {
        this.partitionLoader = requireNonNull(partitionLoader, "partitionLoader is null");
    }

    @Override
    public Iterator<HivePartition> iterator()
    {
        return new LazyIterator(this);
    }

    public List<HivePartition> getFullyLoadedPartitions()
    {
        tryFullyLoad();
        return partitions;
    }

    public boolean isEmpty()
    {
        if (fullyLoaded) {
            return partitions.isEmpty();
        }
        else {
            synchronized (this) {
                if (fullyLoaded) {
                    return partitions.isEmpty();
                }
                return partitionLoader.isEmpty();
            }
        }
    }

    private void tryFullyLoad()
    {
        if (!fullyLoaded) {
            synchronized (this) {
                if (!fullyLoaded) {
                    partitions = ImmutableList.copyOf(partitionLoader.loadPartitions());
                    fullyLoaded = true;
                    partitionLoader = null;
                }
            }
        }
    }

    public interface PartitionLoader
    {
        List<HivePartition> loadPartitions();

        boolean isEmpty();
    }

    private static class LazyIterator
            extends AbstractIterator<HivePartition>
    {
        private final PartitionSet lazyPartitions;
        private List<HivePartition> partitions;
        private int position = -1;

        private LazyIterator(PartitionSet lazyPartitions)
        {
            this.lazyPartitions = lazyPartitions;
        }

        @Override
        protected HivePartition computeNext()
        {
            if (partitions == null) {
                partitions = lazyPartitions.getFullyLoadedPartitions();
            }

            position++;
            if (position >= partitions.size()) {
                return endOfData();
            }
            return partitions.get(position);
        }
    }
}
