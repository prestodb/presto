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

import com.facebook.presto.spi.LazyIterable;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LazyLoadedPartitions
        implements LazyIterable<HivePartition>
{
    private boolean fullyLoaded;
    private PartitionLoader partitionLoader;
    private List<HivePartition> partitions;

    public LazyLoadedPartitions(List<HivePartition> partitions)
    {
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.fullyLoaded = true;
    }

    public LazyLoadedPartitions(PartitionLoader partitionLoader)
    {
        this.partitionLoader = requireNonNull(partitionLoader, "partitionLoader is null");
    }

    @Override
    public Iterator<HivePartition> iterator()
    {
        return new LazyIterator(this);
    }

    public void setMaxIterableCount(int maxPartitionThreshold)
    {
        if (this.partitionLoader != null) {
            this.partitionLoader.setMaxPartitionThreshold(maxPartitionThreshold);
        }
    }

    public List<HivePartition> getFullyLoadedPartitions()
    {
        tryFullyLoad();
        return this.partitions;
    }

    public boolean isEmpty()
    {
        if (this.fullyLoaded) {
            return this.partitions.isEmpty();
        }
        else {
            return this.partitionLoader.isEmpty();
        }
    }

    private void tryFullyLoad()
    {
        if (!this.fullyLoaded) {
            synchronized (this) {
                if (!this.fullyLoaded) {
                    this.partitions = this.partitionLoader.loadPartitions();
                    this.fullyLoaded = true;
                    this.partitionLoader = null;
                }
            }
        }
    }

    public interface PartitionLoader
    {
        List<HivePartition> loadPartitions();

        boolean isEmpty();

        void setMaxPartitionThreshold(int maxPartitionThreshold);
    }

    private static class LazyIterator
            extends AbstractIterator<HivePartition>
    {
        private final LazyLoadedPartitions lazyPartitions;
        private List<HivePartition> partitions;
        private int position = -1;

        private LazyIterator(LazyLoadedPartitions lazyPartitions)
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
