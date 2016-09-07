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
package com.facebook.presto.spiller;

import com.facebook.presto.spi.Page;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public interface PartitioningSpiller
        extends Closeable
{
    /**
     * Partition page and enqueue partitioned pages to spill writers.
     * PartitioningSpillResult::isBlocked returns completed future when finished.
     */
    PartitioningSpillResult partitionAndSpill(Page page);

    /**
     * Returns iterator of previously spilled Pages from given partition.
     */
    Iterator<Page> getSpilledPages(int partition);

    /**
     * Close releases/removes all underlying resources used during spilling
     * like for example all created temporary files.
     */
    @Override
    void close();

    public static class PartitioningSpillResult
    {
        private CompletableFuture<?> blocked;
        private IntArrayList unspilledPositions;

        public PartitioningSpillResult(CompletableFuture<?> blocked, IntArrayList unspilledPositions)
        {
            this.blocked = requireNonNull(blocked, "blocked is null");
            this.unspilledPositions = requireNonNull(unspilledPositions, "unspilledPositions is null");
        }

        public CompletableFuture<?> isBlocked()
        {
            return blocked;
        }

        public IntArrayList getUnspilledPositions()
        {
            return unspilledPositions;
        }
    }
}
