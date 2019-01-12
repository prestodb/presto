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
package io.prestosql.spiller;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.spi.Page;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.IntPredicate;

import static java.util.Objects.requireNonNull;

public interface PartitioningSpiller
        extends Closeable
{
    /**
     * Partition page and enqueue partitioned pages to spill writers.
     * {@link PartitioningSpillResult#getSpillingFuture} is completed when spilling is finished.
     * <p>
     * This method may not be called if previously initiated spilling is not finished yet.
     */
    PartitioningSpillResult partitionAndSpill(Page page, IntPredicate spillPartitionMask);

    /**
     * Returns iterator of previously spilled pages from given partition. Callers are expected to call
     * this method once. Calling multiple times can results in undefined behavior.
     * <p>
     * This method may not be called if previously initiated spilling is not finished yet.
     * <p>
     * This method may perform blocking I/O to flush internal buffers.
     */
    // TODO getSpilledPages should not need flush last buffer to disk
    Iterator<Page> getSpilledPages(int partition);

    void verifyAllPartitionsRead();

    /**
     * Closes and removes all underlying resources used during spilling.
     */
    @Override
    void close()
            throws IOException;

    class PartitioningSpillResult
    {
        private final ListenableFuture<?> spillingFuture;
        private final Page retained;

        public PartitioningSpillResult(ListenableFuture<?> spillingFuture, Page retained)
        {
            this.spillingFuture = requireNonNull(spillingFuture, "spillingFuture is null");
            this.retained = requireNonNull(retained, "retained is null");
        }

        public ListenableFuture<?> getSpillingFuture()
        {
            return spillingFuture;
        }

        public Page getRetained()
        {
            return retained;
        }
    }
}
