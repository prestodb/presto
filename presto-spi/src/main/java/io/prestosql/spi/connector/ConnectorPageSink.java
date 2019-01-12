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
package io.prestosql.spi.connector;

import io.airlift.slice.Slice;
import io.prestosql.spi.Page;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface ConnectorPageSink
{
    CompletableFuture<?> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    /**
     * Gets the number of output bytes written by this page source so far.
     * If size is not available, this method should return zero.
     */
    default long getCompletedBytes()
    {
        return 0;
    }

    /**
     * Get the total memory that needs to be reserved in the general memory pool.
     * This memory should include any buffers, etc. that are used for reading data.
     *
     * @return the memory used so far in table read
     */
    default long getSystemMemoryUsage()
    {
        return 0;
    }

    /**
     * ConnectorPageSink can provide optional validation to check
     * the data is correctly consumed by connector (e.g. output table has the correct data).
     * <p>
     * This method returns the CPU spent on validation, if any.
     */
    default long getValidationCpuNanos()
    {
        return 0;
    }

    /**
     * Returns a future that will be completed when the page sink can accept
     * more pages.  If the page sink can accept more pages immediately,
     * this method should return {@code NOT_BLOCKED}.
     */
    CompletableFuture<?> appendPage(Page page);

    /**
     * Notifies the connector that no more pages will be appended and returns
     * connector-specific information that will be sent to the coordinator to
     * complete the write operation. This method may be called immediately
     * after the previous call to {@link #appendPage} (even if the returned
     * future is not complete).
     */
    CompletableFuture<Collection<Slice>> finish();

    void abort();
}
