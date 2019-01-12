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
package io.prestosql.memory.context;

import com.google.common.util.concurrent.ListenableFuture;

public interface LocalMemoryContext
{
    long getBytes();

    /**
     * When this method returns, the bytes tracked by this LocalMemoryContext has been updated.
     * The returned future will tell the caller whether it should block before reserving more memory
     * (which happens when the memory pools are low on memory).
     * <p/>
     * Note: Canceling the returned future will complete it immediately even though the memory pools are low
     * on memory, and callers blocked on this future will proceed to allocating more memory from the exhausted
     * pools, which will violate the protocol of Presto MemoryPool implementation.
     */
    ListenableFuture<?> setBytes(long bytes);

    /**
     * This method can return false when there is not enough memory available to satisfy a positive delta allocation
     * ({@code bytes} is greater than the bytes tracked by this LocalMemoryContext).
     * <p/>
     *
     * @return true if the bytes tracked by this LocalMemoryContext can be set to {@code bytes}.
     */
    boolean trySetBytes(long bytes);

    /**
     * Closes this LocalMemoryContext. Once closed the bytes tracked by this LocalMemoryContext will be set to 0, and
     * none of its methods (except {@code getBytes()}) can be called.
     */
    void close();
}
