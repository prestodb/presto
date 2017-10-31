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
package com.facebook.presto.memory;

import com.google.common.util.concurrent.ListenableFuture;

public interface LocalMemoryContext
{
    long getBytes();
    ListenableFuture<?> setBytes(long bytes);
    boolean trySetBytes(long bytes);
    /**
     * This method transfers ownership of allocations from this memory context to the "to" memory context,
     * where parent of this is a descendant of to.parent (there can be multiple AggregatedMemoryContexts between them).
     * <p>
     * During the transfer the implementation of this method must not reflect any state changes outside of the contexts
     * (e.g., by calling the reservation handlers).
     */
    void transferOwnership(LocalMemoryContext to);
    void close();
}
