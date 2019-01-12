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

import static com.google.common.base.Preconditions.checkState;

/**
 * SimpleAggregatedMemoryContext doesn't have a parent or a reservation handler. It just counts bytes.
 */
class SimpleAggregatedMemoryContext
        extends AbstractAggregatedMemoryContext
{
    @Override
    synchronized ListenableFuture<?> updateBytes(String allocationTag, long bytes)
    {
        checkState(!isClosed(), "SimpleAggregatedMemoryContext is already closed");
        addBytes(bytes);
        return NOT_BLOCKED;
    }

    @Override
    synchronized boolean tryUpdateBytes(String allocationTag, long delta)
    {
        addBytes(delta);
        return true;
    }

    @Override
    synchronized AbstractAggregatedMemoryContext getParent()
    {
        return null;
    }

    @Override
    void closeContext() {}
}
