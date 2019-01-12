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
package io.prestosql.operator.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.spi.Page;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

class BroadcastExchanger
        implements LocalExchanger
{
    private final List<Consumer<PageReference>> buffers;
    private final LocalExchangeMemoryManager memoryManager;

    public BroadcastExchanger(List<Consumer<PageReference>> buffers, LocalExchangeMemoryManager memoryManager)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(buffers, "buffers is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
    }

    @Override
    public void accept(Page page)
    {
        memoryManager.updateMemoryUsage(page.getRetainedSizeInBytes());

        PageReference pageReference = new PageReference(page, buffers.size(), () -> memoryManager.updateMemoryUsage(-page.getRetainedSizeInBytes()));

        for (Consumer<PageReference> buffer : buffers) {
            buffer.accept(pageReference);
        }
    }

    @Override
    public ListenableFuture<?> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
