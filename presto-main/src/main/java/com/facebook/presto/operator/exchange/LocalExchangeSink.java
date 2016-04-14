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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LocalExchangeSink
{
    public static LocalExchangeSink finishedLocalExchangeSink(List<Type> types, LocalExchangeMemoryManager memoryManager)
    {
        LocalExchangeSink finishedSink = new LocalExchangeSink(types, page -> { }, memoryManager, sink -> { });
        finishedSink.finish();
        return finishedSink;
    }

    private final List<Type> types;
    private final Consumer<Page> exchanger;
    private final LocalExchangeMemoryManager memoryManager;
    private final Consumer<LocalExchangeSink> onFinish;

    private final AtomicBoolean finished = new AtomicBoolean();

    public LocalExchangeSink(
            List<Type> types,
            Consumer<Page> exchanger,
            LocalExchangeMemoryManager memoryManager,
            Consumer<LocalExchangeSink> onFinish)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.exchanger = requireNonNull(exchanger, "exchanger is null");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.onFinish = requireNonNull(onFinish, "onFinish is null");
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public void finish()
    {
        if (finished.compareAndSet(false, true)) {
            onFinish.accept(this);
        }
    }

    public boolean isFinished()
    {
        return finished.get();
    }

    public void addPage(Page page)
    {
        requireNonNull(page, "page is null");

        // ignore pages after finished
        // this can happen with limit queries when all of the source (readers) are closed, so sinks
        // can be aborted early
        if (isFinished()) {
            return;
        }
        checkArgument(page.getChannelCount() == getTypes().size());

        // there can be a race where finished is set between the check above and here
        // it is expected that the exchanger ignores pages after finish
        exchanger.accept(page);
    }

    public ListenableFuture<?> waitForWriting()
    {
        if (isFinished()) {
            return NOT_BLOCKED;
        }
        return memoryManager.getNotFullFuture();
    }
}
