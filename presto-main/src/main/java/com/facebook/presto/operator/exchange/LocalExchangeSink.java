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
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.facebook.presto.operator.exchange.LocalExchanger.FINISHED;
import static java.util.Objects.requireNonNull;

public class LocalExchangeSink
{
    public static LocalExchangeSink finishedLocalExchangeSink()
    {
        LocalExchangeSink finishedSink = new LocalExchangeSink(FINISHED, sink -> {});
        finishedSink.finish();
        return finishedSink;
    }

    private final LocalExchanger exchanger;
    private final Consumer<LocalExchangeSink> onFinish;

    private final AtomicBoolean finished = new AtomicBoolean();

    public LocalExchangeSink(
            LocalExchanger exchanger,
            Consumer<LocalExchangeSink> onFinish)
    {
        this.exchanger = requireNonNull(exchanger, "exchanger is null");
        this.onFinish = requireNonNull(onFinish, "onFinish is null");
    }

    public void finish()
    {
        if (finished.compareAndSet(false, true)) {
            exchanger.finish();
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

        // there can be a race where finished is set between the check above and here
        // it is expected that the exchanger ignores pages after finish
        exchanger.accept(page);
    }

    public ListenableFuture<?> waitForWriting()
    {
        if (isFinished()) {
            return NOT_BLOCKED;
        }
        return exchanger.waitForWriting();
    }
}
