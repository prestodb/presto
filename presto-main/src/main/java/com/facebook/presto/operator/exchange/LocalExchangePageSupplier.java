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

import com.facebook.presto.operator.PageSupplier;
import com.facebook.presto.operator.exchange.LocalMergeExchange.ExchangeBuffer;
import com.facebook.presto.spi.Page;
import com.google.common.util.concurrent.ListenableFuture;

import static java.util.Objects.requireNonNull;

public class LocalExchangePageSupplier
        implements PageSupplier
{
    private final ExchangeBuffer buffer;

    public LocalExchangePageSupplier(ExchangeBuffer buffer)
    {
        this.buffer = requireNonNull(buffer, "buffer is null");
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return buffer.isReadBlocked();
    }

    @Override
    public Page pollPage()
    {
        return buffer.poolPage();
    }

    @Override
    public boolean isFinished()
    {
        return buffer.isWriteFinished() && buffer.isEmpty();
    }
}
