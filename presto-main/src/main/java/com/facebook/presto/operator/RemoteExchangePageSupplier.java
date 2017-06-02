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
package com.facebook.presto.operator;

import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.spi.Page;
import com.google.common.util.concurrent.ListenableFuture;

import static java.util.Objects.requireNonNull;

public class RemoteExchangePageSupplier
        implements PageSupplier
{
    private final ExchangeClient client;
    private final PagesSerde serde;

    public RemoteExchangePageSupplier(ExchangeClient client, PagesSerde serde)
    {
        this.client = requireNonNull(client, "client is null");
        this.serde = requireNonNull(serde, "serde is null");
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return client.isBlocked();
    }

    @Override
    public Page pollPage()
    {
        SerializedPage serializedPage = client.pollPage();
        if (serializedPage == null) {
            return null;
        }
        return serde.deserialize(serializedPage);
    }

    @Override
    public boolean isFinished()
    {
        return client.isFinished();
    }
}
