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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

// A TaskExchangeClientManager should be created for each SqlTask/SqlTaskExecution
public class TaskExchangeClientManager
{
    private final ExchangeClientSupplier supplier;

    @GuardedBy("this")
    private final List<ExchangeClient> exchangeClients;

    @VisibleForTesting
    public TaskExchangeClientManager(ExchangeClientSupplier supplier)
    {
        this.supplier = requireNonNull(supplier, "supplier is null");
        this.exchangeClients = new ArrayList<>();
    }

    public synchronized ExchangeClient createExchangeClient(LocalMemoryContext systemMemoryContext)
    {
        ExchangeClient exchangeClient = supplier.get(systemMemoryContext);
        exchangeClients.add(exchangeClient);
        return exchangeClient;
    }

    public synchronized List<ExchangeClient> getExchangeClients()
    {
        return ImmutableList.copyOf(exchangeClients);
    }
}
