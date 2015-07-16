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

import com.facebook.presto.execution.SystemMemoryUsageListener;

import static com.google.common.base.Preconditions.checkNotNull;

public class ExchangeClientSupplier
{
    private final ExchangeClient exchangeClient;

    public ExchangeClientSupplier(ExchangeClient exchangeClient)
    {
        this.exchangeClient = checkNotNull(exchangeClient, "exchangeClient is null");
    }

    public ExchangeClient get(SystemMemoryUsageListener systemMemoryUsageListener)
    {
        return new ExchangeClient(exchangeClient, systemMemoryUsageListener);
    }
}
