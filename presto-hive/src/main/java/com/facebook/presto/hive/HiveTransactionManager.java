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
package com.facebook.presto.hive;

import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;

public class HiveTransactionManager
{
    private final ConcurrentMap<ConnectorTransactionHandle, ConnectorMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public HiveTransactionManager()
    {
    }

    public ConnectorMetadata get(ConnectorTransactionHandle transactionHandle)
    {
        return transactions.get(transactionHandle);
    }

    public ConnectorMetadata remove(ConnectorTransactionHandle transactionHandle)
    {
        return transactions.remove(transactionHandle);
    }

    public void put(ConnectorTransactionHandle transactionHandle, ConnectorMetadata metadata)
    {
        ConnectorMetadata previousValue = transactions.putIfAbsent(transactionHandle, metadata);
        checkState(previousValue == null);
    }
}
