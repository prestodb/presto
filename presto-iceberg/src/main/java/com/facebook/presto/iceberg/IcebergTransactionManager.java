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
package com.facebook.presto.iceberg;

import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class IcebergTransactionManager
{
    private final Map<ConnectorTransactionHandle, IcebergMetadata> transactions = new ConcurrentHashMap<>();

    public IcebergMetadata get(ConnectorTransactionHandle transaction)
    {
        IcebergMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    public IcebergMetadata remove(ConnectorTransactionHandle transaction)
    {
        IcebergMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    public void put(ConnectorTransactionHandle transaction, IcebergMetadata metadata)
    {
        ConnectorMetadata existing = transactions.putIfAbsent(transaction, metadata);
        checkState(existing == null, "transaction already exists: %s", existing);
    }
}
