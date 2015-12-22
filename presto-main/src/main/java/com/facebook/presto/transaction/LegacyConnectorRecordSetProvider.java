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
package com.facebook.presto.transaction;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.TransactionalConnectorRecordSetProvider;
import com.facebook.presto.spi.transaction.ConnectorTransactionHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LegacyConnectorRecordSetProvider
        implements TransactionalConnectorRecordSetProvider
{
    private final ConnectorRecordSetProvider delegate;

    public LegacyConnectorRecordSetProvider(ConnectorRecordSetProvider delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        return delegate.getRecordSet(session, split, columns);
    }
}
