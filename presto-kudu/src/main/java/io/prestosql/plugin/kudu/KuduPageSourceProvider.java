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
package io.prestosql.plugin.kudu;

import com.google.inject.Inject;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordPageSource;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KuduPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private KuduRecordSetProvider recordSetProvider;

    @Inject
    public KuduPageSourceProvider(KuduRecordSetProvider recordSetProvider)
    {
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        KuduRecordSet recordSet = (KuduRecordSet) recordSetProvider.getRecordSet(transactionHandle, session, split, columns);
        if (columns.contains(KuduColumnHandle.ROW_ID_HANDLE)) {
            return new KuduUpdatablePageSource(recordSet);
        }
        else {
            return new RecordPageSource(recordSet);
        }
    }
}
