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

import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorTransactionContext;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IcebergTransactionContext
        implements ConnectorTransactionContext
{
    final Optional<Table> table;
    final Transaction transaction;
    final Set<DataFile> scannedDataFiles = new HashSet<>();
    final Set<DeleteFile> fullyAppliedDeleteFiles = new HashSet<>();
    final Map<String, Object> relevantData = new HashMap<>();
    Optional<ConnectorSplitSource> connectorSplitSource = Optional.empty();

    public IcebergTransactionContext(Optional<Table> table, Transaction transaction)
    {
        this.table = table;
        this.transaction = transaction;
    }

    public Optional<Table> getTable()
    {
        return table;
    }

    public Transaction getTransaction()
    {
        return transaction;
    }

    public void setConnectorSplitSource(ConnectorSplitSource connectorSplitSource)
    {
        requireNonNull(connectorSplitSource, "connectorSplitSource is null");
        this.connectorSplitSource = Optional.of(connectorSplitSource);
    }

    public Optional<ConnectorSplitSource> getConnectorSplitSource()
    {
        return this.connectorSplitSource;
    }

    public Set<DataFile> getScannedDataFiles()
    {
        return scannedDataFiles;
    }

    public Set<DeleteFile> getFullyAppliedDeleteFiles()
    {
        return fullyAppliedDeleteFiles;
    }

    public Map<String, Object> getRelevantData()
    {
        return relevantData;
    }

    public void destroy()
    {
        this.relevantData.clear();
        this.scannedDataFiles.clear();
        this.fullyAppliedDeleteFiles.clear();
        this.connectorSplitSource.ifPresent(ConnectorSplitSource::close);
        this.connectorSplitSource = null;
    }
}
