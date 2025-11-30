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

import com.facebook.presto.spi.connector.ConnectorProcedureContext;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class IcebergProcedureContext
        implements ConnectorProcedureContext
{
    final Set<DataFile> scannedDataFiles = new HashSet<>();
    final Set<DeleteFile> fullyAppliedDeleteFiles = new HashSet<>();
    final Map<String, Object> relevantData = new HashMap<>();
    Optional<Table> table = Optional.empty();
    Transaction transaction;
    Optional<Consumer<FileScanTask>> fileScanTaskConsumer = Optional.empty();

    public void setTable(Table table)
    {
        this.table = Optional.of(table);
    }

    public void setTransaction(Transaction transaction)
    {
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

    public void setFileScanTaskConsumer(Consumer<FileScanTask> fileScanTaskConsumer)
    {
        requireNonNull(fileScanTaskConsumer, "fileScanTaskConsumer is null");
        this.fileScanTaskConsumer = Optional.of(fileScanTaskConsumer);
    }

    public Optional<Consumer<FileScanTask>> getFileScanTaskConsumer()
    {
        return this.fileScanTaskConsumer;
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
        this.fileScanTaskConsumer = Optional.empty();
    }
}
