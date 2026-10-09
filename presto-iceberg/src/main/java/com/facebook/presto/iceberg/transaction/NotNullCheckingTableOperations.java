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
package com.facebook.presto.iceberg.transaction;

import com.facebook.presto.spi.SchemaTableName;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import static com.facebook.presto.iceberg.IcebergUtil.verifyNoNullValuesInColumnsSetNotNullConcurrently;
import static java.util.Objects.requireNonNull;

/**
 * A transaction commits through these operations on each retry, with {@code base} refreshed to the metadata the
 * commit replaces, so a column set NOT NULL by another query after the transaction read the table is seen here.
 */
public final class NotNullCheckingTableOperations
        implements TableOperations
{
    private final SchemaTableName tableName;
    private final TableOperations delegate;
    private final Schema readSchema;

    public NotNullCheckingTableOperations(SchemaTableName tableName, TableOperations delegate)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.readSchema = delegate.current().schema();
    }

    @Override
    public TableMetadata current()
    {
        return delegate.current();
    }

    @Override
    public TableMetadata refresh()
    {
        return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata)
    {
        if (base != null) {
            verifyNoNullValuesInColumnsSetNotNullConcurrently(readSchema, base, metadata, delegate.io(), tableName);
        }
        delegate.commit(base, metadata);
    }

    @Override
    public FileIO io()
    {
        return delegate.io();
    }

    @Override
    public EncryptionManager encryption()
    {
        return delegate.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName)
    {
        return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider()
    {
        return delegate.locationProvider();
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata)
    {
        return delegate.temp(uncommittedMetadata);
    }

    @Override
    public long newSnapshotId()
    {
        return delegate.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup()
    {
        return delegate.requireStrictCleanup();
    }
}
