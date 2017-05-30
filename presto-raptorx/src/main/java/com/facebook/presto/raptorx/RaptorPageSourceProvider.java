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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.storage.ReaderAttributes;
import com.facebook.presto.raptorx.storage.StorageManager;
import com.facebook.presto.raptorx.util.ConcatPageSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RaptorPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final StorageManager storageManager;

    @Inject
    public RaptorPageSourceProvider(StorageManager storageManager)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit connectorSplit, List<ColumnHandle> columns)
    {
        RaptorSplit split = (RaptorSplit) connectorSplit;

        long tableId = split.getTableId();
        int bucketNumber = split.getBucketNumber();
        OptionalLong transactionId = split.getTransactionId();
        Map<Long, Type> chunkColumnTypes = split.getChunkColumnTypes();
        Optional<CompressionType> compressionType = split.getCompressionType();
        TupleDomain<RaptorColumnHandle> predicate = split.getPredicate();
        ReaderAttributes readerAttributes = ReaderAttributes.from(session);

        List<RaptorColumnHandle> handles = columns.stream().map(RaptorColumnHandle.class::cast).collect(toList());
        List<Long> columnIds = handles.stream().map(RaptorColumnHandle::getColumnId).collect(toImmutableList());
        List<Type> columnTypes = handles.stream().map(RaptorColumnHandle::getColumnType).collect(toImmutableList());

        Function<Long, ConnectorPageSource> factory = chunkId -> storageManager.getPageSource(
                tableId,
                chunkId,
                bucketNumber,
                columnIds,
                columnTypes,
                predicate,
                readerAttributes,
                transactionId,
                chunkColumnTypes,
                compressionType);

        if (split.getChunkIds().size() == 1) {
            return factory.apply(getOnlyElement(split.getChunkIds()));
        }

        return new ConcatPageSource(split.getChunkIds().stream().map(factory).iterator());
    }
}
