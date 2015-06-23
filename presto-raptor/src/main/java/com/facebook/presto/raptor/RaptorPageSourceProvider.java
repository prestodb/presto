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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static com.facebook.presto.raptor.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;

public class RaptorPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final StorageManager storageManager;

    @Inject
    public RaptorPageSourceProvider(StorageManager storageManager)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        RaptorSplit raptorSplit = checkType(split, RaptorSplit.class, "split");

        UUID shardUuid = raptorSplit.getShardUuid();
        List<RaptorColumnHandle> columnHandles = columns.stream().map(toRaptorColumnHandle()).collect(toList());
        List<Long> columnIds = columnHandles.stream().map(RaptorColumnHandle::getColumnId).collect(toList());
        List<Type> columnTypes = columnHandles.stream().map(RaptorColumnHandle::getColumnType).collect(toList());

        return storageManager.getPageSource(shardUuid, columnIds, columnTypes, raptorSplit.getEffectivePredicate());
    }

    private static Function<ColumnHandle, RaptorColumnHandle> toRaptorColumnHandle()
    {
        return handle -> checkType(handle, RaptorColumnHandle.class, "columnHandle");
    }
}
