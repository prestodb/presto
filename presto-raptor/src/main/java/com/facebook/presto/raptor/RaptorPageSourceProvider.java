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
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.inject.Inject;

import java.util.List;
import java.util.UUID;

import static com.facebook.presto.raptor.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;

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
    public ConnectorPageSource createPageSource(ConnectorSplit split, List<ConnectorColumnHandle> columns)
    {
        RaptorSplit raptorSplit = checkType(split, RaptorSplit.class, "split");

        UUID shardUuid = raptorSplit.getShardUuid();
        List<RaptorColumnHandle> columnHandles = FluentIterable.from(columns).transform(toRaptorColumnHandle()).toList();
        List<Long> columnIds = FluentIterable.from(columnHandles).transform(raptorColumnId()).toList();
        List<Type> columnTypes = FluentIterable.from(columnHandles).transform(raptorColumnType()).toList();

        return storageManager.getPageSource(shardUuid, columnIds, columnTypes, raptorSplit.getEffectivePredicate());
    }

    private static Function<ConnectorColumnHandle, RaptorColumnHandle> toRaptorColumnHandle()
    {
        return new Function<ConnectorColumnHandle, RaptorColumnHandle>()
        {
            @Override
            public RaptorColumnHandle apply(ConnectorColumnHandle handle)
            {
                return checkType(handle, RaptorColumnHandle.class, "columnHandle");
            }
        };
    }

    private static Function<RaptorColumnHandle, Long> raptorColumnId()
    {
        return new Function<RaptorColumnHandle, Long>()
        {
            @Override
            public Long apply(RaptorColumnHandle handle)
            {
                return handle.getColumnId();
            }
        };
    }

    private static Function<RaptorColumnHandle, Type> raptorColumnType()
    {
        return new Function<RaptorColumnHandle, Type>()
        {
            @Override
            public Type apply(RaptorColumnHandle handle)
            {
                return handle.getColumnType();
            }
        };
    }
}
