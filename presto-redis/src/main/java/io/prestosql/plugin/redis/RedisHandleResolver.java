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
package io.prestosql.plugin.redis;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Redis specific {@link ConnectorHandleResolver} implementation.
 */
public class RedisHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        return RedisTransactionHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return RedisTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return RedisColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return RedisSplit.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        return RedisTableLayoutHandle.class;
    }

    static RedisTableHandle convertTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof RedisTableHandle, "tableHandle is not an instance of RedisTableHandle");
        return (RedisTableHandle) tableHandle;
    }

    static RedisColumnHandle convertColumnHandle(ColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof RedisColumnHandle, "columnHandle is not an instance of RedisColumnHandle");
        return (RedisColumnHandle) columnHandle;
    }

    static RedisSplit convertSplit(ConnectorSplit split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split instanceof RedisSplit, "split is not an instance of RedisSplit");
        return (RedisSplit) split;
    }

    static RedisTableLayoutHandle convertLayout(ConnectorTableLayoutHandle layout)
    {
        requireNonNull(layout, "layout is null");
        checkArgument(layout instanceof RedisTableLayoutHandle, "layout is not an instance of RedisTableLayoutHandle");
        return (RedisTableLayoutHandle) layout;
    }
}
