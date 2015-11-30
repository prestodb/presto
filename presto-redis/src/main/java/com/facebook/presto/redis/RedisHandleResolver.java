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
package com.facebook.presto.redis;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Redis specific {@link ConnectorHandleResolver} implementation.
 */
public class RedisHandleResolver
        implements ConnectorHandleResolver
{
    private final String connectorId;

    @Inject
    RedisHandleResolver(RedisConnectorId connectorId, RedisConnectorConfig redisConnectorConfig)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        requireNonNull(redisConnectorConfig, "redisConnectorConfig is null");
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return tableHandle != null && tableHandle instanceof RedisTableHandle && connectorId.equals(((RedisTableHandle) tableHandle).getConnectorId());
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        return columnHandle != null && columnHandle instanceof RedisColumnHandle && connectorId.equals(((RedisColumnHandle) columnHandle).getConnectorId());
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return split != null && split instanceof RedisSplit && connectorId.equals(((RedisSplit) split).getConnectorId());
    }

    @Override
    public boolean canHandle(ConnectorTableLayoutHandle handle)
    {
        return handle instanceof RedisTableLayoutHandle && ((RedisTableLayoutHandle) handle).getConnectorId().equals(connectorId);
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

    RedisTableHandle convertTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof RedisTableHandle, "tableHandle is not an instance of RedisTableHandle");
        RedisTableHandle redisTableHandle = (RedisTableHandle) tableHandle;
        checkArgument(redisTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        return redisTableHandle;
    }

    RedisColumnHandle convertColumnHandle(ColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof RedisColumnHandle, "columnHandle is not an instance of RedisColumnHandle");
        RedisColumnHandle redisColumnHandle = (RedisColumnHandle) columnHandle;
        checkArgument(redisColumnHandle.getConnectorId().equals(connectorId), "columnHandle is not for this connector");
        return redisColumnHandle;
    }

    RedisSplit convertSplit(ConnectorSplit split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split instanceof RedisSplit, "split is not an instance of RedisSplit");
        RedisSplit redisSplit = (RedisSplit) split;
        checkArgument(redisSplit.getConnectorId().equals(connectorId), "split is not for this connector");
        return redisSplit;
    }

    RedisTableLayoutHandle convertLayout(ConnectorTableLayoutHandle layout)
    {
        requireNonNull(layout, "layout is null");
        checkArgument(layout instanceof RedisTableLayoutHandle, "layout is not an instance of RedisTableLayoutHandle");
        RedisTableLayoutHandle redisLayout = (RedisTableLayoutHandle) layout;
        checkArgument(redisLayout.getConnectorId().equals(connectorId), "layout is not for this connector");
        return redisLayout;
    }
}
