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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorDistributedProcedureHandle;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMergeTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Optional;

public interface ConnectorCodecProvider
{
    default Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ConnectorTransactionHandle>> getConnectorTransactionHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ConnectorOutputTableHandle>> getConnectorOutputTableHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ConnectorInsertTableHandle>> getConnectorInsertTableHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ConnectorDeleteTableHandle>> getConnectorDeleteTableHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ConnectorMergeTableHandle>> getConnectorMergeTableHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ConnectorTableLayoutHandle>> getConnectorTableLayoutHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ConnectorTableHandle>> getConnectorTableHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ConnectorPartitioningHandle>> getConnectorPartitioningHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ConnectorIndexHandle>> getConnectorIndexHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ConnectorTableFunctionHandle>> getConnectorTableFunctionHandleCodec()
    {
        return Optional.empty();
    }

    default Optional<ConnectorCodec<ConnectorDistributedProcedureHandle>> getConnectorDistributedProcedureHandleCodec()
    {
        return Optional.empty();
    }
}
