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

import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;

import java.util.Optional;

public interface ConnectorCodecProvider
{
    default Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec()
    {
        throw new UnsupportedOperationException("This method is not implemented yet.");
    }

    default Optional<ConnectorCodec<ConnectorTransactionHandle>> getConnectorTransactionHandleCodec()
    {
        throw new UnsupportedOperationException("This method is not implemented yet.");
    }

    default Optional<ConnectorCodec<ConnectorOutputTableHandle>> getConnectorOutputTableHandleCodec()
    {
        throw new UnsupportedOperationException("This method is not implemented yet.");
    }

    default Optional<ConnectorCodec<ConnectorInsertTableHandle>> getConnectorInsertTableHandleCodec()
    {
        throw new UnsupportedOperationException("This method is not implemented yet.");
    }

    default Optional<ConnectorCodec<ConnectorDeleteTableHandle>> getConnectorDeleteTableHandleCodec()
    {
        throw new UnsupportedOperationException("This method is not implemented yet.");
    }

    default Optional<ConnectorCodec<ConnectorTableLayoutHandle>> getConnectorTableLayoutHandleCodec()
    {
        throw new UnsupportedOperationException("This method is not implemented yet.");
    }

    default Optional<ConnectorCodec<ConnectorTableHandle>> getConnectorTableHandleCodec()
    {
        throw new UnsupportedOperationException("This method is not implemented yet.");
    }
}
