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
package com.facebook.presto.maxcompute;

import com.facebook.presto.maxcompute.adapter.ConnectorRecordSinkProvider;
import com.facebook.presto.maxcompute.adapter.RecordSink;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class MaxComputeRecordSinkProvider
        implements ConnectorRecordSinkProvider
{
    private final MaxComputeClient maxComputeClient;

    @Inject
    public MaxComputeRecordSinkProvider(MaxComputeClient maxComputeClient)
    {
        this.maxComputeClient = requireNonNull(maxComputeClient, "odpsClient is null");
    }

    @Override
    public RecordSink getRecordSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordSink getRecordSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        return new MaxComputeSingleRecordSink((MaxComputeInsertTableHandle) tableHandle,
                maxComputeClient, session);
    }
}
