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
package com.facebook.presto.lance.splits;

import com.facebook.presto.lance.metadata.LanceTableLayoutHandle;
import com.facebook.presto.lance.client.LanceClient;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class LanceSplitManager
        implements ConnectorSplitManager
{
    private final LanceClient lanceClient;

    @Inject
    public LanceSplitManager(LanceClient lanceClient) {
        this.lanceClient = requireNonNull(lanceClient, "lanceClient is null");
    }
    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        LanceTableLayoutHandle layoutHandle = (LanceTableLayoutHandle) layout;
        return new LanceSplitSource(lanceClient, layoutHandle.getTable());
    }
}
