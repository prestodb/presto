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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

public final class MemorySplitManager
        implements ConnectorSplitManager
{
    private final int splitsPerNode;

    @Inject
    public MemorySplitManager(MemoryConfig config)
    {
        this.splitsPerNode = config.getSplitsPerNode();
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        MemoryTableLayoutHandle layout = (MemoryTableLayoutHandle) layoutHandle;

        List<HostAddress> hosts = layout.getTable().getHosts();

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (HostAddress host : hosts) {
            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(
                        new MemorySplit(
                                layout.getTable(),
                                i,
                                splitsPerNode,
                                ImmutableList.of(host)));
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
