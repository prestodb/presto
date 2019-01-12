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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

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
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layoutHandle, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        MemoryTableLayoutHandle layout = (MemoryTableLayoutHandle) layoutHandle;

        List<MemoryDataFragment> dataFragments = layout.getDataFragments();

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (MemoryDataFragment dataFragment : dataFragments) {
            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(
                        new MemorySplit(
                                layout.getTable(),
                                i,
                                splitsPerNode,
                                dataFragment.getHostAddress(),
                                dataFragment.getRows()));
            }
        }
        return new FixedSplitSource(splits.build());
    }
}
