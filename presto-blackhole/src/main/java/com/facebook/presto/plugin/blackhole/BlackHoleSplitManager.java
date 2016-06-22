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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.plugin.blackhole.Types.checkType;

public final class BlackHoleSplitManager
        implements ConnectorSplitManager
{
    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        BlackHoleTableLayoutHandle layout = checkType(
                layoutHandle,
                BlackHoleTableLayoutHandle.class,
                "BlackHoleTableLayoutHandle");

        ImmutableList.Builder<BlackHoleSplit> builder = ImmutableList.<BlackHoleSplit>builder();

        for (int i = 0; i < layout.getSplitCount(); i++) {
            builder.add(
                    new BlackHoleSplit(
                            layout.getPagesPerSplit(),
                            layout.getRowsPerPage(),
                            layout.getFieldsLength()));
        }
        return new FixedSplitSource(builder.build());
    }
}
