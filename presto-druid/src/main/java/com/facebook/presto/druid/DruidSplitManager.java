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
package com.facebook.presto.druid;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.druid.DruidSessionProperties.isComputePushdownEnabled;
import static com.facebook.presto.druid.DruidSplit.createBrokerSplit;
import static com.facebook.presto.druid.DruidSplit.createSegmentSplit;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DruidSplitManager
        implements ConnectorSplitManager
{
    private final DruidClient druidClient;

    @Inject
    public DruidSplitManager(DruidClient druidClient)
    {
        this.druidClient = requireNonNull(druidClient, "druid client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        DruidTableLayoutHandle layoutHandle = (DruidTableLayoutHandle) layout;
        DruidTableHandle table = layoutHandle.getTable();
        if (isComputePushdownEnabled(session) || (table.getDql().isPresent() && table.getDql().get().getPushdown())) {
            return new FixedSplitSource(ImmutableList.of(createBrokerSplit(table.getDql().get())));
        }
        List<String> segmentIds = druidClient.getDataSegmentId(table.getTableName());

        List<DruidSplit> splits = segmentIds.stream()
                .map(id -> druidClient.getSingleSegmentInfo(table.getTableName(), id))
                .map(info -> createSegmentSplit(info, HostAddress.fromUri(druidClient.getDruidBroker())))
                .collect(toImmutableList());

        return new FixedSplitSource(splits);
    }
}
