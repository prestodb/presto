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
package com.facebook.presto.mongodb;

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

import static com.facebook.presto.mongodb.TypeUtils.checkType;
import static com.facebook.presto.spi.HostAddress.fromParts;
import static java.util.stream.Collectors.toList;

public class MongoSplitManager
        implements ConnectorSplitManager
{
    private final List<HostAddress> addresses;

    @Inject
    public MongoSplitManager(MongoClientConfig config)
    {
        this.addresses = config.getSeeds().stream()
                .map(s -> fromParts(s.getHost(), s.getPort()))
                .collect(toList());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        MongoTableLayoutHandle tableLayout = checkType(layout, MongoTableLayoutHandle.class, "layout");
        MongoTableHandle tableHandle = tableLayout.getTable();

        MongoSplit split = new MongoSplit(
                tableHandle.getSchemaTableName(),
                tableLayout.getTupleDomain(),
                addresses);

        return new FixedSplitSource(ImmutableList.of(split));
    }
}
